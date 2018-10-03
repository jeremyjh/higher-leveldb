{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Database.LevelDB.HigherSpec (main, spec) where

import           Control.Applicative          (Applicative)
import           Control.Monad.Base           (MonadBase (..))
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Control.Monad.Writer
import qualified Data.ByteString              as BS
import           Data.Maybe                   (fromJust)
import           Data.Monoid
import           Data.Serialize               (encode)
import           Database.LevelDB.Higher
import           System.Process               (system)
import           Test.Hspec
import           UnliftIO.Concurrent          (ThreadId, threadDelay)
import           UnliftIO.MVar

--debug
import           Debug.Trace
debug :: (Show a) => a -> a
debug a = traceShow a a
--debug


main :: IO ()
main = hspec spec

setup :: IO ()
setup = system ("rm -rf " <> testDB) >> return ()

spec :: Spec
spec = do
        it "setup" $ setup >>= shouldReturn (return())
        describe "has a reader context API that" $ do
            it "can put" $ do
                withDBT $ put "putgetkey" "putgetvalue"
                `shouldReturn` ()
            it "can get" $ do
                withDBT $ get "putgetkey"
                `shouldReturn` (Just "putgetvalue")
            it "can delete" $ do
                withDBRT $ do
                    put "deletekey" "doesn't matter"
                    delete "deletekey"
                    get "deletekey"
                `shouldReturn` Nothing
            it "can isolate data with keyspaces" $ do
                runCreateLevelDB testDB "thespace" $ do
                    put "thekey" "thevalue"
                    withKeySpace "otherspace" $ put "thekey" "othervalue"
                    get "thekey"
                `shouldReturn` (Just "thevalue")
            it "can override read/write options locally" $ do
                withDBT $ do
                    withOptions (def, def {sync = True}) $ do
                        put "puttingsync" "can't you tell?"
                        get "puttingsync"
                `shouldReturn` Just "can't you tell?"
            it "can scan and transform" $ do
                runCreateLevelDB testDB "scan" $ do
                    put "employee:1" "Jill"
                    put "employee:2" "Jack"
                    put "cheeseburgers:1" "do not want"
                    r1 <- scan "employee:" queryItems
                    r2 <- scan "employee" $
                                   queryList {scanMap = \ (k, v) -> v <> " Smith"}
                    r3 <- scan "e"
                                   queryItems { scanFilter = \ (_, v) -> v > "Jack" }
                    r4 <- scan "employee" $
                                queryBegins   { scanInit = 0
                                              , scanMap = \ (_, v) -> BS.head v
                                              , scanFold = (+) }
                    return (r1, r2, r3, r4)
                `shouldReturn` ( [("employee:1", "Jill"), ("employee:2", "Jack")]
                               , [ "Jill Smith", "Jack Smith"]
                               , [("employee:1", "Jill")]
                               , 148)
            it "can write data in batches" $ do
                runLevelDB testDB dbOpts def "batches" $ do
                    runBatch $ do
                        putB "\1" "first"
                        putB "\2" "second"
                        putB "\3" "third"
                        deleteB "\2"
                    scan "" queryCount
               `shouldReturn` 2
            it "will do consistent reads in a snapshot" $ do
                runCreateLevelDB testDB "snapshot" $ do
                    put "first" "initial value"
                    withSnapshot $ do
                        put "first" "don't see me"
                        get "first"
                `shouldReturn` Just "initial value"
        describe "can be used in a custom MonadT stack" $ do
            it "can be used with a reader" $ do
                runTestAppR testDB "TestAppReader" $ do
                    value <- ask
                    put "thiskey" value
                    get "thiskey"
                `shouldReturn` (Just "a string value to read")
            it "still works withKeySpace" $ do
                runTestAppR testDB "TestAppReader" $ do
                    withKeySpace "TestAppReader2" $ do
                        notit <- get "thiskey" -- not found in this keyspace
                        gotit <- ask -- our top Reader still works
                        return (notit, gotit)
                `shouldReturn` (Nothing, "a string value to read")
            it "can be used with a writer" $ do
                runTestAppW testDB "TestAppWriter" $ do
                    put "writekey" "words"
                    tell "tolja"
                    withKeySpace "TestAppWriter2" $ do
                        put "writekey" "not these words"
                        tell "twice"
                    get "writekey"
                `shouldReturn` (Just "words", "toljatwice")
            it "can work with a reader/writer" $ do
                runTestAppRW testDB "TestAppRW" $ do
                    v <- ask
                    put "writekey" v
                    tell "toljer"
                    get "writekey"
                `shouldReturn` (Just "a different string value to read"
                               , "toljer")
            it "can get forked" $ do
                runTestAppR testDB "forkInReader" $ do
                    put "onetwo" "three"
                    forkTestAppR $ do
                        rv <- ask
                        threadDelay 1
                        put "three" rv
                    threadDelay 100
                    get "three"
                `shouldReturn` Just "a string value to read"
            it "scans with a keyspace" $ do
                withDBT $ withKeySpace "overflow" $ do
                    forM ([1..10] :: [Int]) $ \i -> do
                        put (encode i) "hi guys"
                    xs <- scan "" queryItems
                    return $ length xs
                `shouldReturn` 10
        describe "exports a working MonadUnliftIO instance" $ do
          it "can modifyMVar in a working LevelDBT context" $ do
            withDBT $ do
                mv <- newMVar 0
                modifyMVar_ mv $ \v -> do
                    put "somekey" "somedata"
                    return (v + 1)
                mvalue <- readMVar mv
                ldbvalue <- fromJust <$> get "somekey"
                return (mvalue, ldbvalue)
            `shouldReturn` (1, "somedata")

testDB = "/tmp/leveltest"
dbOpts = def {createIfMissing = True, cacheSize= 2048}

withDBT :: LevelDBT IO a -> IO a
withDBT = runLevelDB testDB dbOpts def "Database.LevelDB.HigherSpec"

withDBRT :: LevelDBT IO a -> IO a
withDBRT = runResourceT . runLevelDB' testDB dbOpts def "Database.LevelDB.HigherSpec"

runTestAppR :: FilePath -> KeySpace -> TestAppR a -> IO a
runTestAppR path ks ta = runLevelDB path dbOpts def ks $ do
    runReaderT (unTestAppR ta) "a string value to read"

runTestAppW :: FilePath -> KeySpace -> TestAppW a -> IO (a, BS.ByteString)
runTestAppW path ks ta = runLevelDB path dbOpts def ks $ do
    runWriterT (unTestAppW ta)

runTestAppRW :: FilePath -> KeySpace -> TestAppRW a -> IO (a, BS.ByteString)
runTestAppRW path ks ta = runLevelDB path dbOpts def ks $ do
    runWriterT $ runReaderT (unTestAppRW ta) "a different string value to read"

forkTestAppR :: TestAppR () -> TestAppR ThreadId
forkTestAppR ma = TestAppR $
    mapReaderT
        (\ld -> forkLevelDB ld)
        (unTestAppR ma)


newtype TestAppR a = TestAppR { unTestAppR :: ReaderT BS.ByteString (LevelDBT IO) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadReader BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadLevelDB
                     )

newtype TestAppW a = TestAppW { unTestAppW :: WriterT BS.ByteString (LevelDBT IO) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadWriter BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadLevelDB
                     )
newtype TestAppRW a = TestAppRW { unTestAppRW :: ReaderT BS.ByteString
                                                (WriterT BS.ByteString (LevelDBT IO)) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadReader BS.ByteString, MonadWriter BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadLevelDB)
