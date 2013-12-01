{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.LevelDB.Higher.ClassSpec (main, spec) where

import           Database.LevelDB.Higher.Core

import qualified Data.ByteString                  as BS
import           Data.Monoid
import           Test.Hspec
import           System.Process(system)

import           Control.Monad.Trans.Resource
import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Applicative              (Applicative)
import           Control.Monad.Base               (MonadBase(..))
import           Control.Concurrent.Lifted

main :: IO ()
main = hspec spec

setup :: IO ()
setup = void $ system ("rm -rf " <> testDB)

spec :: Spec
spec = do
        it "setup" $ setup >>= shouldReturn (return())
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
                        ks <- currentKeySpace -- why not test this here
                        notit <- get "thiskey" -- not found in this keyspace
                        gotit <- ask -- our top Reader still works
                        return (notit, gotit, ks)
                `shouldReturn` (Nothing, "a string value to read", "TestAppReader2")

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
                    runBatch $ do
                        put "writekey" v
                        lift $ tell "toljer" -- lift over the BatchWriterT
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
                    threadDelay 20000 --fiddlesome - if test fails bump it up
                    get "three"
                `shouldReturn` Just "a string value to read"

testDB = "/tmp/leveltest"

dbOpts = def {createIfMissing = True, cacheSize= 2048}


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
    mapReaderT forkLevelDB $ unTestAppR ma


newtype TestAppR a = TestAppR { unTestAppR :: ReaderT BS.ByteString (LevelDBT IO) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadReader BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadUnsafeIO, MonadLevelDB
                     )

newtype TestAppW a = TestAppW { unTestAppW :: WriterT BS.ByteString (LevelDBT IO) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadWriter BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadUnsafeIO, MonadLevelDB
                     )
newtype TestAppRW a = TestAppRW { unTestAppRW :: ReaderT BS.ByteString
                                                (WriterT BS.ByteString (LevelDBT IO)) a}
            deriving ( Functor, Applicative, Monad, MonadBase IO
                     , MonadReader BS.ByteString, MonadWriter BS.ByteString, MonadResource
                     , MonadIO, MonadThrow, MonadUnsafeIO, MonadLevelDB)
