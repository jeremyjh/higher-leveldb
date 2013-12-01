{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.LevelDB.Higher.CoreSpec (main, spec) where

import           Database.LevelDB.Higher.Core

import qualified Data.ByteString                  as BS
import           Data.ByteString                  (ByteString)
import           Data.Serialize                  (decode)
import           Data.Monoid
import           Test.Hspec
import           System.Process(system)

import           Control.Monad.Reader
import           Control.Concurrent.Lifted
import           Data.Serialize (encode)

--debug
import           Debug.Trace
debug :: (Show a) => a -> a
debug a = traceShow a a

debugKSID :: ByteString -> ByteString
debugKSID a = traceShow (debugKeySpaceId a) a
    where
        debugKeySpaceId :: ByteString -> Int
        debugKeySpaceId bs = let (Right i) = decode bs in i
--debug


main :: IO ()
main = hspec spec

setup :: IO ()
setup = void $ system ("rm -rf " <> testDB)

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
                    value <- runBatch $ do
                                put "\1" "first"
                                put "\2" "second"
                                put "\3" "third"
                                delete "\2"
                                get "\3" -- won't see this in the batch
                    count <- scan "" queryCount
                    return (value, count)
               `shouldReturn` (Nothing, 2)

            it "will do consistent reads in a snapshot" $ do
                runCreateLevelDB testDB "snapshot" $ do
                    put "first" "initial value"
                    withSnapshot $ do
                        put "first" "don't see me"
                        get "first"
                `shouldReturn` Just "initial value"

            it "scans with a keyspace" $ do
                withDBT $ withKeySpace "overflow" $ do
                    runBatch $ do
                        forM_ ([1..10] :: [Int]) $ \i -> do
                            put (encode i) "hi guys"
                    xs <- scan "" queryItems
                    return $ length xs
                `shouldReturn` 10


testDB = "/tmp/leveltest"
dbOpts = def {createIfMissing = True, cacheSize= 2048}

withDBT :: LevelDBT IO a -> IO a
withDBT = runLevelDB testDB dbOpts def "Database.LevelDB.HigherSpec"

withDBRT :: LevelDBT IO a -> IO a
withDBRT = runResourceT . runLevelDB' testDB dbOpts def "Database.LevelDB.HigherSpec"
