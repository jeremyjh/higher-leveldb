{-# LANGUAGE OverloadedStrings #-}
module Database.LevelDB.HigherSpec (main, spec) where

import qualified Data.ByteString                  as BS
import           Data.Monoid
import           Test.Hspec
import           System.Process(system)
import           Database.LevelDB.Higher
import           Control.Monad.Trans.Resource

main :: IO ()
main = hspec spec

setup :: IO ()
setup = system ("rm -rf " <> testDB) >> return ()

spec :: Spec
spec = do
    describe "Dash.Store" $ do
        it "setup" $ setup >>= shouldReturn (return())
        describe "has a reader context API that" $ do
            it "can put" $ do
                withDBT $ put "putgetkey" "putgetvalue"
                `shouldReturn` ()
            it "can get" $ do
                withDBT $ get "putgetkey"
                `shouldReturn` (Just "putgetvalue")
            it "can delete" $ do
                withDBT $ do
                    put "deletekey" "doesn't matter"
                    delete "deletekey"
                    get "deletekey"
                `shouldReturn` Nothing
            it "can isolate data with keyspaces" $ do
                runLevelDB testDB "thespace" $ do
                    put "thekey" "thevalue"
                    withKeySpace "otherspace" $ put "thekey" "othervalue"
                    get "thekey"
                `shouldReturn` (Just "thevalue")
            it "can scan and transform" $ do
                runLevelDB testDB "scan" $ do
                    put "employee:1" "Jill"
                    put "employee:2" "Jack"
                    put "cheeseburgers:1" "do not want"
                    r1 <- scan "employee:" queryItems
                    r2 <- scan "employee:" $
                                   queryList {scanMap = \ (k, v) -> v <> " Smith"}
                    r3 <- scan "employee:"
                                   queryItems { scanFilter = \ (_, v) -> v > "Jack" }
                    r4 <- scan "employee:" $
                                queryBegins   { scanInit = 0
                                              , scanMap = \ (_, v) -> BS.head v
                                              , scanReduce = (+) }
                    return (r1, r2, r3, r4)
                `shouldReturn` ( [("employee:1", "Jill"), ("employee:2", "Jack")]
                               , [ "Jill Smith", "Jack Smith"]
                               , [("employee:1", "Jill")]
                               , 148)

testDB = "/tmp/leveltest"

withDBT :: LevelDBT IO a -> IO a
withDBT = runLevelDB testDB "Database.LevelDB.HigherSpec"
