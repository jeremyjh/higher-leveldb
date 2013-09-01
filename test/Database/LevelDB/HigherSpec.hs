{-# LANGUAGE OverloadedStrings #-}
module Database.LevelDB.HigherSpec (main, spec) where

import           Prelude                          hiding ((++))
import qualified Data.ByteString                  as BS
import           Data.Monoid
import           Test.Hspec
import           System.Process(system)
import           Database.LevelDB.Higher
import           Control.Monad.Trans.Resource      (release, ResIO, runResourceT, liftResourceT)

main :: IO ()
main = hspec spec

setup :: IO ()
setup = system ("rm -rf " ++ testDB) >> return ()

spec :: Spec
spec = do
    describe "Dash.Store" $ do
        it "setup" $ setup >>= shouldReturn (return())
        describe "has a simple API that" $ do
            it "can write an arbitrary bytestring" $
                withDBT (put "somekey" "somevalue") >>= shouldReturn (return ())
        describe "has a reader context API that" $ do
            it "is awesome" $ do
                (Just simple)
                    <- runLevelDB testDB "awesome" $ do
                        put "thekey" "thevalue"
                        withKeySpace "otherspace" $ do
                            put "thekey" "othervalue"
                        simple <- get "thekey"
                        return simple
                simple `shouldBe` "thevalue"
            it "can scan partial matches" $ do
                results <- runLevelDB testDB "scan" $ do
                    put "employee:1" "Jill"
                    put "employee:2" "Jack"
                    put "cheeseburgers:1" "do not want"
                    first <-  scan "employee:" queryItems
                    second <- scan "employee:" $
                                   queryList {scanMap = (\(k, v) -> v ++ " Smith")}
                    third  <-  scan "employee:"
                                   queryItems { scanFilter = (\(_, v) -> v > "Jack") }
                    fourth <- scan "employee:" $
                                   queryBegins { scanInit = 0
                                              , scanMap = (\(_, v) -> BS.head v)
                                              , scanReduce = (+)
                                              }
                    return (first, second, third, fourth)
                results `shouldBe` ( [("employee:1", "Jill"), ("employee:2", "Jack")]
                                   , [ "Jill Smith", "Jack Smith"]
                                   , [("employee:1", "Jill")]
                                   , 148)

testDB = "/tmp/leveltest"

withDBT :: LevelDB a -> IO a
withDBT = runLevelDB testDB "Database.LevelDB.HigherSpec"

(++) :: Monoid w => w -> w -> w
(++) = mappend
