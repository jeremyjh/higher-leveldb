{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.LevelDB.Higher.StoreSpec (main, spec) where

import qualified Data.ByteString                  as BS
import           Data.Monoid
import           Test.Hspec

import           System.Process(system)
import           Data.Typeable (Typeable(..))
import           Data.SafeCopy
import           Data.Serialize

import           Database.LevelDB.Higher
import           Database.LevelDB.Higher.Store

import           Control.Monad

--debug
import           Debug.Trace
debug :: (Show a) => a -> a
debug a = traceShow a a
--debug


main :: IO ()
main = hspec spec

setup :: IO ()
setup = void $ system ("rm -rf " <> testDB)

spec :: Spec
spec = do
        it "setup" $ setup >>= shouldReturn (return())
        describe "can store serializable objects" $ do
            it "writes Things to the DB" $
                withDBT (store "aThing" aThing) `shouldReturn` ()

            it "reads Things from the DB" $
                withDBT (fetch "aThing") `shouldReturn` Right aThing

            it "can store in a batch" $ do
                withDBT $ do
                    nofetch :: (Either FetchFail Thing)
                        <- runBatch $ do
                              store "aThingbatch" aThing
                              fetch "aThingbatch" -- won't see it in the batch

                    dofetch <- fetch "aThingbatch"
                    return (nofetch, dofetch)
                `shouldReturn` (Left (NotFound "\"aThingbatch\""), Right aThing)

            it "safely handles version migrations" $ do
                withDBT $ do
                    store "aThingV1" aThingV1
                    fetch "aThingV1"
                `shouldReturn` Right aThing


testDB = "/tmp/leveltest"
dbOpts = def {createIfMissing = True, cacheSize= 2048}

withDBT :: LevelDBT IO a -> IO a
withDBT = runLevelDB testDB dbOpts def "Database.LevelDB.Higher.StoreSpec"


aThingV1 = ThingV1 42 "hi guys" 9.9

aThing = Thing 42.0 "hi guys" 9.9



data ThingV1 = ThingV1 Int String Double deriving (Typeable, Show, Eq)

data Thing = Thing Double String Double deriving (Typeable, Show, Eq)

instance Migrate (Thing) where
  type MigrateFrom Thing = ThingV1
  migrate (ThingV1 i s d) = Thing (fromIntegral i) s d

deriveStorable ''ThingV1
deriveStorableVersion 2 ''Thing
