{-# LANGUAGE ConstraintKinds #-}

module Database.LevelDB.Higher.Store
    ( fetch, scanFetch
    , store, storeB
    , decodeStore
    , FetchFail(..), Storeable
    ) where

import           Data.ByteString
import           Data.Typeable
import           Control.Monad.Writer
import           Database.LevelDB.Higher
import           Data.Serialize           hiding (get, put)
import           Data.SafeCopy            (SafeCopy(..))


data FetchFail = ParseFail String | NotFound String deriving (Show, Eq)

type Storeable a = (SafeCopy a, Serialize a, Show a, Typeable a)

decodeStore :: (Storeable a) => ByteString -> Either FetchFail a
decodeStore serial =
    case decode serial of
    Left s -> Left $ ParseFail s
    Right ser -> Right ser

-- | Save a serializeble type using a provided key
store :: (MonadLevelDB m, Storeable a) => Key -> a -> m ()
store k s = put k (encode s)

-- | Store the object in the database - batch mode with 'runBatch'
--
storeB :: (Monad m, Storeable a)
       => Key
       -> a -> BatchWriter m
storeB k s = putB k (encode s)

-- | Fetch the 'Storeable' from the database
--
fetch :: (MonadLevelDB m, Storeable a) => Key -> m (Either FetchFail a)
fetch k = fmap decode_found $ get k
  where
    decode_found Nothing = Left $ NotFound (show k)
    decode_found (Just bs) = decodeStore bs

scanFetch :: (MonadLevelDB m, Storeable a) => Key -> m [Either FetchFail a]
scanFetch k = scan k queryList {scanMap = \ (_, v) -> decodeStore v}
