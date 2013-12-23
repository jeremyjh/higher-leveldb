{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module Database.LevelDB.Higher.Store
    ( fetch, scanFetch
    , store
    , decodeStore
    , FetchFail(..), Storeable
    , deriveStorable, deriveStorableVersion, Version
    ) where

import           Database.LevelDB.Higher.Core hiding (get, put)
import qualified Database.LevelDB.Higher.Core as Core
import           Data.ByteString          (ByteString)
import           Data.Typeable            (Typeable)
import           Data.Serialize
import           Data.SafeCopy

import           Language.Haskell.TH (Name, Q, Dec)
import           Language.Haskell.TH.Lib (conT)


data FetchFail = ParseFail String | NotFound String deriving (Show, Eq)

type Storeable a = (SafeCopy a, Serialize a, Show a, Typeable a)


-- | Deserialize bytes into a concrete type
decodeStore :: (Storeable a) => ByteString -> Either FetchFail a
decodeStore serial =
    case decode serial of
    Left s -> Left $ ParseFail s
    Right ser -> Right ser

-- | Save a serializeble type using a provided key
store :: (MonadLevelDB m, Storeable a) => Key -> a -> m ()
store k s = Core.put k (encode s)


-- | Fetch the 'Storeable' from the database
--
fetch :: (MonadLevelDB m, Storeable a) => Key -> m (Either FetchFail a)
fetch k = fmap decode_found $ Core.get k
  where
    decode_found Nothing = Left $ NotFound (show k)
    decode_found (Just bs) = decodeStore bs

scanFetch :: (MonadLevelDB m, Storeable a) => Key -> m [Either FetchFail a]
scanFetch k = scan k queryList {scanMap = \ (_, v) -> decodeStore v}


-- | Template haskell function to create the Serialize and SafeCopy
-- instances for a given type
--
-- > data MyData = MyData Int
-- > deriveStorable ''MyData
deriveStorable :: Name -> Q [Dec]
deriveStorable = deriveStorableVersion 1

-- | Template haskell function to create the Serialize and SafeCopy
-- instances for a given type - use this one to specify a later version
-- (also will require a migration instance - see SafeCopy docs for more info )
--
-- > data MyDataV1 = MyDataV1 Int
-- > data MyData = MyData Int String
-- > deriveStorable ''MyDataV1
-- > deriveStoreableVersion 2 ''MyData
deriveStorableVersion :: Version a -> Name -> Q [Dec]
deriveStorableVersion ver name = do
    sc <- case ver of
        1 -> deriveSafeCopy 1 'base name
        _ -> deriveSafeCopy ver 'extension name
    ss <- [d| instance Serialize $(conT name) where
                get = safeGet
                put = safePut
          |]
    return $ sc ++ ss
