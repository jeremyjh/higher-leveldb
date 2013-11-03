{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ConstraintKinds #-}

module Database.LevelDB.Higher.Internal.Types where


import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Monad.Identity
import           Data.Word                         (Word32)

import           Control.Applicative               (Applicative)
import           Control.Monad.Base                (MonadBase(..))

import           Control.Concurrent.MVar.Lifted

import qualified Data.ByteString                   as BS
import           Data.ByteString                   (ByteString)

import           Database.LevelDB
    hiding (put, get, delete, write, withSnapshot)
import           Control.Monad.Trans.Resource
import           Control.Monad.Trans.Control




type Key = ByteString
type Value = ByteString
-- | A KeySpace is similar concept to a \"bucket\" in other libraries and database systems.
-- The ByteString for KeySpace can be arbitrarily long without performance impact because
-- the system maps the KeySpace name to a 4-byte KeySpaceId internally which is preprended to each Key.
-- KeySpaces are cheap and plentiful and indeed with this library you cannot escape them
-- (you can supply an empty ByteString to use a default KeySpace, but it is still used).
-- One intended use case is to use the full
-- Key of a "parent" as the KeySpace of its children (instance data in a time-series for example).
-- This lets you scan over a range-based key without passing over any unneeded items.
type KeySpace = ByteString
type KeySpaceId = ByteString
-- | The basic unit of storage is a Key/Value pair.
type Item = (Key, Value)

type RWOptions = (ReadOptions, WriteOptions)
-- | Reader-based data context API
--
-- Context contains database handle and KeySpace
data DBContext = DBC { dbcDb :: DB
                     , dbcKsId :: KeySpaceId
                     , dbcSyncMV :: MVar Word32
                     , dbcRWOptions :: RWOptions
                     , dbcKeySpace :: KeySpace
                     }
instance Show (DBContext) where
    show = (<>) "KeySpaceID: " . show . dbcKsId

-- | LevelDBT Transformer provides a context for database operations
-- provided in this module.
--
-- This transformer has the same constraints as 'ResourceT' as it wraps
-- 'ResourceT' along with a 'DBContext' 'Reader'.
--
-- If you aren't building a custom monad stack you can just use the 'LevelDB' alias.
newtype LevelDBT m a
        =  LevelDBT { unLevelDBT :: ReaderT DBContext (ResourceT m) a }
            deriving ( Functor, Applicative, Monad, MonadIO, MonadThrow)

instance (MonadBase b m) => MonadBase b (LevelDBT m) where
    liftBase = lift . liftBase

instance MonadTrans LevelDBT where
    lift = LevelDBT . lift . lift

instance (MonadResourceBase m) => MonadResource (LevelDBT m) where
    liftResourceT = LevelDBT . liftResourceT

instance MonadTransControl LevelDBT where
    newtype StT LevelDBT a = StLevelDBT
            {unStLevelDBT :: StT ResourceT (StT (ReaderT DBContext) a) }
    liftWith f =
            LevelDBT $ liftWith $ \run ->
                       liftWith $ \run' ->
                       f $ liftM StLevelDBT . run' . run . unLevelDBT
    restoreT = LevelDBT . restoreT . restoreT . liftM unStLevelDBT

instance (MonadBaseControl b m) => MonadBaseControl b (LevelDBT m) where
    newtype StM (LevelDBT m) a =  StMT {unStMT :: ComposeSt LevelDBT m a}
    liftBaseWith = defaultLiftBaseWith StMT
    restoreM     = defaultRestoreM unStMT

-- | alias for LevelDBT IO - useful if you aren't building a custom stack.
type LevelDB a = LevelDBT IO a
