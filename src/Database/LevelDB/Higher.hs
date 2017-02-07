-- |
-- Higher LevelDB provides a rich monadic API for working with leveldb (<http://code.google.com/p/leveldb>) databases. It uses
-- the leveldb-haskell bindings to the C++ library. The LevelDBT transformer is
-- a Reader that maintains a database context with the open database as well as
-- default read and write options. It also manages a concept called a KeySpace, which is a bucket
-- scheme that provides a low (storage) overhead named identifier to segregate data. Finally it wraps a 'ResourceT'
-- which is required for use of leveldb-haskell functions.
--
-- The other major feature is the scan function and its ScanQuery structure that provides a
-- map / fold abstraction over the Iterator exposed by leveldb-haskell.
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RecordWildCards #-}

module Database.LevelDB.Higher
    (
    -- * Introduction
    -- $intro

    -- * Basic types
      Key, Value, Item, KeySpace, KeySpaceId
    -- * Basic operations
    , get, put, delete
    -- * Batch operations
    , runBatch, putB, deleteB
    -- * Scans
    , scan, ScanQuery(..), queryItems, queryList, queryBegins, queryCount
    -- * Context modifiers
    , withKeySpace, withOptions, withSnapshot
    , forkLevelDB
    -- * Monadic Types and Operations
    , MonadLevelDB(..), LevelDBT, LevelDB
    , mapLevelDBT
    , runLevelDB, runLevelDB', runCreateLevelDB
    -- * Re-exports
    , runResourceT
    , Options(..), ReadOptions(..), WriteOptions(..), RWOptions
    , WriteBatch, def
    , MonadThrow, MonadResourceBase
    ) where


import           Control.Monad.Reader
import           Control.Monad.Writer
import           Data.Word                         (Word32)

#if !MIN_VERSION_base(4,8,0)
import           Control.Applicative               (Applicative)
#endif
import           Control.Monad.Base                (MonadBase(..))

import           Control.Concurrent.MVar.Lifted
import           Control.Concurrent                (ThreadId)

import qualified Data.ByteString                   as BS
import           Data.ByteString                   (ByteString)
import           Data.Serialize                    (encode, decode)

import           Data.Default                      (def)
import qualified Database.LevelDB                  as LDB
import           Database.LevelDB
    hiding (put, get, delete, write, withSnapshot)
import           Control.Monad.Trans.Resource
import           Control.Monad.Trans.Control
import           Control.Monad.Catch               (MonadCatch (..)
                                                   , MonadMask (..))


#if MIN_VERSION_mtl(2,2,1)
import qualified Control.Monad.Except              as Except
#else
import qualified Control.Monad.Trans.Error as Error
#endif


import qualified Control.Monad.Trans.Cont          as Cont
import qualified Control.Monad.Trans.Identity      as Identity
import qualified Control.Monad.Trans.List          as List
import qualified Control.Monad.Trans.Maybe         as Maybe
import qualified Control.Monad.Trans.State         as State
import qualified Control.Monad.Trans.Writer        as Writer
import qualified Control.Monad.Trans.RWS           as RWS
import qualified Control.Monad.Trans.RWS.Strict    as Strict
import qualified Control.Monad.Trans.State.Strict  as Strict
import qualified Control.Monad.Trans.Writer.Strict as Strict

-- $intro
-- Operations take place within a 'MonadLevelDB' which is built with the LevelDBT transformer; the most
-- basic type would be 'LevelDBT' 'IO' which is type aliased as 'LevelDB'. The basic operations are
-- the same as the underlying leveldb-haskell versions except that the DB and Options arguments are
-- passed along by the LevelDB Reader, and the keys are automatically qualified with the KeySpaceId.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > import Database.LevelDB.Higher
-- >
-- > runCreateLevelDB "/tmp/mydb" "MyKeySpace" $ do
-- >     put "key:1" "this is a value"
-- >     get "key:1"
-- >
-- > Just "this is a value"
--

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
                     }
instance Show DBContext where
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

instance MonadCatch m => MonadCatch (LevelDBT m) where
    catch (LevelDBT m) c =
      LevelDBT . ReaderT $ \r -> runReaderT m r `catch` \e -> runReaderT (unLevelDBT (c e)) r

instance MonadMask m => MonadMask (LevelDBT m) where
    mask a = LevelDBT . ReaderT $ \e -> mask $ \u -> runReaderT (unLevelDBT (a $ q u)) e
      where
        q ::  (ResourceT m a -> ResourceT m a) -> LevelDBT m a -> LevelDBT m a
        q u (LevelDBT (ReaderT b)) =
          LevelDBT $ ReaderT (u . b)
    uninterruptibleMask a =
      LevelDBT . ReaderT $ \e -> uninterruptibleMask $ \u -> runReaderT (unLevelDBT (a $ q u)) e
      where
        q ::  (ResourceT m a -> ResourceT m a) -> LevelDBT m a -> LevelDBT m a
        q u (LevelDBT (ReaderT b)) =
          LevelDBT $ ReaderT (u . b)

#if MIN_VERSION_monad_control(1,0,0)
instance MonadTransControl LevelDBT where
    type StT LevelDBT a = StT ResourceT (StT (ReaderT DBContext) a)
    liftWith f =
            LevelDBT $ liftWith $ \run ->
                       liftWith $ \run' ->
                       f $ run' . run . unLevelDBT
    restoreT = LevelDBT . restoreT . restoreT

instance (MonadBaseControl b m) => MonadBaseControl b (LevelDBT m) where
    type StM (LevelDBT m) a =  ComposeSt LevelDBT m a
    liftBaseWith = defaultLiftBaseWith
    restoreM     = defaultRestoreM
#else
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
#endif

-- | MonadLevelDB class used by all the public functions in this module.
class ( Monad m
      , MonadThrow m
      , MonadIO m
      , Applicative m
      , MonadResource m
      , MonadBase IO m )
      => MonadLevelDB m where
    -- | Override context for an action - only usable internally for functions
    -- like 'withKeySpace' and 'withOptions'.
    withDBContext :: (DBContext -> DBContext) -> m a -> m a
    -- | Lift a LevelDBT IO action into the current monad.
    liftLevelDB :: LevelDBT IO a -> m a

instance (MonadResourceBase m) => MonadLevelDB (LevelDBT m) where
    liftLevelDB = mapLevelDBT liftIO
    withDBContext = localLDB

-- transformer instances boilerplate; "inspired" by ResourceT
#define INST(M,T, F)                                              \
instance (M, MonadLevelDB m) => MonadLevelDB (T m)                \
    where                                                         \
      liftLevelDB = lift . liftLevelDB                        ; \
      withDBContext f = F (withDBContext f)                     ; \

INST(Monad m,ReaderT r, mapReaderT) --Monad m is a no-op to save another define
INST(Monad m,Maybe.MaybeT, Maybe.mapMaybeT)
INST(Monad m,Identity.IdentityT, Identity.mapIdentityT)
INST(Monad m,List.ListT, List.mapListT)
INST(Monad m,Cont.ContT r, Cont.mapContT)
INST(Monad m,State.StateT s, State.mapStateT )
INST(Monad m,Strict.StateT s, Strict.mapStateT )
INST(Monoid w, Writer.WriterT w, Writer.mapWriterT)
INST(Monoid w, Strict.WriterT w, Strict.mapWriterT)
INST(Monoid w, RWS.RWST r w s, RWS.mapRWST)
INST(Monoid w, Strict.RWST r w s, Strict.mapRWST)

#if MIN_VERSION_mtl(2,2,1)
INST(Monad m, Except.ExceptT e, Except.mapExceptT)
#else
INST(Error.Error e, Error.ErrorT e, Error.mapErrorT)
#endif

#undef INST
-- | alias for LevelDBT IO - useful if you aren't building a custom stack.
type LevelDB a = LevelDBT IO a

-- |Build a context and execute the actions; uses a 'ResourceT' internally.
--
-- tip: you can use the Data.Default (def) method to specify default options e.g.
--
-- > runLevelDB "/tmp/mydb" def (def, def{sync = true}) "My Keyspace" $ do
runLevelDB :: (MonadResourceBase m)
           => FilePath -- ^ path to DB to open/create
           -> Options -- ^ database options to use
           -> RWOptions -- ^ default read/write ops; use 'withOptions' to override
           -> KeySpace -- ^ "Bucket" in which Keys will be unique
           -> LevelDBT m a -- ^ The actions to execute
           -> m a
runLevelDB path dbopt rwopt ks ma = runResourceT $ runLevelDB' path dbopt rwopt ks ma

-- |Same as 'runLevelDB' but doesn't call 'runResourceT'. This gives you the option
-- to manage that yourself
runLevelDB' :: (MonadResourceBase m)
           => FilePath -- ^ path to DB to open/create
           -> Options -- ^ database options to use
           -> RWOptions -- ^ default read/write ops; use 'withOptions' to override
           -> KeySpace -- ^ "Bucket" in which Keys will be unique
           -> LevelDBT m a -- ^ The actions to execute
           -> ResourceT m a
runLevelDB' path dbopt rwopt ks ma = do
    db <- openDB
    mv <- newMVar 0
    ksId <- withSystemContext db mv $ getKeySpaceId ks
    runReaderT (unLevelDBT ma) (DBC db ksId mv rwopt)
  where
    openDB = LDB.open path dbopt
    withSystemContext db mv sctx =
        runReaderT (unLevelDBT sctx) $ DBC db systemKeySpaceId mv rwopt

-- | A helper for runLevelDB using default 'Options' except createIfMissing=True
runCreateLevelDB :: (MonadResourceBase m)
           => FilePath -- ^ path to DB to open/create
           -> KeySpace -- ^ "Bucket" in which Keys will be unique
           -> LevelDBT m a -- ^ The actions to execute
           -> m a
runCreateLevelDB path = runLevelDB path def{createIfMissing=True} def


-- | Fork a LevelDBT IO action and return ThreadId into the current monad.
-- This uses 'resourceForkIO' to handle the reference counting and cleanup resources
-- when the last thread exits.
forkLevelDB :: (MonadLevelDB m)
              => LevelDB ()
              -> m ThreadId
forkLevelDB ma = liftLevelDB $ LevelDBT $
    mapReaderT resourceForkIO (unLevelDBT ma)

-- | Use a local keyspace for the operation. e.g.:
--
-- > runCreateLevelDB "/tmp/mydb" "MyKeySpace" $ do
-- >    put "somekey" "somevalue"
-- >    withKeySpace "Other KeySpace" $ do
-- >        put "somekey" "someother value"
-- >    get "somekey"
-- >
-- > Just "somevalue"
withKeySpace :: (MonadLevelDB m) => KeySpace -> m a -> m a
withKeySpace ks ma = do
    ksId <- getKeySpaceId ks
    withDBContext (\dbc -> dbc { dbcKsId = ksId}) ma

-- | Local Read/Write Options for the action.
withOptions :: (MonadLevelDB m) => RWOptions -> m a -> m a
withOptions opts =
    withDBContext (\dbc -> dbc { dbcRWOptions = opts })

-- | Run a block of get operations based on a single snapshot taken at
-- the beginning of the action. The snapshot will be automatically
-- released when complete.
--
-- This means that you can do put operations in the same block, but you will not see
-- those changes inside this computation.
withSnapshot :: (MonadLevelDB m) => m a -> m a
withSnapshot ma = do
    (db, _, _) <- getDB
    LDB.withSnapshot db $ \ss ->
        withDBContext (\dbc -> dbc {dbcRWOptions = setSnap dbc ss}) ma
  where
    setSnap dbc ss =
        let (ropts, wopts) = dbcRWOptions dbc in
        (ropts {useSnapshot = Just ss}, wopts)


-- | Put a value in the current DB and KeySpace.
put :: (MonadLevelDB m) => Key -> Value -> m ()
put k v = do
    (db, ksId, (_, wopt)) <- getDB
    let packed = ksId <> k
    LDB.put db wopt packed v

-- | Get a value from the current DB and KeySpace.
get :: (MonadLevelDB m) => Key -> m (Maybe Value)
get k = do
    (db, ksId, (ropt, _)) <- getDB
    let packed = ksId <> k
    LDB.get db ropt packed

-- | Delete an entry from the current DB and KeySpace.
delete :: (MonadLevelDB m) => Key -> m ()
delete k = do
    (db, ksId, (_, wopt)) <- getDB
    let packed = ksId <> k
    LDB.delete db wopt packed

-- | Write a batch of operations - use the 'write' and 'deleteB' functions to
-- add operations to the batch list.
runBatch :: (MonadLevelDB m)
          => WriterT WriteBatch m ()
          -> m ()
runBatch wb = do
    (db, _, (_, wopt)) <- getDB
    (_, ops) <- runWriterT wb
    LDB.write db wopt ops

-- | Add a "Put" operation to a WriteBatch -- for use with 'runBatch'.
putB :: (MonadLevelDB m) => Key -> Value -> WriterT WriteBatch m ()
putB k v = do
    (_, ksId, _) <- getDB
    tell [Put (ksId <> k) v]
    return ()

-- | Add a "Del" operation to a WriteBatch -- for use with 'runBatch'.
deleteB :: (MonadLevelDB m) => Key -> WriterT WriteBatch m ()
deleteB k = do
    (_, ksId, _) <- getDB
    tell [Del (ksId <> k)]
    return ()


-- | Scan the keyspace, applying functions and returning results.
-- Look at the documentation for 'ScanQuery' for more information.
--
-- This is essentially a fold left that will run until the 'scanWhile'
-- condition is met or the iterator is exhausted. All the results will be
-- copied into memory before the function returns.
scan :: (MonadLevelDB m)
     => Key  -- ^ Key at which to start the scan.
     -> ScanQuery a b -- ^ query functions to execute -- see 'ScanQuery' docs.
     -> m b
scan k ScanQuery{..} = do
    (db, ksId, (ropt,_)) <- getDB
    withIterator db ropt $ doScan (ksId <> k) ksId
  where
    doScan prefix ksId iter = do
        iterSeek iter prefix
        applyIterate scanInit
      where
        readItem = do
            nk <- iterKey iter
            nv <- iterValue iter
            return $
                if sameKsId nk then (fmap (BS.drop 4) nk, nv) --unkeyspace
                else (Nothing, Nothing)
        applyIterate acc = do
            item <- readItem
            case item of
                (Just nk, Just nv) ->
                    if scanWhile k (nk, nv) acc then do
                        iterNext iter
                        items <- applyIterate acc
                        return $ if scanFilter (nk, nv) then
                                     scanFold (scanMap (nk, nv)) items
                                 else items
                    else return acc
                _ -> return acc
        sameKsId Nothing = False
        sameKsId (Just nk) = BS.take 4 nk == ksId

-- | Structure containing functions used within the 'scan' function. You may want to start
-- with one of the builder/helper funcions such as 'queryItems', which is defined as:
--
-- > queryItems = queryBegins { scanInit = []
-- >                          , scanMap = id
-- >                          , scanFold = (:)
-- >                          }
data ScanQuery a b = ScanQuery {
                         -- | starting value for fold/reduce
                         scanInit :: b

                         -- | scan will continue until this returns false
                       , scanWhile :: Key -> Item -> b -> Bool

                         -- | map or transform an item before it is reduced/accumulated
                       , scanMap ::  Item -> a

                         -- | filter function - return 'False' to leave
                         -- this 'Item' out of the result
                       , scanFilter :: Item -> Bool

                         -- | accumulator/fold function e.g. (:)
                       , scanFold :: a -> b -> b
                       }

-- | A partial ScanQuery helper; this query will find all keys that begin with the Key argument
-- supplied to scan.
--
-- Requires an 'scanInit', a 'scanMap' and a 'scanFold' function.
queryBegins :: ScanQuery a b
queryBegins = ScanQuery
                   { scanWhile = \ prefix (nk, _) _ ->
                                          BS.length nk >= BS.length prefix
                                          && BS.take (BS.length prefix) nk == prefix
                   , scanInit = error "No scanInit provided."
                   , scanMap = error "No scanMap provided."
                   , scanFilter = const True
                   , scanFold = error "No scanFold provided."
                   }

-- | A basic ScanQuery helper; this query will find all keys that begin the Key argument
-- supplied to scan, and returns them in a list of 'Item'.
--
-- Does not require any function overrides.
queryItems :: ScanQuery Item [Item]
queryItems = queryBegins { scanInit = []
                       , scanMap = id
                       , scanFold = (:)
                       }

-- | a ScanQuery helper with defaults for queryBegins and a list result; requires a map function e.g.:
--
-- > scan "encoded-values:" queryList { scanMap = \(_, v) -> decode v }
queryList :: ScanQuery a [a]
queryList  = queryBegins { scanInit = []
                       , scanFilter = const True
                       , scanFold = (:)
                       }

-- | a ScanQuery helper to count items beginning with Key argument.
queryCount :: (Num a) => ScanQuery a a
queryCount = queryBegins { scanInit = 0
                         , scanMap = const 1
                         , scanFold = (+) }

-- | Map/transform the monad below the LevelDBT
mapLevelDBT  :: (m a -> n b) -> LevelDBT m a -> LevelDBT n b
mapLevelDBT f ma = LevelDBT $
    mapReaderT (transResourceT f) $ unLevelDBT ma

getDB :: (MonadLevelDB m) => m (DB, KeySpaceId, RWOptions)
getDB = liftLevelDB $ asksLDB (\dbc ->
        (dbcDb dbc, dbcKsId dbc, dbcRWOptions dbc))


-- | This little dance with asksLDB & localLDB let's us get away from
-- exposing MonadReader DBContext in LevelDBT.
asksLDB :: (MonadResourceBase m) => (DBContext -> a) -> LevelDBT m a
asksLDB = LevelDBT . asks

localLDB :: (MonadResourceBase m)
         => (DBContext -> DBContext)
         -> LevelDBT m a -> LevelDBT m a
localLDB f ma = LevelDBT $ local f (unLevelDBT ma)


defaultKeySpaceId :: KeySpaceId
defaultKeySpaceId = "\0\0\0\0"

systemKeySpaceId ::  KeySpaceId
systemKeySpaceId = "\0\0\0\1"

systemKeySpace :: KeySpace
systemKeySpace = "system"

getKeySpaceId :: (MonadLevelDB m) => KeySpace -> m KeySpaceId
getKeySpaceId ks
    | ks == ""  = return defaultKeySpaceId
    | ks == systemKeySpace = return systemKeySpaceId
    | otherwise = liftLevelDB $ withKeySpace systemKeySpace $ do
        findKS <- get $ "keyspace:" <> ks
        case findKS of
            (Just foundId) -> return foundId
            Nothing -> do -- define new KS
                nextId <- incr "max-keyspace-id"
                put ("keyspace:" <> ks) nextId
                return nextId
  where
    incr k = do
        mv <- takeMVarDBC
        curId <- case mv of
            0 -> initKeySpaceIdMV k >> takeMVarDBC
            n -> return n
        let nextId = curId + 1
        put k $ encode nextId
        putMVarDBC nextId
        return $ encode curId
    initKeySpaceIdMV k = do
        findMaxId <- get k
        case findMaxId of
            (Just found) -> putMVarDBC $ decodeKsId found
            Nothing      -> putMVarDBC 2 -- first user keyspace
    putMVarDBC v = asksLDB dbcSyncMV >>= flip putMVar v
    takeMVarDBC = asksLDB dbcSyncMV >>= takeMVar
    decodeKsId bs =
        case decode bs of
            Left e -> error $
                "Error decoding Key Space ID: " <> show bs <> "\n" <> e
            Right i -> i :: Word32
