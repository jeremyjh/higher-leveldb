{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.LevelDB.Higher.Core
    (
    -- * Basic types
      Key, Value, Item, KeySpace, KeySpaceId
    -- * Basic operations
    , get, put, delete
    -- * Batch operations
    , runBatch, runBatchIO, putB, deleteB, BatchWriter
    -- * Scans
    , scan, ScanQuery(..), queryItems, queryList, queryBegins, queryCount
    -- * Context modifiers
    , withKeySpace, withOptions, withSnapshot, currentKeySpace
    , forkLevelDB
    -- * Monadic Types and Operations
    , MonadLevelDB(..), LevelDBT, LevelDB
    , mapLevelDBT
    , runLevelDB, runLevelDB', runCreateLevelDB
    -- * Re-exports
    , runResourceT
    , Options(..), ReadOptions(..), WriteOptions(..), RWOptions
    , WriteBatch, def
    , MonadUnsafeIO, MonadThrow, MonadResourceBase
    ) where

import           Database.LevelDB.Higher.Internal.Types
import           Database.LevelDB.Higher.Class

import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Monad.Identity
import           Data.Word                         (Word32)


import           Control.Concurrent.MVar.Lifted
import           Control.Concurrent                (ThreadId)

import qualified Data.ByteString                   as BS
import           Data.Serialize                    (encode, decode)

import           Data.Default                      (def)
import qualified Database.LevelDB                  as LDB
import           Database.LevelDB
    hiding (put, get, delete, write, withSnapshot)
import           Control.Monad.Trans.Resource


instance (MonadResourceBase m) => MonadLevelDB (LevelDBT m) where
    liftLevelDB = mapLevelDBT liftIO
    withDBContext = localLDB


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
    runReaderT (unLevelDBT ma) (DBC db ksId mv rwopt ks)
  where
    openDB = LDB.open path dbopt
    withSystemContext db mv sctx =
        runReaderT (unLevelDBT sctx) $ DBC db systemKeySpaceId mv rwopt systemKeySpaceId

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
    mapReaderT resourceForkIO $ unLevelDBT ma

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
    withDBContext (\dbc -> dbc { dbcKsId = ksId
                               , dbcKeySpace = ks}) ma

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

type BatchWriter m = ReaderT KeySpaceId (WriterT WriteBatch m) ()

-- | Write an atomic batch of operations - use the 'putB' and 'deleteB' functions to
-- add operations to the batch list. This is the "pure" version - use 'runBatchIO'
-- if you need to execute LevelDB or other IO actions in the same block.
runBatch :: (MonadLevelDB m)
         => BatchWriter Identity
         -> m ()
runBatch bw = do
    (db, ksId, (_, wopt)) <- getDB
    let wtr = runReaderT bw ksId
    let ops = execWriter wtr
    LDB.write db wopt ops

-- | Write an atomic batch of operations - use the 'putB' and 'deleteB' functions to
-- add operations to the batch list. Use this version if you need to execute LevelDB
-- or other IO actions in the same block. This may be required for example if you are
-- building a batch from an I/O stream. Where possible use 'runBatch' as it will prevent
-- mistakenly using the standalone put/delete functions in what you think is an atomic batch.
runBatchIO :: (MonadLevelDB m)
           => BatchWriter m
           -> m ()
runBatchIO bw = do
    (db, ksId, (_, wopt)) <- getDB
    let wtr = runReaderT bw ksId
    ops <- execWriterT wtr
    LDB.write db wopt ops

-- | Add a "Put" operation to a WriteBatch -- for use with 'runBatch' and 'runBatchIO'.
putB :: (Monad m) => Key -> Value -> BatchWriter m
putB k v = do
    ksId <- ask
    tell [Put (ksId <> k) v]

-- | Add a "Del" operation to a WriteBatch -- for use with 'runBatch'.
deleteB :: (Monad m) => Key -> BatchWriter m
deleteB k = do
    ksId <- ask
    tell [Del (ksId <> k)]

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
scan k scanQuery = do
    (db, ksId, (ropt,_)) <- getDB
    withIterator db ropt $ doScan (ksId <> k)
  where
    doScan prefix iter = do
        iterSeek iter prefix
        applyIterate initV
      where
        readItem = do
            nk <- iterKey iter
            nv <- iterValue iter
            return (fmap (BS.drop 4) nk, nv) --unkeyspace
        applyIterate acc = do
            item <- readItem
            case item of
                (Just nk, Just nv) ->
                    if whileFn (nk, nv) acc then do
                        iterNext iter
                        items <- applyIterate acc
                        return $ if filterFn (nk, nv) then
                                     reduceFn (mapFn (nk, nv)) items
                                 else items
                    else return acc
                _ -> return acc
    initV = scanInit scanQuery
    whileFn = scanWhile scanQuery k
    mapFn = scanMap scanQuery
    filterFn = scanFilter scanQuery
    reduceFn = scanFold scanQuery

-- | Structure containing functions used within the 'scan' function. You may want to start
-- with one of the builder/helper funcions such as 'queryItems', which is defined as:
--
-- >queryItems = queryBegins { scanInit = []
-- >                         , scanMap = id
-- >                         , scanFold = (:)
-- >                         }
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

currentKeySpace :: (MonadLevelDB m) => m KeySpace
currentKeySpace = liftLevelDB $ asksLDB dbcKeySpace


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
