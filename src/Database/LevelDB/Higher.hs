{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving #-}

module Database.LevelDB.Higher
    ( get, put, delete
    , scan, ScanQuery(..), queryItems, queryList, queryBegins
    , LevelDB, runLevelDB, withKeySpace
    , Key, Value, KeySpace
    ) where


import           Control.Monad.Reader

import           Data.Int                         (Int32)
import           Data.Monoid                      ((<>))

import           Control.Applicative              (Applicative)
import           Control.Arrow                    ((&&&))
import           Control.Monad.Base               (MonadBase)

import           Control.Concurrent.MVar.Lifted

import qualified Data.ByteString                   as BS
import           Data.ByteString                   (ByteString)
import           Data.Serialize                    (encode, decode)

import           Data.Default                      (def)
import qualified Database.LevelDB                  as LDB
import           Database.LevelDB                  hiding (put, get, delete)
import           Control.Monad.Trans.Resource      (ResourceT
                                                   , MonadUnsafeIO
                                                   , MonadThrow)

type Key = ByteString
type Value = ByteString
type KeySpace = ByteString
type KeySpaceId = ByteString
type Item = (Key, Value)

-- | Reader-based data context API
--
-- Context contains database handle and KeySpace
data DBContext = DBC { dbcDb :: DB
                     , dbcKsId :: KeySpaceId
                     , dbcSyncMV :: MVar Int32
                     }
instance Show (DBContext) where
    show = (<>) "KeySpaceID: " . show . dbcKsId

-- | LevelDB Monad provides a context for database operations provided in this module
--
-- Use 'runLevelDB'
newtype LevelDB a = DBCIO {unDBCIO :: ReaderT DBContext (ResourceT IO) a }
    deriving ( Functor, Applicative, Monad
             , MonadIO, MonadBase IO, MonadReader DBContext
             , MonadResource, MonadUnsafeIO, MonadThrow )

instance Show (LevelDB a) where
    show = asks show

-- | Specify a filepath to use for the database (will create if not there)
-- Also specify an application-defined keyspace in which keys will be guaranteed unique
runLevelDB :: FilePath -> KeySpace -> LevelDB a -> IO a
runLevelDB dbPath ks ctx = runResourceT $ do
    db <- openDB dbPath
    mv <- newMVar 0
    ksId <- withSystemContext db mv $ getKeySpaceId ks
    runReaderT (unDBCIO ctx) (DBC db ksId mv)
  where
    openDB path =
        LDB.open path
            LDB.defaultOptions{LDB.createIfMissing = True, LDB.cacheSize= 2048}
    withSystemContext db mv sctx =
        runReaderT (unDBCIO sctx) $ DBC db systemKeySpaceId mv

-- | Override keyspace with a local keyspace for an (block) action(s)
--
withKeySpace :: KeySpace -> LevelDB a -> LevelDB a
withKeySpace ks a = do
    ksId <- getKeySpaceId ks
    local (\dbc -> dbc { dbcKsId = ksId}) a

put :: Key -> Value -> LevelDB ()
put k v = do
    (db, ksId) <- asks $ dbcDb &&& dbcKsId
    let packed = ksId <> k
    liftResourceT $ LDB.put db def packed v

get :: Key -> LevelDB (Maybe Value)
get k = do
    (db, ksId) <- asks $ dbcDb &&& dbcKsId
    let packed = ksId <> k
    liftResourceT $ LDB.get db def packed

delete :: Key -> LevelDB ()
delete k = do
    (db, ksId) <- asks $ dbcDb &&& dbcKsId
    let packed = ksId <> k
    liftResourceT $ LDB.delete db def packed

-- | Structure containing functions used within the 'scan' function
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
                           , scanReduce :: a -> b -> b
                           }

-- | a basic ScanQuery helper that defaults scanWhile to continue while
-- the key argument supplied to scan matches the beginning of the key returned
-- by the iterator
--
-- requires an 'scanInit', a 'scanMap' and a 'scanReduce' function
queryBegins :: ScanQuery a b
queryBegins = ScanQuery
                   { scanWhile = \ prefix (nk, _) _ ->
                                          BS.length nk >= BS.length prefix
                                          && BS.take (BS.length nk -1) nk == prefix
                   , scanInit = error "No scanInit provided."
                   , scanMap = error "No scanMap provided."
                   , scanFilter = const True
                   , scanReduce = error "No scanReduce provided."
                   }

-- | a ScanQuery helper that will produce the list of items as-is
-- while the key matches as queryBegins
--
-- does not require any functions though they could be substituted
queryItems :: ScanQuery Item [Item]
queryItems = queryBegins { scanInit = []
                       , scanMap = id
                       , scanReduce = (:)
                       }

-- | a ScanQuery helper with defaults for a list result; requires a map function
--
-- while the key matches as queryBegins
queryList :: ScanQuery a [a]
queryList  = queryBegins { scanInit = []
                       , scanFilter = const True
                       , scanReduce = (:)
                       }

-- | Scan the keyspace, applying functions and returning results
-- Look at the documentation for 'ScanQuery' for more information.
--
-- This is essentially a fold left that will run until the 'scanWhile'
-- condition is met or the iterator is exhausted. All the results will be
-- copied into memory before the function returns.
scan :: Key  -- ^ Key at which to start the scan
     -> ScanQuery a b
     -> LevelDB b
scan k scanQuery = do
    (db, ksId) <- asks $ dbcDb &&& dbcKsId
    liftResourceT $ withIterator db def $ doScan (ksId <> k)
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
    reduceFn = scanReduce scanQuery

defaultKeySpaceId :: KeySpaceId
defaultKeySpaceId = "\0\0\0\0"

systemKeySpaceId ::  KeySpaceId
systemKeySpaceId = "\0\0\0\1"

getKeySpaceId :: KeySpace -> LevelDB KeySpaceId
getKeySpaceId ks
    | ks == ""  = return defaultKeySpaceId
    | ks == "system" = return systemKeySpaceId
    | otherwise = do
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
    putMVarDBC v = asks dbcSyncMV >>= flip putMVar v
    takeMVarDBC = asks dbcSyncMV >>= takeMVar
    decodeKsId bs =
        case decode bs of
            Left e -> error $
                "Error decoding Key Space ID: " <> show bs <> "\n" <> e
            Right i -> i :: Int32
