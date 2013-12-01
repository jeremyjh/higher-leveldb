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

module Database.LevelDB.Higher
    (
      -- $intro
      -- * Introduction
      MonadLevelDB(..)
    , module X
    ) where

import Database.LevelDB.Higher.Core as X
import Database.LevelDB.Higher.Store as X

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
-- >Just "this is a value"
--
