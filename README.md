## Example

```Haskell
{-# LANGUAGE OverloadedStrings #-}
import Database.LevelDB.Higher

main =
    runCreateLevelDB "/tmp/mydb" "MyKeySpace" $ do
        put "key:1" "this is a value"
        put "key:2" "another value"
        scan "key:" queryItems

> [("key:1","this is a value"),("key:2","another value")])

```

## Summary

Higher LevelDB provides a rich monadic API for working with [leveldb] (http://code.google.com/p/leveldb) databases. It uses the leveldb-haskell bindings to the C++ library. The LevelDBT transformer is a Reader that maintains a database context with the open database as well as default read and write options. It also manages a concept called a KeySpace, which is a bucket scheme that provides a low (storage) overhead named identifier to segregate data. Finally it wraps a ResourceT which is required for use of leveldb-haskell functions.

The other major feature is the scan function and its ScanQuery structure that provides a map / fold abstraction over the Iterator exposed by leveldb-haskell.
