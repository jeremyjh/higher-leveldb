{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}

module Database.LevelDB.Higher.Class where

import           Database.LevelDB.Higher.Internal.Types

import           Control.Applicative               (Applicative)
import           Control.Monad.Trans.Resource

import           Control.Monad.Base                (MonadBase(..))
import           Data.Monoid                       (Monoid)

import           Control.Monad.Reader
   (MonadIO, ReaderT, mapReaderT, lift)

import qualified Control.Monad.Trans.Cont          as Cont
import qualified Control.Monad.Trans.Identity      as Identity
import qualified Control.Monad.Trans.List          as List
import qualified Control.Monad.Trans.Maybe         as Maybe
import qualified Control.Monad.Trans.Error         as Error
import qualified Control.Monad.Trans.State         as State
import qualified Control.Monad.Trans.Writer        as Writer
import qualified Control.Monad.Trans.RWS           as RWS
import qualified Control.Monad.Trans.RWS.Strict    as Strict
import qualified Control.Monad.Trans.State.Strict  as Strict
import qualified Control.Monad.Trans.Writer.Strict as Strict

-- | MonadLevelDB class used by all the public functions in this module.
class ( Monad m
      , MonadThrow m
      , MonadUnsafeIO m
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
INST(Error.Error e, Error.ErrorT e, Error.mapErrorT)
INST(Monoid w, Writer.WriterT w, Writer.mapWriterT)
INST(Monoid w, Strict.WriterT w, Strict.mapWriterT)
INST(Monoid w, RWS.RWST r w s, RWS.mapRWST)
INST(Monoid w, Strict.RWST r w s, Strict.mapRWST)
#undef INST
