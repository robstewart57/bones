module Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
  (
    registry

  , getRefFromRegistry
  , addRefToRegistry

  , readFromRegistry
  , addToRegistry

  , addGlobalSearchSpaceToRegistry
  , getGlobalSearchSpace

  , initRegistryBound

  , getUserState
  , putUserState

  , searchSpaceKey
  , solutionKey
  , solutionSignalKey
  , boundKey
  , userStateKey
  ) where

import           Control.Parallel.HdpH (Closure, Thunk(..), Par, io)

import           Data.IORef      (IORef, atomicWriteIORef, newIORef, readIORef)

import Data.Array.Base (unsafeWrite, unsafeRead)
import Data.Array.IO (IOArray)
import qualified Data.Array.IO as A

import           System.IO.Unsafe      (unsafePerformIO)

--------------------------------------------------------------------------------
-- Global (process local) Registry
--------------------------------------------------------------------------------

registry :: IOArray Int (IORef a)
{-# NOINLINE registry #-}
registry = unsafePerformIO $ A.newArray_ (0,3)

getRefFromRegistry :: Int -> IO (IORef a)
{-# INLINE getRefFromRegistry #-}
getRefFromRegistry = unsafeRead registry

readFromRegistry :: Int -> IO a
readFromRegistry k = getRefFromRegistry k >>= readIORef

-- Note: Adding new values the registry is *not thread safe*. This is not an
-- issue in practice as global state is initialised on each node once before
-- starting computation. Updating values may be made thread safe using atomic
-- modify IORef.
addRefToRegistry :: Int -> IORef a -> IO ()
{-# INLINE addRefToRegistry #-}
addRefToRegistry = unsafeWrite registry

addToRegistry :: Int -> a -> IO ()
addToRegistry k v = newIORef v >>= addRefToRegistry k

-- Functions a user can use to manipulate state
getUserState :: IO a
getUserState = readFromRegistry userStateKey

putUserState :: a -> IO ()
putUserState = addToRegistry userStateKey

--------------------------------------------------------------------------------
-- Skeleton Interface
--------------------------------------------------------------------------------

searchSpaceKey :: Int
{-# INLINE searchSpaceKey #-}
searchSpaceKey = 0

solutionKey :: Int
{-# INLINE solutionKey #-}
solutionKey = 1

solutionSignalKey :: Int
{-# INLINE solutionSignalKey #-}
solutionSignalKey = 3

boundKey :: Int
{-# INLINE boundKey #-}
boundKey = 2

userStateKey :: Int
{-# INLINE userStateKey #-}
userStateKey = 3

initRegistryBound :: Closure a -> Thunk (Par ())
initRegistryBound bnd = Thunk $ io (addToRegistry boundKey bnd)

addGlobalSearchSpaceToRegistry :: IORef a -> IO ()
addGlobalSearchSpaceToRegistry = addRefToRegistry searchSpaceKey

getGlobalSearchSpace :: IO a
getGlobalSearchSpace = readFromRegistry searchSpaceKey
