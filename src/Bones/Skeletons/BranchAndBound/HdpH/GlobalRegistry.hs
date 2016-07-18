{-# LANGUAGE TemplateHaskell #-}
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
  , initLocalRegistries

  , getUserState
  , putUserState

  , searchSpaceKey
  , solutionKey
  , solutionSignalKey
  , boundKey
  , userStateKey
  ) where

import           Control.Parallel.HdpH (Closure, Thunk(..), Par, io, unClosure,
                                        Node, pushTo, mkClosure)

import           Control.Monad (forM_)

import           Data.IORef      (IORef, atomicWriteIORef, newIORef, readIORef)

import           Data.Array.Base (unsafeWrite, unsafeRead)
import           Data.Array.IO (IOArray)
import qualified Data.Array.IO as A

import           System.IO.Unsafe      (unsafePerformIO)

import           Bones.Skeletons.BranchAndBound.HdpH.Types

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

-- | Ensure all nodes know of the starting bound
initLocalRegistries :: [Node] -- ^ Nodes to initialise
                    -> b      -- ^ Bound value
                    -> Closure (ToCFns a b s) -- ^ Explicit toClosure instances
                    -> Par () -- ^ Side-effect only
initLocalRegistries nodes bnd toCFns =
  let toC    = unClosure toCFns
      toCBnd = unClosure $ toCb toC
      bnd' = toCBnd bnd
  in
  forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd' |]) n

initRegistryBound :: Closure a -> Thunk (Par ())
initRegistryBound bnd = Thunk $ io (addToRegistry boundKey (unClosure bnd))

addGlobalSearchSpaceToRegistry :: IORef a -> IO ()
addGlobalSearchSpaceToRegistry = addRefToRegistry searchSpaceKey

getGlobalSearchSpace :: IO a
getGlobalSearchSpace = readFromRegistry searchSpaceKey
