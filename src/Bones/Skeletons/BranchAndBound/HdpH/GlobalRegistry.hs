module Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
  (
    registry

  , getRefFromRegistry
  , addRefToRegistry

  , readFromRegistry
  , addToRegistry
  ) where

import           Data.IORef      (IORef, atomicWriteIORef, newIORef, readIORef)
import qualified Data.Map.Strict as Map (Map, empty, insert, lookup)

import           System.IO.Unsafe      (unsafePerformIO)

--------------------------------------------------------------------------------
-- Global (process local) Registry
--------------------------------------------------------------------------------

registry :: IORef (Map.Map Int (IORef a))
{-# NOINLINE registry #-}
registry = unsafePerformIO $ newIORef Map.empty

getRefFromRegistry :: Int -> IO (IORef a)
getRefFromRegistry k = do
  r <- readIORef registry
  case Map.lookup k r of
    Nothing -> error $ " Could not find key: " ++ show k ++ " in global registry."
    Just x  -> return x

readFromRegistry :: Int -> IO a
readFromRegistry k = getRefFromRegistry k >>= readIORef

addRefToRegistry :: Int -> IORef a -> IO ()
addRefToRegistry k v = do
  reg <- readIORef registry
  atomicWriteIORef registry $ Map.insert k v reg

addToRegistry :: Int -> a -> IO ()
addToRegistry k v = newIORef v >>= addRefToRegistry k
