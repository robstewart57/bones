-- Sequential Branch and Bound Skeleton. Makes use of HdpH features and a global
-- repository for storing the solutions. Useful for assessing the overheads of parallelism.
module Bones.Skeletons.BranchAndBound.HdpH.Sequential
  (
    search
  ) where

import Control.Parallel.HdpH

import Control.Monad (when)
import Data.IORef (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

-- Assumes any global space state is already initialised
search :: NodeBB a b s -> BAndBFunctionsL a b s -> Par a
search root@(ssol, sbnd, _) fns = do
  io $ addToRegistry solutionKey (ssol, sbnd)
  io $ addToRegistry boundKey sbnd
  expand root fns
  io $ fst <$> readFromRegistry solutionKey

expand :: NodeBB a b s -> BAndBFunctionsL a b s -> Par ()
expand root fns = go1 root
  where
    go1 n = orderedGenerator fns n >>= go

    go [] = return ()

    go (n:ns) = do
      bnd <- io $ readFromRegistry boundKey

      sp <- pruningPredicateL fns n bnd
      case sp of
        Prune      -> go sol ns cs
        PruneLevel -> return ()
        NoPrune    -> do
         when (strengthenL fns n)
            updateLocalBoundAndSol n fns

          go1 n >> go ns

-- TODO: Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol :: NodeBB a b s -> BAndBFunctionsL a b c s -> Par ()
updateLocalBoundAndSol n@(sol, bnd, _) fns = do
  -- Bnd
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if strengthen fns n b then (bnd, ()) else (b, ())

  -- Sol
  ref <- io $ getRefFromRegistry solutionKey
  io $ atomicModifyIORef' ref $ \prev@(_,b) ->
        if strength fns n b
            then ((sol, bnd), True)
            else (prev, False)

  return ()
