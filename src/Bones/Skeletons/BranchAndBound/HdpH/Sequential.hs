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
search :: BBNode a b s -> BAndBFunctionsL a b s -> Par a
search root@(ssol, sbnd, _) fns = do
  io $ addToRegistry solutionKey (ssol, sbnd)
  io $ addToRegistry boundKey sbnd
  expand root fns
  io $ fst <$> readFromRegistry solutionKey

expand :: BBNode a b s -> BAndBFunctionsL a b s -> Par ()
expand root fns = go1 root
  where
    go1 n = orderedGeneratorL fns n >>= go

    go [] = return ()

    go (n:ns) = do
      bnd <- io $ readFromRegistry boundKey

      sp <- pruningPredicateL fns n bnd
      case sp of
        Prune      -> go ns
        PruneLevel -> return ()
        NoPrune    -> do
         when (strengthenL fns n bnd) (updateLocalBoundAndSol n fns)
         go1 n >> go ns

-- Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol :: BBNode a b s -> BAndBFunctionsL a b s -> Par ()
updateLocalBoundAndSol n@(sol, bnd, _) fns = do
  -- Bnd
  bndRef <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' bndRef $ \b ->
    if strengthenL fns n b then (bnd, ()) else (b, ())

  -- Sol
  solRef <- io $ getRefFromRegistry solutionKey
  _ <- io $ atomicModifyIORef' solRef $ \prev@(_,b) ->
        if strengthenL fns n b
            then ((sol, bnd), True)
            else (prev, False)

  return ()
