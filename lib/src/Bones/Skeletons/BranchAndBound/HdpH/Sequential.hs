-- Sequential Branch and Bound Skeleton. Makes use of HdpH features and a global
-- repository for storing the solutions. Useful for assessing the overheads of parallelism.
module Bones.Skeletons.BranchAndBound.HdpH.Sequential
  (
    search
  ) where

import Control.Parallel.HdpH

import Control.Monad (when, unless)
import Data.IORef (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

-- Assumes any global space state is already initialised
search :: Bool -> BBNode a b s -> BAndBFunctions a b s -> Par a
search pl root@(ssol, sbnd, _) fns = do
  io $ addToRegistry solutionKey (ssol, sbnd)
  io $ addToRegistry boundKey sbnd
  expand pl root fns
  io $ fst <$> readFromRegistry solutionKey

expand :: Bool -> BBNode a b s -> BAndBFunctions a b s -> Par ()
expand pl root fns = go1 root
  where
    go1 n = orderedGenerator fns n >>= go

    go [] = return ()

    go (n:ns) = do
      gbnd <- io $ readFromRegistry boundKey

      -- Manually force evaluation (used to avoid fully evaluating the node list
      -- if it's not needed)
      n' <- n

      lbnd <- pruningHeuristic fns n'
      case compareB fns lbnd gbnd of
        GT -> do
         when (compareB fns (bound n') gbnd == GT) (updateLocalBoundAndSol n' fns)
         go1 n' >> go ns
        _  -> unless pl $ go ns

-- Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol :: BBNode a b s -> BAndBFunctions a b s -> Par ()
updateLocalBoundAndSol n@(sol, bnd, _) fns = do
  -- Bnd
  bndRef <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' bndRef $ \b ->
    if compareB fns (bound n) b == GT then (bnd, ()) else (b, ())

  -- Sol
  solRef <- io $ getRefFromRegistry solutionKey
  _ <- io $ atomicModifyIORef' solRef $ \prev@(_,b) ->
        if compareB fns (bound n) b == GT
            then ((sol, bnd), True)
            else (prev, False)

  return ()
