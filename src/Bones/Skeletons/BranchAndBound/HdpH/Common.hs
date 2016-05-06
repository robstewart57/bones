-- Module of Common functionality used by multiple skeletons

{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Common
(
  -- Initialisation
  initSolutionOnMaster

  -- State Updates
  , updateLocalBounds
  , notifyParentOfNewBound

  -- Static Decl
  , declareStatic
)

where

import Control.Parallel.HdpH (Par, Closure, unClosure, io, Thunk(..), mkClosure,
                              spawnAt, Node, get, allNodes, pushTo, toClosure,
                              StaticDecl, declare, static)

import Control.Monad (when)

import Data.IORef            (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types hiding (declareStatic)
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry


-- | Ensure the initial solution is set on the master node. This is important
-- for ensuring there is a value to get (even if it is empty) at the end of the
-- run.
initSolutionOnMaster :: a -- ^ Starting Solution
                     -> b -- ^ Starting Bound
                     -> Closure (ToCFns a b c s ) -- ^ Explicit toClosure instances
                     -> Par () -- ^ Side-effect only
initSolutionOnMaster sol bnd toC =
  let toCsol = unClosure (toCa (unClosure toC))
      solC   = toCsol sol
      -- We keep solutions in closure form until we ask for them. Bounds are
      -- kept unClosured for faster comparison.
  in io $ addToRegistry solutionKey (solC, bnd)

-- | Update local bounds
updateLocalBounds :: b
                  -- ^ New bound
                  -> UpdateBoundFn b
                  -- ^ Functions (to access updateBound function)
                  -> Par ()
                  -- ^ Side-effect only function
updateLocalBounds bnd updateB = do
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if updateB bnd b then (bnd, ()) else (b, ())

updateLocalBoundsT :: (Closure b, Closure (UpdateBoundFn b))
                   -> Thunk (Par ())
updateLocalBoundsT (bnd, bndfn) = Thunk $ updateLocalBounds (unClosure bnd) (unClosure bndfn)

-- | Push new bounds to the master node. Also sends the new solution to avoid
--   additional messages.
notifyParentOfNewBound :: Node
                       -- ^ Master node
                       -> (Closure a, Closure b)
                       -- ^ (Solution, Bound)
                       -> Closure (UpdateBoundFn b)
                       -- ^ B&B Functions
                       -> Par ()
                       -- ^ Side-effect only function
notifyParentOfNewBound parent solPlusBnd fs = do
  -- We wait for an ack (get) to avoid a race condition where all children
  -- finish before the final updateBest task is ran on the master node.
  spawnAt parent $(mkClosure [| updateParentBoundT (solPlusBnd, fs) |]) >>= get
  return ()

-- | Update the global solution with the new solution. If this succeeds then
--   tell all other nodes to update their local information.
updateParentBoundT :: ((Closure a, Closure b), Closure (UpdateBoundFn b))
                     -- ^ ((newSol, newBound), UpdateBound)
                  -> Thunk (Par (Closure ()))
                     -- ^ Side-effect only function
updateParentBoundT ((sol, bnd), updateB) = Thunk $ do
  ref     <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(_, b) ->
                if unClosure updateB (unClosure bnd) b
                    then ((sol, unClosure bnd), True)
                    else (prev                , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| updateLocalBoundsT (bnd, updateB) |])) ns

  return $ toClosure ()

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'updateParentBoundT)
  , declare $(static 'updateLocalBoundsT)
  ]
