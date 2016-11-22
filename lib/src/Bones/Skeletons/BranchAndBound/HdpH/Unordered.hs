{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Unordered
  (
    declareStatic
  , search
  -- , findSolution
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl, Thunk (Thunk),
                                        allNodes, declare, get, io, mkClosure, myNode,
                                        fork, one, spawn, new, glob, unClosure, toClosure,
                                        static)

import           Control.Monad         (when)

import           Bones.Skeletons.BranchAndBound.HdpH.Common hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Common as Common
import           Bones.Skeletons.BranchAndBound.HdpH.Types hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Types as Types (declareStatic)
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import           Bones.Skeletons.BranchAndBound.HdpH.Util

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

-- | Perform a backtracking search using a skeleton with distributed work
-- spawning. Makes no guarantees on task ordering.
search ::
          Bool                              -- ^ Enable pruneLevel optimisation
       -> Int                               -- ^ Depth in the tree to spawn to. 0 implies top level tasks.
       -> BBNode a b s                      -- ^ Root Node
       -> Closure (BAndBFunctions g a b s)    -- ^ Higher order B&B functions
       -> Closure (ToCFns a b s)            -- ^ Explicit toClosure instances
       -> Par a                             -- ^ The resulting solution after the search completes
search pl depth root fs' toC = do
  master <- myNode
  nodes  <- allNodes

  -- Configuration initial state
  initLocalRegistries nodes (bound root) toC
  initSolutionOnMaster root toC

  -- Gen top level
  space <- io getGlobalSearchSpace
  ts <- orderedGenerator (unClosure fs') space root >>= sequence
  let tasks = map (createChildren depth master) ts

  mapM (spawn one) tasks >>= mapM_ get

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      createChildren d m n =
          let n' = toCnode (unClosure toC) n
          in $(mkClosure [| branchAndBoundChild (d, m, pl, n', fs', toC) |])

branchAndBoundChild ::
    ( Int
    , Node
    , Bool
    , Closure (BBNode a b s)
    , Closure (BAndBFunctions g a b s)
    , Closure (ToCFns a b s))
    -> Thunk (Par (Closure ()))
branchAndBoundChild (spawnDepth, parent, pl, n, fs', toC) =
  Thunk $ do
    let fs = unClosure fs'
    gbnd <- io $ readFromRegistry boundKey
    space <- io getGlobalSearchSpace

    lbnd <- pruningHeuristic fs space (unClosure n)
    case compareB fs lbnd gbnd  of
      GT -> branchAndBoundExpand pl spawnDepth parent n fs' toC >> return toClosureUnit
      _  -> return toClosureUnit

branchAndBoundExpand ::
       Bool
    -> Int
    -> Node
    -> Closure (BBNode a b s)
    -> Closure (BAndBFunctions g a b s)
    -> Closure (ToCFns a b s)
    -> Par ()
branchAndBoundExpand pl depth parent n fs toC
  | depth == 0 = let fsl  = unClosure fs
                     toCl = unClosure toC
                 in do
                    space <- io getGlobalSearchSpace
                    expandSequential pl parent (unClosure n) space fs fsl toCl
  | otherwise  = do
        -- Duplication from the main search function, extract
        let fs' = unClosure fs
        space <- io getGlobalSearchSpace
        ns <- orderedGenerator fs' space (unClosure n) >>= sequence

        let tasks = map (createChildren (depth - 1) parent) ns

        mapM (spawn one) tasks >>= mapM_ get

  where
      createChildren d m n =
          let n' = toCnode (unClosure toC) n
          in $(mkClosure [| branchAndBoundChild (d, m, pl, n', fs, toC) |])

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'branchAndBoundChild)
  , declare $(static 'initRegistryBound)
  , Common.declareStatic
  , Types.declareStatic
  ]
