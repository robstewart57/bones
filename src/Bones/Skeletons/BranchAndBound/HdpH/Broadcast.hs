{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Broadcast
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
search :: Int                               -- ^ Depth in the tree to spawn to. 0 implies top level tasks.
       -> BBNode (a, b, s)                  -- ^ Root Node
       -> Closure (BAndBFunctions a b s)    -- ^ Higher order B&B functions
       -> Closure (ToCFns a b s)            -- ^ Explicit toClosure instances
       -> Par a                             -- ^ The resulting solution after the search completes
search depth root space bnd fs' toC = do
  master <- myNode
  nodes  <- allNodes

  -- Configuration initial state
  initLocalRegistries nodes bnd toC
  initSolutionOnMaster root toC

  -- Gen top level
  ts <- unClosure (orderedGenerator (unClousre fs')) root
  let tasks = map (createChildren depth master) ts

  mapM (spawn one) tasks >>= mapM_ get

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      createChildren d m n =
          let n' = toClosure n
          in $(mkClosure [| branchAndBoundChild (d, m, n', fs', toC) |])

branchAndBoundChild ::
    ( Int
    , Node
    , Closure (BBNode a b s)
    , Closure (BAndBFunction a b s)
    , Closure (ToCFns a b s))
    -> Thunk (Par (Closure ()))
branchAndBoundChild (spawnDepth, n, fs', toC) =
  Thunk $ do
    let fs = unClosure fs'
    bnd <- io $ readFromRegistry boundKey

    sp <- unClosure (pruningPredicate fs) $ (unClosure n) (unClosure c) bnd
    case sp of
      NoPrune -> branchAndBoundExpand spawnDepth n fs' toC >> return toClosureUnit
      _       -> return toClosureUnit

branchAndBoundExpand ::
       Int
    -> Node
    -> Closure n
    -> Closure (BAndBFunction a b s)
    -> Closure (ToCFns a b s)
    -> Par ()
branchAndBoundExpand depth n fs toC
  | depth == 0 = let fsl  = extractBandBFunctions fs
                     toCl = extractToCFunctions toC
                 in expandSequential parent n fsl toCl
  | otherwise  = do
        -- Duplication from the main search function, extract
        let fs' = unClosure fs
        ns <- (unClosure $ orderedGenerator fs') (unClosure n)

        let tasks = map (createChildren (depth - 1) parent) ns

        mapM (spawn one) tasks >>= mapM_ get children
        mapM_ get children

  where
      createChildren d m n =
          let n' = toClosure n
          in $(mkClosure [| branchAndBoundChild (d, m, n', fs, toC) |])

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'branchAndBoundChild)
  , declare $(static 'initRegistryBound)
  , Common.declareStatic
  , Types.declareStatic
  ]
