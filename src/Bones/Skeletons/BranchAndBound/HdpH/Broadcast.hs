{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Broadcast
  (
    declareStatic
  , search
  -- , findSolution
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure, allNodes, declare, get, here,
                                        io, locToClosure, mkClosure, myNode, fork,
                                        one, pushTo, spawn, spawnAt, static, put,
                                        new, glob, staticToClosure, unClosure, toClosure)

import           Control.Monad         (forM_, when)

import           Data.IORef            (IORef, atomicModifyIORef')

import           Data.Monoid           (mconcat)

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
       -> a                                 -- ^ Initial solution closure
       -> s                                 -- ^ Initial search-space closure
       -> b                                 -- ^ Initial bounds closure
       -> Closure (BAndBFunctions a b c s)  -- ^ Higher order B&B functions
       -> Closure (ToCFns a b c s )         -- ^ Explicit toClosure instances
       -> Par a                             -- ^ The resulting solution after the search completes
search depth startingSol space bnd fs' toC = do
  master <- myNode
  nodes  <- allNodes

  -- Configuration initial state
  initLocalRegistries nodes bnd toC
  initSolutionOnMaster startingSol bnd toC


  let fs = unClosure fs'

  -- Gen top level tasks
  ts  <- (unClosure $ generateChoices fs) startingSol space
  sr <- scanM (flip (unClosure (removeChoice fs))) space ts

  let tasks = zipWith (createChildren depth master) sr ts
  children <- mapM (spawn one) tasks

  -- Wait until all children have been explored
  mapM_ get children

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      createChildren d m rem c =
          let choice = unClosure (toCc (unClosure toC)) c
              sol    = unClosure (toCa (unClosure toC)) startingSol
              space  = unClosure (toCs (unClosure toC)) rem
          in $(mkClosure [| branchAndBoundChild (d, m, choice, sol, space, fs', toC) |])

branchAndBoundChild ::
    ( Int
    , Node
    , Closure c
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s)
    , Closure (ToCFns a b c s))
    -> Thunk (Par (Closure ()))
branchAndBoundChild (spawnDepth, n, c, sol, rem, fs', toC) =
  Thunk $ do
    let fs = unClosure fs'
    let updateBnd = updateBound $ unClosure fs'

    bnd <- io $ readFromRegistry boundKey
    sp <- unClosure (shouldPrune fs) (unClosure c) bnd (unClosure sol) (unClosure rem)
    case sp of
      NoPrune -> do
        (startingSol, _, rem') <- (unClosure $ step fs) (unClosure c) (unClosure sol) (unClosure rem)
        let sol'   = unClosure (toCa (unClosure toC)) startingSol
            space  = unClosure (toCs (unClosure toC)) rem'
        branchAndBoundExpand spawnDepth n sol' space updateBnd fs' toC
        return $ toClosure ()
      _       -> return $ toClosure ()

branchAndBoundExpand ::
       Int
    -> Node
    -> Closure a
    -> Closure s
    -> Closure (UpdateBoundFn b)
    -> Closure (BAndBFunctions a b c s)
    -> Closure (ToCFns a b c s)
    -> Par ()
branchAndBoundExpand depth parent sol rem updateBnd fs toC
  | depth == 0 = let fsl  = extractBandBFunctions fs
                     toCl = extractToCFunctions toC
                 in expandSequential parent (unClosure sol) (unClosure rem) updateBnd fsl toCl
  | otherwise  = do
        let fs' = unClosure fs
        -- Gen top level tasks
        cs <- (unClosure $ generateChoices fs') (unClosure sol) (unClosure rem)

        sr <- scanM (flip (unClosure (removeChoice fs'))) (unClosure rem) cs
        let tasks = zipWith (createChildren (depth - 1) parent) sr cs

        children <- mapM (spawn one) tasks
        mapM_ get children

  where
      -- TODO: Extract common functionality
      createChildren d m rem c =
          let choice = unClosure (toCc (unClosure toC)) c
              -- sol'   = unClosure (toCa (unClosure toC)) sol
              space  = unClosure (toCs (unClosure toC)) rem
          in $(mkClosure [| branchAndBoundChild (d, m, choice, sol, space, fs, toC) |])

expandSequential ::
       Node
       -- ^ Master node (for transferring new bounds)
    -> a
       -- ^ Current solution
    -> s
       -- ^ Current search-space
    -> Closure (UpdateBoundFn b)
       -- ^ Higher order B&B functions
    -> BAndBFunctionsL a b c s
       -- ^ Pre-unclosured local function variants
    -> ToCFnsL a b c s
       -- ^ Explicit toClosure instances
    -> Par ()
       -- ^ Side-effect only function
expandSequential parent sol remaining updateBnd fsl toCL = expand sol remaining
    where
      expand s r = generateChoicesL fsl s r >>= go s r

      go _ _ [] = return ()

      go sol remaining (c:cs) = do
        bnd <- io $ readFromRegistry boundKey

        sp <- shouldPruneL fsl c bnd sol remaining
        case sp of
          Prune      -> do
            remaining'' <- removeChoiceL fsl c remaining
            go sol remaining'' cs

          PruneLevel -> return ()

          NoPrune    -> do
            (newSol, newBnd, remaining') <- stepL fsl c sol remaining

            when (updateBoundL fsl newBnd bnd) $ do
                let cSol = toCaL toCL newSol
                    cBnd = toCbL toCL newBnd
                updateLocalBounds newBnd (updateBoundL fsl)
                notifyParentOfNewBound parent (cSol, cBnd) updateBnd

            expand newSol remaining'

            remaining'' <- removeChoiceL fsl c remaining
            go sol remaining'' cs

---------------------------------------------------------------------------------
-- Skeleton with early exit semantics. Stop when a solution is found (if one exists)
---------------------------------------------------------------------------------

-- findSolution :: Int
--              -> Closure a
--              -> Closure s
--              -> Closure b
--              -> Closure (BAndBFunctions a b c s)
--              -> Par a
-- findSolution depth startingSol space bnd fs' = do
--   master <- myNode
--   ns     <- allNodes

--   found  <- new

--   initLocalRegistries ns found

--   let fs = unClosure fs'

--   -- Gen at top level
--   ts   <- (unClosure $ generateChoices fs) startingSol space

--   -- Generating the starting tasks remembering to remove choices from their left
--   -- from the starting "remaining" set

--   sr <- scanM (flip (unClosure (removeChoice fs))) space ts
--   let tasks = zipWith (createChildren depth master) sr ts

--   children <- mapM (spawn one) tasks

--   -- Thread to check if we terminate without finding a solution
--   fork $ checkTerm children found

--   -- Block until either we find a solution or all tasks have terminated
--   _ <- get found

--   io $ unClosure . fst <$> readFromRegistry solutionKey
--     where
--       initLocalRegistries nodes fnd = do
--         io $ addToRegistry solutionKey (startingSol, bnd)
--         io $ addToRegistry solutionSignalKey fnd
--         forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

--       createChildren d m rem c =
--           $(mkClosure [| branchAndBoundFindChild (d, m, c, startingSol, rem, fs') |])

--       checkTerm ts fnd = mapM_ get ts >> put fnd ()

-- branchAndBoundFindChild ::
--     ( Int
--     , Node
--     , Closure c
--     , Closure a
--     , Closure s
--     , Closure (BAndBFunctions a b c s)
--     )
--     -> Thunk (Par (Closure ()))
-- branchAndBoundFindChild (spawnDepth, n, c, sol, rem, fs') =
--   Thunk $ do
--     let fs = unClosure fs'

--     bnd <- io $ readFromRegistry boundKey
--     sp <- unClosure (shouldPrune fs) c bnd sol rem
--     case sp of
--       NoPrune -> do
--         (startingSol, _, rem') <- (unClosure $ step fs) c sol rem
--         branchAndBoundFindExpand spawnDepth n startingSol rem' fs'
--         return unitClosure
--       _ -> return unitClosure

-- branchAndBoundFindExpand ::
--        Int
--     -> Node
--     -> Closure a
--     -> Closure s
--     -> Closure (BAndBFunctions a b c s)
--     -> Par ()
-- branchAndBoundFindExpand depth parent sol rem fs' = do
--   let fs  = unClosure fs'

--   choices <- (unClosure $ generateChoices fs) sol rem

--   go depth sol rem choices fs
--     where go depth sol remaining [] fs      = return ()

--           go 0 sol remaining (c:cs) fs  = do
--             bnd <- io $ readFromRegistry boundKey

--             sp <- unClosure (shouldPrune fs) c bnd sol rem
--             case sp of
--               NoPrune -> do
--                 (newSol, newBnd, remaining') <- (unClosure $ step fs) c sol remaining

--                 when ((unClosure $ updateBound fs) newBnd bnd) $ do
--                   bAndbFind_notifyParentOfNewBest parent (newSol, newBnd) fs'

--                 branchAndBoundFindExpand depth parent newSol remaining' fs'

--                 remaining'' <- unClosure (removeChoice fs) c remaining
--                 go 0 sol remaining'' cs fs

--               Prune -> do
--                 remaining'' <- unClosure (removeChoice fs) c remaining
--                 go 0 sol remaining'' cs fs

--               PruneLevel -> return ()

--            -- Spawn New Tasks
--           go depth sol remaining cs fs = do
--             sr <- scanM (flip (unClosure (removeChoice fs))) remaining cs
--             let tasks = zipWith (createChildren (depth - 1) parent) sr cs

--             children <- mapM (spawn one) tasks
--             mapM_ get children

--           createChildren sdepth m rem c =
--             $(mkClosure [| branchAndBoundFindChild (sdepth, m, c, sol, rem, fs') |])

-- bAndbFind_notifyParentOfNewBest :: Node
--                             -> (Closure a, Closure b)
--                             -> Closure (BAndBFunctions a b c s)
--                             -> Par ()
-- bAndbFind_notifyParentOfNewBest parent solPlusBnd fs = do
--   -- Wait for an update ack to avoid a race condition in the case when all
--   -- children finish before the final updateBest task has ran on master.
--   spawnAt parent updateFn  >>= get
--   return ()

--   where updateFn = $(mkClosure [| bAndbFind_updateParentBest (solPlusBnd, fs) |])

-- bAndbFind_updateParentBest :: ( (Closure a, Closure b)
--                           , Closure (BAndBFunctions a b c s))
--                        -> Thunk (Par (Closure ()))
-- bAndbFind_updateParentBest ((sol, bnd), fs) = Thunk $ do
--   ref <- io $ getRefFromRegistry solutionKey
--   updated <- io $ atomicModifyIORef' ref $ \prev@(oldSol, b) ->
--                 if (unClosure $ updateBound (unClosure fs)) bnd b
--                     then ((sol, bnd), True)
--                     else (prev      , False)

--   when updated $ do
--     fnd <- io $ readFromRegistry solutionSignalKey
--     put fnd () -- Signal early exit

--   return unitClosure

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'branchAndBoundChild)

  -- Decision Problem Skeleton
  -- , declare $(static 'branchAndBoundFindChild)
  -- , declare $(static 'bAndbFind_updateParentBest)

  , Common.declareStatic
  , Types.declareStatic
  ]
