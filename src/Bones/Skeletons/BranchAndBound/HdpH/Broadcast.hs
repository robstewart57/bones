{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Broadcast
  (
    declareStatic
  , search
  , findSolution
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure, allNodes, declare, get, here,
                                        io, locToClosure, mkClosure, myNode, fork,
                                        one, pushTo, spawn, spawnAt, static, put,
                                        new, glob, staticToClosure, toClosure, unClosure)

import           Control.Monad         (forM_, when)

import           Data.IORef            (IORef, atomicModifyIORef')

import           Data.Monoid           (mconcat)

import           Bones.Skeletons.BranchAndBound.HdpH.Types
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import           Bones.Skeletons.BranchAndBound.HdpH.Util

--------------------------------------------------------------------------------
--- Data Types
--------------------------------------------------------------------------------

instance ToClosure () where
  locToClosure = $(here)

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

search :: Int
       -> Closure a
       -> Closure s
       -> Closure b
       -> Closure (BAndBFunctions a b c s)
       -> Par a
search depth startingSol space bnd fs' = do
  master <- myNode
  ns     <- allNodes

  initLocalRegistries ns

  searchSpace <- io $ readFromRegistry searchSpaceKey

  let fs = unClosure fs'

  -- Gen at top level
  ts   <- (unClosure $ generateChoices fs) searchSpace space

  -- Generating the starting tasks remembering to remove choices from their left
  -- from the starting "remaining" set

  sr <- scanM (flip (unClosure (removeChoice fs))) space ts
  let tasks = zipWith (createChildren depth master) sr ts

  children <- mapM (spawn one) tasks
  mapM_ get children

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initLocalRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      createChildren d m rem c =
          $(mkClosure [| branchAndBoundChild (d, m, c, startingSol, rem, fs') |])

branchAndBoundChild ::
    ( Int
    , Node
    , Closure c
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s)
    )
    -> Thunk (Par (Closure ()))
branchAndBoundChild (spawnDepth, n, c, sol, rem, fs') =
  Thunk $ do
    let fs = unClosure fs'

    bnd <- io $ readFromRegistry boundKey
    sp <- unClosure (shouldPrune fs) c bnd sol rem
    case sp of
      NoPrune -> do
        (startingSol, _, rem') <- (unClosure $ step fs) c sol rem
        toClosure <$> branchAndBoundExpand spawnDepth n startingSol rem' fs'
      _       -> return $ toClosure ()

branchAndBoundExpand ::
       Int
    -> Node
    -> Closure a
    -> Closure s
    -> Closure (BAndBFunctions a b c s)
    -> Par ()
branchAndBoundExpand depth parent sol rem fs' = do
  let fs  = unClosure fs'

  choices <- (unClosure $ generateChoices fs) sol rem

  go depth sol rem choices fs
    where go depth sol remaining [] fs      = return ()

          go 0 sol remaining (c:cs) fs  = do
            bnd <- io $ readFromRegistry boundKey

            sp <- unClosure (shouldPrune fs) c bnd sol rem
            case sp of
              NoPrune -> do
                (newSol, newBnd, remaining') <- (unClosure $ step fs) c sol remaining

                when ((unClosure $ updateBound fs) newBnd bnd) $ do
                  bAndb_parUpdateLocalBounds newBnd fs'
                  bAndb_notifyParentOfNewBest parent (newSol, newBnd) fs'

                branchAndBoundExpand depth parent newSol remaining' fs'

                remaining'' <- unClosure (removeChoice fs) c remaining
                go 0 sol remaining'' cs fs

              Prune -> do
                remaining'' <- unClosure (removeChoice fs) c remaining
                go 0 sol remaining'' cs fs

              PruneLevel -> return ()

           -- Spawn New Tasks
          go depth sol remaining cs fs = do

            sr <- scanM (flip (unClosure (removeChoice fs))) remaining cs
            let tasks = zipWith (createChildren (depth - 1) parent) sr cs

            children <- mapM (spawn one) tasks
            mapM_ get children

          createChildren sdepth m rem c =
            $(mkClosure [| branchAndBoundChild (sdepth, m, c, sol, rem, fs') |])

bAndb_parUpdateLocalBounds :: Closure b
                           -> Closure (BAndBFunctions a b c s)
                           -> Par ()
bAndb_parUpdateLocalBounds bnd fs = do
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if (unClosure $ updateBound (unClosure fs)) bnd b
      then (bnd, ())
      else (b, ())

bAndb_updateLocalBounds :: (Closure b,
                            Closure (BAndBFunctions a b c s))
                        -> Thunk (Par ())
bAndb_updateLocalBounds (bnd, fs) =
  Thunk $ bAndb_parUpdateLocalBounds bnd fs

bAndb_notifyParentOfNewBest :: Node
                            -> (Closure a, Closure b)
                            -> Closure (BAndBFunctions a b c s)
                            -> Par ()
bAndb_notifyParentOfNewBest parent solPlusBnd fs = do
  -- Wait for an update ack to avoid a race condition in the case when all
  -- children finish before the final updateBest task has ran on master.
  spawnAt parent updateFn  >>= get
  return ()

  where updateFn = $(mkClosure [| bAndb_updateParentBest (solPlusBnd, fs) |])

bAndb_updateParentBest :: ( (Closure a, Closure b)
                          , Closure (BAndBFunctions a b c s))
                       -> Thunk (Par (Closure ()))
bAndb_updateParentBest ((sol, bnd), fs) = Thunk $ do
  ref <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(oldSol, b) ->
                if (unClosure $ updateBound (unClosure fs)) bnd b
                    then ((sol, bnd), True)
                    else (prev      , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| bAndb_updateLocalBounds (bnd, fs) |])) ns

  return $ toClosure ()

findSolution :: Int
             -> Closure a
             -> Closure s
             -> Closure b
             -> Closure (BAndBFunctions a b c s)
             -> Par a
findSolution depth startingSol space bnd fs' = do
  master <- myNode
  ns     <- allNodes

  found  <- new

  initLocalRegistries ns found

  searchSpace <- io $ readFromRegistry searchSpaceKey

  let fs = unClosure fs'

  -- Gen at top level
  ts   <- (unClosure $ generateChoices fs) searchSpace space

  -- Generating the starting tasks remembering to remove choices from their left
  -- from the starting "remaining" set


  let tasks = let sr = tail $ scanl (flip (unClosure $ removeChoice fs)) space ts
              in  zipWith (createChildren depth master) sr ts

  children <- mapM (spawn one) tasks

  -- Thread to check if we terminate without finding a solution
  fork $ checkTerm children found

  -- Block until either we find a solution or all tasks have terminated
  _ <- get found

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initLocalRegistries nodes fnd = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        io $ addToRegistry solutionSignalKey fnd
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      createChildren d m rem c =
          $(mkClosure [| branchAndBoundFindChild (d, m, c, startingSol, rem, fs') |])

      checkTerm ts fnd = mapM_ get ts >> put fnd ()

branchAndBoundFindChild ::
    ( Int
    , Node
    , Closure c
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s)
    )
    -> Thunk (Par (Closure ()))
branchAndBoundFindChild (spawnDepth, n, c, sol, rem, fs') =
  Thunk $ do
    let fs = unClosure fs'

    bnd <- io $ readFromRegistry boundKey
    if (unClosure $ shouldPrune fs) c sol bnd then
        return $ toClosure ()
    else do
        (startingSol, _, rem') <- (unClosure $ step fs) c sol rem
        toClosure <$> branchAndBoundFindExpand spawnDepth n startingSol rem' fs'

branchAndBoundFindExpand ::
       Int
    -> Node
    -> Closure a
    -> Closure s
    -> Closure (BAndBFunctions a b c s)
    -> Par ()
branchAndBoundFindExpand depth parent sol rem fs' = do
  let fs  = unClosure fs'

  choices <- (unClosure $ generateChoices fs) sol rem

  go depth sol rem choices fs
    where go depth sol remaining [] fs      = return ()

          go 0 sol remaining (c:cs) fs  = do
            bnd <- io $ readFromRegistry boundKey

            if (unClosure $ shouldPrune fs) c sol bnd then
              return ()
            else do
              (newSol, newBnd, remaining') <- (unClosure $ step fs) c sol remaining

              when ((unClosure $ updateBound fs) newBnd bnd) $ do
                 bAndbFind_notifyParentOfNewBest parent (newSol, newBnd) fs'

              branchAndBoundFindExpand depth parent newSol remaining' fs'

              let remaining'' = (unClosure $ removeChoice fs) c remaining
              go 0 sol remaining'' cs fs

           -- Spawn New Tasks
          go depth sol remaining cs fs = do
            let tasks = let sr = tail $ scanl (flip (unClosure $ removeChoice fs)) remaining cs
                        in  zipWith (createChildren (depth - 1) parent) sr cs

            children <- mapM (spawn one) tasks
            mapM_ get children

          createChildren sdepth m rem c =
            $(mkClosure [| branchAndBoundFindChild (sdepth, m, c, sol, rem, fs') |])

bAndbFind_notifyParentOfNewBest :: Node
                            -> (Closure a, Closure b)
                            -> Closure (BAndBFunctions a b c s)
                            -> Par ()
bAndbFind_notifyParentOfNewBest parent solPlusBnd fs = do
  -- Wait for an update ack to avoid a race condition in the case when all
  -- children finish before the final updateBest task has ran on master.
  spawnAt parent updateFn  >>= get
  return ()

  where updateFn = $(mkClosure [| bAndbFind_updateParentBest (solPlusBnd, fs) |])

bAndbFind_updateParentBest :: ( (Closure a, Closure b)
                          , Closure (BAndBFunctions a b c s))
                       -> Thunk (Par (Closure ()))
bAndbFind_updateParentBest ((sol, bnd), fs) = Thunk $ do
  ref <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(oldSol, b) ->
                if (unClosure $ updateBound (unClosure fs)) bnd b
                    then ((sol, bnd), True)
                    else (prev      , False)

  when updated $ do
    fnd <- io $ readFromRegistry solutionSignalKey
    put fnd () -- Signal early exit

  return $ toClosure ()

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'branchAndBoundChild)
  , declare $(static 'bAndb_updateParentBest)
  , declare $(static 'bAndb_updateLocalBounds)

  -- Decision Problem Skeleton
  , declare $(static 'branchAndBoundFindChild)
  , declare $(static 'bAndbFind_updateParentBest)
  , declare (staticToClosure :: StaticToClosure ())
  ]
