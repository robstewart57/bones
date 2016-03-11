{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Safe
  (
    declareStatic
  , search
  , searchDynamic
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure, IVar, GIVar, allNodes, declare, get, here,
                                        io, locToClosure, mkClosure, myNode, rput,
                                        one, pushTo, spawn, spawnAt, static, spawnWithPrio, sparkWithPrio,
                                        staticToClosure, toClosure, unClosure, probe,
                                        put, new, glob, tryGet, tryRPut, fork)

import           Control.Monad         (forM_, forM, foldM, foldM_, when, replicateM, zipWithM)

import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TChan (TChan, newTChanIO, writeTChan, readTChan)

import           Data.IORef            (IORef, atomicModifyIORef')
import           Data.List             (mapAccumL, sortBy)

import           Data.Maybe            (catMaybes, mapMaybe)
import           Data.Monoid           (mconcat)

import           Data.Ord  (comparing)

import           Bones.Skeletons.BranchAndBound.HdpH.Types
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
--- Data Types
--------------------------------------------------------------------------------

instance ToClosure () where locToClosure = $(here)

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

-- Safe B&B skeleton with a single WQ
search :: Int
       -> Closure a
       -> Closure s
       -> Closure b
       -> Closure (BAndBFunctions a b c s)
       -> Par a
search spawnDepth startingSol space bnd fs = do
  let fs' = unClosure fs

  allNodes >>= initRegistries

  -- Get top level choices
  searchSpace <- io $ readFromRegistry searchSpaceKey
  cs <- (unClosure $ generateChoices fs') searchSpace space

  -- Sequentially execute tasks up to depth d.
  myNode >>= spawnAndExecuteTasks cs fs'

  -- Return the result stored in the global bound as the answer
  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      spawnAndExecuteTasks choices fs' master  = do
        -- TODO: Build task list in parallel

        -- Fold over it with a parSpawn function
        --   Check depth == spawn depth: spawn and return ivar
        --   else build a new list of tasks and fold over it
        -- "Handle" The tasks based on the ivar ordering (which will be maintained)

        let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) space choices
            topLevelChoices = zip3 choices (replicate (length spaces) startingSol) spaces
            ones = 1 : ones
            tlc = zip (0 : ones) topLevelChoices

        tlist  <- spawnAtDepth tlc master spawnDepth spawnDepth fs

        -- TODO: we probably want to sort the output so that we spawn high priority tasks first, this way
        -- the work stealing doesn't steal the low priority tasks before the
        -- high priorities

        tasksWithOrder <- mapM_ spawnTasksWithPrios tlist

        mapM_ (handleTask master) tlist

      handleTask master (_, (taken, resM, resG, _, c, sol, rem)) = do
        wasTaken <- probe taken
        if wasTaken
         then spinGet resM
         else do
          put taken (toClosure ())
          safeBranchAndBoundSkeletonChild (c , master , sol , rem , fs)
          -- Probably need to write to result for GC to take effect?
          rput resG (toClosure ())
          return $ toClosure ()

      spawnTasksWithPrios (p, (taken, resM, resG, task, c, sol, rem)) = sparkWithPrio one p $(mkClosure [| runAndFill (task, resG) |])

      fib n = fibs !! n
      fibs = 0 : 1 : zipWith (+) fibs (tail fibs)

runAndFill :: (Closure (Par (Closure a)), GIVar (Closure a)) -> Thunk (Par ())
runAndFill (clo, gv) = Thunk $ unClosure clo >>= rput gv

-- Probably really want [Par a] not Par [a]. How can I get this?
spawnAtDepth ::
              [(Int, (Closure c, Closure a, Closure s))]
           -> Node
           -> Int
           -> Int
           -> Closure (BAndBFunctions a b c s)
           -> Par [(Int,(IVar (Closure ()), IVar (Closure ()), GIVar (Closure ()), Closure (Par (Closure())), Closure c, Closure a, Closure s))]
spawnAtDepth ts master maxDepth curDepth fs =
  -- This seems to be getting forced early. I want to keep this lazy if possible!
  if curDepth == 0
    then
        forM ts $ \(p, (c,s,r)) -> do
          -- Check pruning here as well? (It gets checked in the task but we may
          -- as well prune earlier if possible - this happens in another thread
          -- so it's not a big overhead?)
          taken <- new
          g <- glob taken

          resMaster <- new
          resG <- glob resMaster 

          let task  = $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g
                                                                         , c
                                                                         , master
                                                                         , s
                                                                         , r
                                                                         , fs
                                                                         ) |])

          return (p, (taken, resMaster, resG, task, c, s, r))
    else do
      -- Apply step function to each task and continue spawning tasks
      -- We are given a "level" [(choice, sol, rem) | (choice', sol', rem')]
      -- We want to recursively get ourselves the next level.
      step     <- catMaybes <$> mapM stepSol ts
      -- Then we generate choices form this step
      newLevel <- mapM constructChoices step

      -- This might be forcing the list. I don't want this - how do I fix it? No idea yet.

      -- and recurse
      concat <$> mapM (\ts' -> spawnAtDepth ts' master maxDepth (curDepth - 1) fs) newLevel

  where
       -- I think I need scoped type variables if I really want to specify this type
       -- stepSol :: (Closure c, Closure a, Closure r) -> Par (Maybe (Closure a, Closure r))
       stepSol (p, (c, sol, remaining)) = do
        let fs' = unClosure fs

        bnd <- io $ readFromRegistry boundKey

        -- Check if we can prune first to avoid any extra work
        if (unClosure $ shouldPrune fs') c sol bnd
          then return Nothing
          else do
            (sol', _, remaining') <- (unClosure $ step fs') c sol remaining
            return $ Just (p, (sol', remaining'))

       constructChoices (p, (sol, remaining)) = do
           let fs' = unClosure fs
           cs <- (unClosure $ generateChoices fs') sol remaining

           let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) remaining cs

           -- Best to update the priorities here
           return $ zipWith (\i c -> (p + i,c)) (0 : inf ((maxDepth + 2) - curDepth)) (zip3 cs (replicate (length spaces) sol) spaces)

       inf x = x : inf x

spinGet :: IVar a -> Par a
spinGet v = do
  res <- tryGet v
  case res of
    Just x -> return x
    -- TODO: Sleep a bit first?
    Nothing -> spinGet v

-- TODO: Perhaps all spawns should happen here, that way we never need to pass
-- spawnDepth to expand and we can remove a pattern match case for performance.
safeBranchAndBoundSkeletonChildTask ::
    ( GIVar (Closure ())
    , Closure c
    , Node
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s))
    -> Thunk (Par (Closure ()))
safeBranchAndBoundSkeletonChildTask (taken, c, n, sol, remaining, fs) =
  Thunk $ do
    -- Notify the parent that we are doing this task.
    -- The parent might start it's task before our signal reaches it
    -- but given enough cores that's not an issue
    suc <- tryRPut taken (toClosure ())
    res <- spinGet suc -- See if we should start or not
    if unClosure res
      then
        safeBranchAndBoundSkeletonChild (c, n, sol, remaining, fs)
      else
        return (toClosure ())

safeBranchAndBoundSkeletonChild ::
    ( Closure c
    , Node
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s))
    -> Par (Closure ())
safeBranchAndBoundSkeletonChild (c, parent, sol, remaining, fs) = do
    bnd <- io $ readFromRegistry boundKey

    -- Check if we can prune first to avoid any extra work
    let fs' = unClosure fs

    if (unClosure $ shouldPrune fs') c sol bnd
      then return $ toClosure ()
      else do
       (startingSol, _, remaining') <- (unClosure $ step fs') c sol remaining
       toClosure <$> safeBranchAndBoundSkeletonExpand parent startingSol remaining' fs

safeBranchAndBoundSkeletonExpand ::
       Node
    -> Closure a
    -> Closure s
    -> Closure (BAndBFunctions a b c s)
    -> Par ()
safeBranchAndBoundSkeletonExpand parent sol remaining fs = do
  let fs' = unClosure fs
  choices <- (unClosure $ generateChoices fs') sol remaining
  go sol remaining choices fs'
    where
      go sol remaining [] fs' = return ()

      go sol remaining (c:cs) fs' = do
        bnd <- io $ readFromRegistry boundKey

        if (unClosure $ shouldPrune fs') c sol bnd
          then
            return ()
          else do
            (newSol, newBnd, remaining') <- (unClosure $ step fs') c sol remaining

            let shouldUpdate = (unClosure $ updateBound fs') newBnd bnd
            when shouldUpdate $ do
                bAndb_parUpdateLocalBounds newBnd fs
                bAndb_notifyParentOfNewBest parent (newSol, newBnd) fs

            safeBranchAndBoundSkeletonExpand parent newSol remaining' fs

            let remaining'' = (unClosure $ removeChoice fs') c remaining
            go sol remaining'' cs fs'

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

-- Skeleton which attempts to keep "activeTasks" tasks available without
-- generating all work ahead of time
searchDynamic :: Int
              -> Int
              -> Closure a
              -> Closure s
              -> Closure b
              -> Closure (BAndBFunctions a b c s)
              -> Par a
searchDynamic activeTasks spawnDepth startingSol space bnd fs = do
  let fs' = unClosure fs

  allNodes >>= initRegistries

  -- Get top level choices
  searchSpace <- io $ readFromRegistry searchSpaceKey
  cs <- (unClosure $ generateChoices fs') searchSpace space

  -- Sequentially execute tasks up to depth d.
  myNode >>= spawnAndExecuteTasks cs fs'

  -- Return the result stored in the global bound as the answer
  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      spawnAndExecuteTasks choices fs' master  = do

        let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) space choices
            topLevelChoices = zip3 choices (replicate (length spaces) startingSol) spaces

        -- Channels may block the scheduler, issue with scheds=1
        taskQueue <- io newTChanIO

        fork $ taskGenerator topLevelChoices master spawnDepth fs taskQueue activeTasks

        handleTasks master taskQueue

      handleTasks m tq = do
        t <- io . atomically $ readTChan tq
        case t of
          Task (taken, res, c, sol, rem) -> do
            wasTaken <- probe taken
            if wasTaken
            then spinGet res >> handleTasks m tq
            else do
              put taken (toClosure ())
              safeBranchAndBoundSkeletonChild (c , m, sol , rem , fs)
              handleTasks m tq
          Done -> return $ toClosure ()

data Task a = Task a | Done deriving (Show)

taskGenerator ::
     [(Closure c, Closure a, Closure s)]
  -> Node
  -> Int
  -> Closure (BAndBFunctions a b c s)
  -> TChan (Task (IVar (Closure ()), IVar (Closure ()), Closure c, Closure a, Closure s))
  -> Int
  -> Par ()
taskGenerator toplvl m depth fs tq n = do
  -- Keep generating tasks such that we keep N tasks active if possible
  -- Idea is to just call spawnAtDepthLazy and let lazyness do it's thing

   -- Take N to start with
  ts <- spawnAtDepthLazy toplvl m depth fs
  let startingTasks = take n ts
      remainingTasks = drop n ts

  st <- catMaybes <$> sequence startingTasks

  (nextP, signals, tasks) <- foldM spawnTasksWithPrios (1, [], []) st

  -- Give the tasks to the master
  mapM_ (io . atomically . writeTChan tq . Task) tasks

  generateNewTasks remainingTasks signals nextP

  where
      spawnTasksWithPrios (p, ts, lst) (taken, task, c, sol, rem) =
        spawnWithPrio one p task >>= \res -> return (p + 1, taken:ts, (taken, res, c, sol, rem):lst)

      spawnTaskWithPrio p (taken, task, c, sol, rem) =
        spawnWithPrio one p task >>= \res -> return (p + 1, taken, (taken, res, c, sol, rem))

      generateNewTasks taskList takenSignals prio = do

        signalList <- getAny takenSignals

        -- SpawnNewTask always gets a task when one is available
        (task, tl) <- spawnNewTask taskList
        case task of
          Just t -> do
            (prio', s, t') <- spawnTaskWithPrio prio t
            io . atomically $ writeTChan tq (Task t')
            generateNewTasks tl (s:signalList) prio'
          Nothing -> io . atomically $ writeTChan tq Done

      spawnNewTask tl =
        case tl of
          (t:ts) -> do
            t' <- t
            case t' of
              Just x   -> return (Just x, ts)
              Nothing  -> spawnNewTask ts
          [] -> return (Nothing, [])

      -- Not sure how efficient this is due to the cyclic appending, it could be a bottleneck?
      -- Zippers might be a better choice here
      getAny [] = return []

      getAny (x:xs)= do
        filled <- probe x
        if filled then return xs else getAny (xs ++ [x])


spawnAtDepthLazy ::
              [(Closure c, Closure a, Closure s)]
           -> Node
           -> Int
           -> Closure (BAndBFunctions a b c s)
           -> Par [Par (Maybe (IVar (Closure ()), Closure (Par (Closure())), Closure c, Closure a, Closure s))]
spawnAtDepthLazy ts master curDepth fs
  | curDepth == 0 = return $ map makeTask ts
  | otherwise = do
      step     <- catMaybes <$> mapM stepSol ts
      newLevel <- mapM constructChoices step
      concat <$> mapM (\ts' -> spawnAtDepthLazy ts' master (curDepth - 1) fs) newLevel
  where
       stepSol (c, sol, remaining) = do
        let fs' = unClosure fs

        (sol', _, remaining') <- (unClosure $ step fs') c sol remaining
        return $ Just (sol', remaining')

       constructChoices (sol, remaining) = do
           let fs' = unClosure fs
           cs <- (unClosure $ generateChoices fs') sol remaining

           let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) remaining cs

           return $ zip3 cs (replicate (length spaces) sol) spaces

       makeTask (c, s, r) = do
        bnd <- io $ readFromRegistry boundKey

        -- Check if we can prune first to avoid extra work
        let fs' = unClosure fs
        if (unClosure $ shouldPrune fs') c s bnd
          then return Nothing
          else do
          l <- new
          g <- glob l

          let task  =
                $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g , c , master
                                                                   , s , r , fs) |])

          return $ Just (l, task, c, s, r)

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'bAndb_updateParentBest)
  , declare $(static 'bAndb_updateLocalBounds)
  , declare $(static 'safeBranchAndBoundSkeletonChildTask)
  , declare $(static 'runAndFill)
  , declare (staticToClosure :: StaticToClosure ())
  ]
