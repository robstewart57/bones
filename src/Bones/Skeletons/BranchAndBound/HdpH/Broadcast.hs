{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Broadcast
  (
    declareStatic
  , search
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure, allNodes, declare, get, here,
                                        io, locToClosure, mkClosure, myNode,
                                        one, pushTo, spawn, spawnAt, static,
                                        staticToClosure, toClosure, unClosure)

import           Control.Monad         (forM_, when)

import           Data.IORef            (IORef, atomicModifyIORef')

import           Data.Monoid           (mconcat)

import           Bones.Skeletons.BranchAndBound.HdpH.Types
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
--- Data Types
--------------------------------------------------------------------------------

instance ToClosure () where
  locToClosure = $(here)

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

search :: Closure a
       -> Closure s
       -> Closure b
       -> Closure (BAndBFunctions a b c s)
       -> Par a
search startingSol space bnd fs' = do
  master <- myNode
  ns     <- allNodes

  initLocalRegistries ns

  searchSpace <- io $ readFromRegistry searchSpaceKey

  let fs = unClosure fs'

  -- Gen at top level
  ts   <- (unClosure $ generateChoices fs) searchSpace space

  -- Generating the starting tasks remembering to remove choices from their left
  -- from the starting "remaining" set
  let tasks = let sr = tail $ scanl (flip (unClosure $ removeChoice fs)) space ts
              in  zipWith (createChildren master) sr ts

  children <- mapM (spawn one) (reverse tasks)
  mapM_ get children

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initLocalRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      createChildren m rem c =
        $(mkClosure [| branchAndBoundChild (m, c, startingSol, rem, fs') |])

branchAndBoundChild ::
    ( Node
    , Closure c
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s)
    )
    -> Thunk (Par (Closure ()))
branchAndBoundChild (n, c, sol, rem, fs') =
  Thunk $ do
    let fs = unClosure fs'

    bnd <- io $ readFromRegistry boundKey
    if (unClosure $ shouldPrune fs) c sol bnd then
        return $ toClosure ()
    else do
        (startingSol, _, rem') <- (unClosure $ step fs) c sol rem
        toClosure <$> branchAndBoundExpand n startingSol rem' fs'

branchAndBoundExpand ::
       Node
    -> Closure a
    -> Closure s
    -> Closure (BAndBFunctions a b c s)
    -> Par ()
branchAndBoundExpand parent sol rem fs' = do
  let fs  = unClosure fs'

  choices <- (unClosure $ generateChoices fs) sol rem

  go sol rem choices fs
    where go sol remaining [] fs      = return ()
          go sol remaining (c:cs) fs  = do
            bnd <- io $ readFromRegistry boundKey

            if (unClosure $ shouldPrune fs) c sol bnd then
              return ()
            else do
              (newSol, newBnd, remaining') <- (unClosure $ step fs) c sol remaining

              when ((unClosure $ updateBound fs) newBnd bnd) $ do
                 bAndb_parUpdateLocalBounds newBnd fs'
                 bAndb_notifyParentOfNewBest parent (newSol, newBnd) fs'

              branchAndBoundExpand parent newSol remaining' fs'

              let remaining'' = (unClosure $ removeChoice fs) c remaining
              go sol remaining'' cs fs

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

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'branchAndBoundChild)
  , declare $(static 'bAndb_updateParentBest)
  , declare $(static 'bAndb_updateLocalBounds)
  , declare (staticToClosure :: StaticToClosure ())
  ]
