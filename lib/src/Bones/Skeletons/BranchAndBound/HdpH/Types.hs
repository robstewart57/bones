{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.Parallel.HdpH (Closure, Par, StaticDecl,
                                        declare, mkClosure, static)

import           GHC.Generics          (Generic)

-- Functions required to specify a B&B computation
type BBNode a b s = (a, b, s)

bound :: BBNode a b s -> b
bound (_, b, _) = b

solution :: BBNode a b s -> a
solution (s, _ , _ )= s

candidates :: BBNode a b s -> s
candidates (_, _, s) = s

data BAndBFunctions a b s =
  BAndBFunctions
    { orderedGenerator :: BBNode a b s -> Par [Par (BBNode a b s)]
    , pruningHeuristic :: BBNode a b s -> Par b
    , compareB         :: b -> b -> Ordering
    } deriving (Generic)

data ToCFns a b s =
  ToCFns
    { toCa :: a -> Closure a
    , toCb :: b -> Closure b
    , toCs :: s -> Closure s
    , toCnode :: (a, b, s) -> Closure (a, b, s)
    } deriving (Generic)

data PruneType = NoPrune | Prune | PruneLevel

toClosureUnit :: Closure ()
toClosureUnit = $(mkClosure [| unit |])

unit :: ()
unit = ()

-- Static Information
$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'unit)
  ]
