{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.DeepSeq       (NFData)

import           Control.Parallel.HdpH (Closure, Par, unClosure, StaticDecl,
                                        declare, mkClosure, static)

import           Data.Serialize        (Serialize)

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
    { orderedGenerator :: Closure (BBNode a b s -> Par [Par (BBNode a b s)])
    , pruningHeuristic :: Closure (BBNode a b s -> b -> Par PruneType)
    , compareB         :: Closure (b -> b -> Ordering)
    } deriving (Generic)

data ToCFns a b s =
  ToCFns
    { toCa :: Closure (a -> Closure a)
    , toCb :: Closure (b -> Closure b)
    , toCs :: Closure (s -> Closure s)
    , toCnode :: Closure ((a, b, s) -> Closure (a, b, s))
    } deriving (Generic)

data BAndBFunctionsL a b s =
  BAndBFunctionsL
    { orderedGeneratorL :: BBNode a b s -> Par [Par (BBNode a b s)]
    , pruningHeuristicL :: BBNode a b s -> b -> Par PruneType
    , compareBL         :: b -> b -> Ordering
    } deriving (Generic)

data ToCFnsL a b s =
  ToCFnsL
    { toCaL :: a -> Closure a
    , toCbL :: b -> Closure b
    , toCsL :: s -> Closure s
    , toCnodeL :: (a, b, s) -> Closure (a, b, s)
    } deriving (Generic)

instance NFData (BAndBFunctions a b s)
instance Serialize (BAndBFunctions a b s)

instance NFData (ToCFns a b s)
instance Serialize (ToCFns a b s)

data PruneType = NoPrune | Prune | PruneLevel

toClosureUnit :: Closure ()
toClosureUnit = $(mkClosure [| unit |])

unit :: ()
unit = ()

--------------------------------------------------------------------------------
-- Type Utility Functions
--------------------------------------------------------------------------------
extractBandBFunctions :: Closure (BAndBFunctions a b s) -> BAndBFunctionsL a b s
extractBandBFunctions fns =
  let BAndBFunctions !a !b !c = unClosure fns
  in  BAndBFunctionsL (unClosure a) (unClosure b) (unClosure c)

extractToCFunctions :: Closure (ToCFns a b s) -> ToCFnsL a b s
extractToCFunctions fns =
  let ToCFns !a !b !c !d = unClosure fns
  in  ToCFnsL (unClosure a) (unClosure b) (unClosure c) (unClosure d)

-- Static Information
$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'unit)
  ]
