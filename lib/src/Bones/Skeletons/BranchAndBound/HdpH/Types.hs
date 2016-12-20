{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.Parallel.HdpH (Closure, Par, StaticDecl,
                                        declare, mkClosure, static)

import           GHC.Generics          (Generic)

type BBNode g = (PartialSolution g, Bound g, Candidates g)
class Ord (Bound g) => BranchAndBound g where
  data Space g
  data PartialSolution g
  data Candidates g
  data Bound g
  orderedGenerator :: Space g -> BBNode g -> Par [Par (BBNode g)]
  pruningHeuristic :: Space g -> BBNode g -> Par (Bound g)

bound :: BBNode g -> Bound g
bound (_, b, _) = b

solution :: BBNode g -> PartialSolution g
solution (s, _ , _ )= s

candidates :: BBNode g -> Candidates g
candidates (_, _, s) = s

-- data BAndBFunctions g a b s =
--   BAndBFunctions
--     { orderedGenerator :: g -> BBNode a b s -> Par [Par (BBNode a b s)]
--     , pruningHeuristic :: g -> BBNode a b s -> Par b
--     , compareB         :: b -> b -> Ordering
--     } deriving (Generic)

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
