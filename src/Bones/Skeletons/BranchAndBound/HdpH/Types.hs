{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.DeepSeq       (NFData)
import           Control.Parallel.HdpH (Closure, Par, ToClosure, StaticToClosure,
                                        StaticDecl, here, locToClosure,
                                        declare, staticToClosure)

import           Data.Serialize        (Serialize)

import           GHC.Generics          (Generic)

-- Functions required to specify a B&B computation
data BAndBFunctions a b c s =
  BAndBFunctions
    { generateChoices :: Closure (Closure a -> Closure s -> Par [Closure c])
    , shouldPrune     :: Closure (Closure c -> Closure b -> Closure a -> Closure s -> Par PruneType)
    , updateBound     :: Closure (Closure b -> Closure b -> Bool)
    , step            :: Closure (Closure c -> Closure a -> Closure s
                          -> Par (Closure a, Closure b, Closure s))
    , removeChoice    :: Closure (Closure c -> Closure s-> Par (Closure s))
    } deriving (Generic)

instance NFData (BAndBFunctions a b c s)
instance Serialize (BAndBFunctions a b c s)

data PruneType = NoPrune | Prune | PruneLevel

instance ToClosure () where
  locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = declare (staticToClosure :: StaticToClosure ())
