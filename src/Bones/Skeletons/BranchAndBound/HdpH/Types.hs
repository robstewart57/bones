{-# LANGUAGE DeriveGeneric #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.DeepSeq       (NFData)
import           Control.Parallel.HdpH (Closure, Par)

import           Data.Serialize        (Serialize)

import           GHC.Generics          (Generic)

-- Functions required to specify a B&B computation
data BAndBFunctions a b c s =
  BAndBFunctions
    { generateChoices :: Closure (Closure a -> Closure s -> Par [Closure c])
    , shouldPrune     :: Closure (Closure c -> Closure b -> Closure a -> Closure s -> Bool)
    , updateBound     :: Closure (Closure b -> Closure b -> Bool)
    , step            :: Closure (Closure c -> Closure a -> Closure s
                          -> Par (Closure a, Closure b, Closure s))
    , removeChoice    :: Closure (Closure c -> Closure s-> Closure s)
    } deriving (Generic)

instance NFData (BAndBFunctions a b c s)
instance Serialize (BAndBFunctions a b c s)
