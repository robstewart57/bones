{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Types where

import           Control.DeepSeq       (NFData)
import           Control.Parallel.HdpH (Closure, Par, mkClosure)

import           Data.Serialize        (Serialize)

import           GHC.Generics          (Generic)

-- Functions required to specify a B&B computation
data BAndBFunctions a b c s =
  BAndBFunctions
    { generateChoices :: Closure (a -> s -> Par [c])
    , shouldPrune     :: Closure (c -> b -> a -> s -> Par PruneType)
    , updateBound     :: Closure (b -> b -> Bool)
    , step            :: Closure (c -> a -> s -> Par (a, b, s))
    , removeChoice    :: Closure (c -> s-> Par s)
    } deriving (Generic)

data ToCFns a b c s =
  ToCFns
    { toCa :: Closure (a -> Closure a)
    , toCb :: Closure (b -> Closure b)
    , toCc :: Closure (c -> Closure c)
    , toCs :: Closure (s -> Closure s)
    } deriving (Generic)

type UpdateBoundFn b = b -> b -> Bool

data BAndBFunctionsL a b c s =
  BAndBFunctionsL
    { generateChoicesL :: a ->  s -> Par [ c]
    , shouldPruneL     :: c ->  b -> a -> s -> Par PruneType
    , updateBoundL     :: b ->  b -> Bool
    , stepL            :: c ->  a -> s -> Par ( a,  b,  s)
    , removeChoiceL    :: c ->  s-> Par s
    } deriving (Generic)

instance NFData (BAndBFunctions a b c s)
instance Serialize (BAndBFunctions a b c s)

instance NFData (ToCFns a b c s)
instance Serialize (ToCFns a b c s)

data PruneType = NoPrune | Prune | PruneLevel

unitClosure :: Closure ()
{-#INLINE unitClosure #-}
unitClosure = $(mkClosure [| () |])
