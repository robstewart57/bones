-- | Sequential Max Clique solver

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}

module Solvers.SequentialSolverBBMC
  (
  sequentialMaxCliqueBBMC
  ) where

{-
  This is a Haskell port of the BBMC max clique algorithm:
  http://www.dcs.gla.ac.uk/~pat/maxClique/distribution/java-20120710/BBMC.java

  See Section 2.3 "Bitset Encodings", of:"Multi-Threaded Maximum Clique".
  Ciaran McCreesh, University of Glasgow. March 2013.
  http://www.dcs.gla.ac.uk/~pat/4yProjects/HallOfFame/reports2013/McCreeshC.pdf

  The algorithm is "Algorithm 3: bmcsa, a bitset encoding of mcsa".
  The heavy lifting of this implementation corresponds to the `colourise`
  function as it is written in the algorithm. The inner `while` loop from
  line 29 correspondds to bbColour.goInner in this Haskell module. It is
  here that the runtime spends most of its time.
-}

import Prelude hiding (filter,length)
import Clique (Clique)
import Data.Int (Int64)
import qualified Data.List as List
import Data.Array.Unboxed (UArray,array)
import qualified Data.Array.Unboxed as Arr
import qualified Data.Vector.Unboxed as U
import Data.Vector hiding (foldr,(++))
import Data.Bits
import Data.Maybe
import Data.Vector.Unboxed.Mutable (unsafeWrite)
import GraphBitSet (GraphBitSet)
import qualified Data.Array.BitArray as BA
import Debug.Trace
import Control.Parallel

-- type BitSet = U.Vector Bool
type BitSet = BA.BitArray Int

sequentialMaxCliqueBBMC :: Int               -- ^ number of nodes
                        -> [(Int,Int)]       -- ^ list of edges from 'parseDIMACS2'
                        -> (Clique, Int64)   -- ^ result
sequentialMaxCliqueBBMC !n !edges = (clique,0)
    where
      arrZeros  = array ((0,0),(n-1,n-1)) [((x,y),False) | x <- [0..n-1], y <- [0..n-1]]
      adjMatrixI :: GraphBitSet
      adjMatrixI = (Arr.//) arrZeros  [((x-1,y-1), True) | (x,y) <- edges]
      adjMatrix = (Arr.//) adjMatrixI [((y-1,x-1), True) | (x,y) <- edges]
      degree = U.fromList (Prelude.map (\i -> List.length (List.filter (==True) (List.map (\j -> (Arr.!) adjMatrix (i,j)) [0..n-1]))) [0..n-1])
      (_,_,_,bestSolution,maxSize) = searchMaxClique n adjMatrix degree
      clique = (generateSolution bestSolution,maxSize)

generateSolution :: BitSet -> [Int]
generateSolution bs = List.concat $ List.zipWith (\b i -> if b then [i] else []) (BA.elems bs) [0..]

instance Show (BA.BitArray Int) where
    show a = show (BA.elems a)

-- | initialises the Max Clique search
searchMaxClique
    :: Int
    -> GraphBitSet
    -> U.Vector Int
    -> (BitSet, BitSet, Int,BitSet,Int)
searchMaxClique !n bigAdjMatrix !degree =
    let nodes = 0
        maxSize = 0
        solution = BA.false (0,n-1) :: BitSet
        bigC = BA.false (0,n-1)
        bigP = BA.true (0,n-1)
        bigV :: Vector Vertex
        bigV  = fromList (Prelude.map (\i -> newVertex i ((U.!) degree i)) [0..n-1])
        (bigV',bigN',invN') = orderVertices bigV n bigAdjMatrix degree
    in bbMaxClique n bigP bigC bigV' bigN' invN' nodes maxSize solution

bbMaxClique :: Int
            -> BitSet
            -> BitSet
            -> Vector Vertex
            -> Vector (BitSet)
            -> Vector (BitSet)
            -> Int
            -> Int
            -> BitSet
            -> (BitSet, BitSet, Int,BitSet,Int)
bbMaxClique !n bigPT bigCT bigVT bigNT invNT !nodes !maxSize curSolutionT =
    go i bigPT bigCT maxSize nodes' curSolutionT
  where nodes' = nodes + 1
        !m = cardinality bigPT
        bigUGen = U.generate m (const 0)
        colourGen = U.generate m (const 0)
        (colour',bigU') = bbColour bigPT bigUGen invNT colourGen
        i = m - 1

        go :: Int -> BitSet -> BitSet -> Int -> Int -> BitSet -> (BitSet, BitSet, Int, BitSet,Int)
        go i bigP bigC maxSize nodes' bestSolution
           | i < 0 = (bigP,bigC,nodes',bestSolution,maxSize)
           | (U.!) colour' i + cardinality bigC <= maxSize = (bigP,bigC,nodes',bestSolution,maxSize)
           | otherwise =
               let v = (U.!) bigU' i
                   !bigC' = modVecElem bigC v True
                   !newP = bitsetAnd bigP ((!) bigNT v)
               in
                 let (maxSize',bestSolution') =
                                    if isEmpty newP && (cardinality bigC' > maxSize)
                                    then let
                                             newSolution :: BitSet
                                             newSolution =
                                                 foldr (\i solutionSt ->
                                                            if (BA.!!!) bigC' i
                                                            then modVecElem solutionSt (idxV ((!) bigVT i)) True
                                                            else solutionSt) (BA.false (0,n-1)) [0..(let (_,n) = BA.bounds bigC'
                                                                                                   in n - 1)]
                                             newSize = cardinality bigC'
                                         in (newSize,newSolution)
                                    else (maxSize,bestSolution)
                     (_,bigC'',nodes'',bestSolution'',maxSize'') =
                                               if not (isEmpty newP)
                                               then bbMaxClique n newP bigC' bigVT bigNT invNT nodes' maxSize' bestSolution'
                                               else (bigP,bigC',nodes',bestSolution',maxSize')
                 in go (i-1) (modVecElem bigP v False) (modVecElem bigC'' v False) maxSize'' nodes'' bestSolution''


bitsetAnd :: BitSet -> BitSet -> BitSet
bitsetAnd !bs1 !bs2 = BA.zipWith (.&.) bs1 bs2
{-# INLINE bitsetAnd #-}

isEmpty :: BitSet -> Bool
isEmpty = not . BA.or
{-# INLINE isEmpty #-}

cardinality :: BitSet -> Int
cardinality !bs = BA.popCount bs
{-# INLINE cardinality #-}

-- | this function takes the vast majority of runtime
bbColour :: BitSet
         -> U.Vector Int
         -> Vector (BitSet)
         -> U.Vector Int
         -> (U.Vector Int,U.Vector Int)
bbColour bigPT bigUT invNT colourT = goOuter bigPT colourT colourClassT 0 bigUT
   where
     colourClassT = 0 :: Int

     goOuter bigP' colour colourClass i bigU =
         if isEmpty bigP'
         then (colour,bigU)
         else
             let (bigP'',colour',i',bigU') =
                     let colourClass' = colourClass + 1 :: Int
                         bigQ = bigP'
                     in goInner bigP' bigQ bigU colourClass' colour i
             in goOuter bigP'' colour' (colourClass + 1) i' bigU'

     goInner :: BitSet -> BitSet -> U.Vector Int -> Int -> U.Vector Int -> Int -> (BitSet, U.Vector Int, Int, U.Vector Int)
     goInner !bigP !bigQ !bigU !colourClass !colour !iT = goInnerDo bigP bigQ bigU colour iT
         where
           goInnerDo p q u !color !i =
               if isEmpty q
               then (p,color,i,u)
               else
                    let !v = firstSetBit q
                        !p'  = (BA.//) p [(v,False)]
                        !q'  = (BA.//) q [(v,False)]
                        !q'' = bitsetAnd q' ((!) invNT v)
                        !u'  = U.modify (\mv -> unsafeWrite mv i v) u
                        color' = modIntVecElem color i colourClass
                        !i'      = i + 1 :: Int
                    in goInnerDo p' q'' u' color' i'

firstSetBit :: BitSet -> Int
firstSetBit = fromJust . BA.elemIndex True
{-# INLINE firstSetBit #-}

modVecElem :: BitSet -> Int -> Bool -> BitSet
modVecElem !vec !pos !v = (BA.//) vec [(pos,v)]
{-# INLINE modVecElem #-}

modIntVecElem :: U.Vector Int -> Int -> Int -> U.Vector Int
modIntVecElem !vec !pos !v = U.modify (\mv -> unsafeWrite mv pos v) vec
{-# INLINE modIntVecElem #-}

data Vertex = Vertex
            { idxV    :: !Int
            , degreeV :: !Int
            , nebDegV :: !Int
            } deriving Show

instance Eq Vertex where
    (==) vertex1 vertex2 = idxV vertex1 == idxV vertex2

instance Ord Vertex where
  vertex1 `compare` vertex2 =
   if degreeV vertex1 <  degreeV vertex2
      || degreeV vertex1 == degreeV vertex2
      && idxV vertex1 > idxV vertex2
   then GT
   else LT

orderVertices ::Vector Vertex
              -> Int -> GraphBitSet
              -> U.Vector Int
              -> (Vector Vertex,Vector (BitSet),Vector (BitSet))
orderVertices bigV {- bigN invN -} n bigAdjMatrix degree =

    let nebDegs = Prelude.map (\i ->
                         let js = Prelude.map (\j ->
                                       if (Arr.!) bigAdjMatrix (i,j)
                                       then (U.!) degree j
                                       else 0) [0..n-1]
                         in Prelude.sum js) [0..n-1]

        bigV' = imap (\i elem -> elem { nebDegV = nebDegs !! i }) bigV
        bigVSorted = fromList $ List.sort (toList bigV')

        (bigN',invN') =
            let xs :: [(Int,[(Int,Bool)])]
                xs = Prelude.map (\i ->
                          (i,Prelude.map (\j ->
                                       let u = idxV ((!) bigVSorted i)
                                           v = idxV ((!) bigVSorted j)
                                           newN    = (j,(Arr.!) bigAdjMatrix (u,v))
                                       in newN) [0..n-1])) [0..n-1]
--                newBigN = fromList $ Prelude.map (\(_,zs) -> U.fromList (Prelude.map snd zs)) xs
                newBigN = fromList $ Prelude.map (\(_,zs) -> BA.array (0,n-1) zs) xs

                ys :: [(Int,[(Int,Bool)])]
                ys = Prelude.map (\i ->
                          (i,Prelude.map (\j ->
                                       let u = idxV ((!) bigVSorted i)
                                           v = idxV ((!) bigVSorted j)
                                           newInvN    = (j,not ((Arr.!) bigAdjMatrix (u,v)))
                                       in newInvN) [0..n-1])) [0..n-1]
                -- newInvN = fromList $ Prelude.map (\(_,zs) -> U.fromList (Prelude.map snd zs)) ys
                newInvN = fromList $ Prelude.map (\(_,zs) -> BA.array (0,n-1) zs) ys
            in (newBigN,newInvN)
    in (bigVSorted,bigN',invN')

newVertex :: Int -> Int -> Vertex
newVertex !idx !deg = Vertex idx deg 0
{-# INLINE newVertex #-}
