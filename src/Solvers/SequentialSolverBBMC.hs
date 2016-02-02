-- | Sequential Max Clique solver

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

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

  This module is unfortunately a lot significantly slower than the
  other sequential algorithm in the 'Sequential' module and the
  reference Java code BBMC.java. The bottleneck is bbColour.goInner. This bitset
  version though can be parallelised, see Chapter 3 of the above PDF document.
-}

import Prelude hiding (filter,length)
import Clique (Clique)
import Data.Int (Int64)
import qualified Data.List as List
import Data.Array.Unboxed (UArray,array)
import qualified Data.Array.Unboxed as Arr
import qualified Data.Vector.Unboxed as UVect
import Data.Vector hiding (foldr,(++))
import Data.Bits
import Data.Maybe
import Data.Vector.Unboxed.Mutable (unsafeWrite)
import GraphBitSet (GraphBitSet)

type BitSet = UVect.Vector Bool

sequentialMaxCliqueBBMC :: Int               -- ^ number of nodes
                        -> [(Int,Int)]       -- ^ list of edges from 'parseDIMACS2'
                        -> (Clique, Int64)   -- ^ result
sequentialMaxCliqueBBMC !n !edges = (clique,0)
    where
      arrZeros  = array ((0,0),(n-1,n-1)) [((x,y),False) | x <- [0..n-1], y <- [0..n-1]]
      adjMatrixI :: GraphBitSet
      adjMatrixI = (Arr.//) arrZeros  [((x-1,y-1), True) | (x,y) <- edges]
      adjMatrix = (Arr.//) adjMatrixI [((y-1,x-1), True) | (x,y) <- edges]
      degree = UVect.fromList (Prelude.map (\i -> List.length (List.filter (==True) (List.map (\j -> (Arr.!) adjMatrix (i,j)) [0..n-1]))) [0..n-1])
      (_,_,_,bestSolution,maxSize) = searchMaxClique n adjMatrix degree
      clique = (generateSolution bestSolution,maxSize)

generateSolution :: UVect.Vector Int -> [Int]
generateSolution solution = Prelude.map (+1) indexes
    where indexes = List.filter (\i -> (UVect.!) solution i == 1) [0..UVect.length solution-1]

-- | initialises the Max Clique search
searchMaxClique :: Int -> GraphBitSet
       -> UVect.Vector Int
       -> (BitSet, BitSet, Int,UVect.Vector Int,Int)
searchMaxClique !n bigAdjMatrix !degree =
    let nodes = 0
        maxSize = 0
        solution = UVect.generate n (const 0) :: UVect.Vector Int
        bigC = UVect.generate n (const False)
        bigP = UVect.generate n (const True)
        bigV :: Vector Vertex
        bigV  = fromList (Prelude.map (\i -> newVertex i ((UVect.!) degree i)) [0..n-1])
        (bigV',bigN',invN') = orderVertices bigV n bigAdjMatrix degree
    in bbMaxClique n bigP bigC bigV' bigN' invN' nodes maxSize solution

bbMaxClique :: Int -> BitSet
            -> BitSet
            -> Vector Vertex
            -> Vector (BitSet)
            -> Vector (BitSet)
            -> Int
            -> Int
            -> UVect.Vector Int
            -> (BitSet, BitSet, Int,UVect.Vector Int,Int)
bbMaxClique !n bigPT bigCT bigVT bigNT invNT !nodes !maxSize curSolutionT =
    go i bigPT bigCT maxSize nodes' curSolutionT
  where nodes' = nodes + 1
        m = cardinality bigPT
        bigUGen = UVect.generate m (const 0)
        colourGen = UVect.generate m (const 0)
        (colour',bigU') = bbColour bigPT bigUGen invNT colourGen
        i = m - 1

        go :: Int -> BitSet -> BitSet -> Int -> Int -> UVect.Vector Int -> (BitSet, BitSet, Int,UVect.Vector Int,Int)
        go i bigP bigC maxSize nodes' bestSolution
           | i < 0 = (bigP,bigC,nodes',bestSolution,maxSize)
           | (UVect.!) colour' i + cardinality bigC <= maxSize = (bigP,bigC,nodes',bestSolution,maxSize)
           | otherwise =
               let v = (UVect.!) bigU' i
                   !bigC' = modVecElem bigC v True
                   !newP = bitsetAnd bigP ((!) bigNT v)
               in
                 let (maxSize',bestSolution') =
                                    if isEmpty newP && (cardinality bigC' > maxSize)
                                    then let newSolution :: UVect.Vector Int
                                             newSolution =
                                                 foldr (\i solutionSt ->
                                                            if (UVect.!) bigC' i
                                                            then modVecElem solutionSt (idxV ((!) bigVT i)) 1
                                                            else solutionSt) (UVect.generate n (const 0)) [0..(UVect.length bigC' - 1)]
                                             newSize = cardinality bigC'
                                         in (newSize,newSolution)
                                    else (maxSize,bestSolution)
                     (_,bigC'',nodes'',bestSolution'',maxSize'') =
                                               if not (isEmpty newP)
                                               then bbMaxClique n newP bigC' bigVT bigNT invNT nodes' maxSize' bestSolution'
                                               else (bigP,bigC',nodes',bestSolution',maxSize')
                 in go (i-1) (modVecElem bigP v False) (modVecElem bigC'' v False) maxSize'' nodes'' bestSolution''


bitsetAnd :: BitSet -> BitSet -> BitSet
bitsetAnd = UVect.zipWith (.&.)
{-# INLINE bitsetAnd #-}

isEmpty :: BitSet -> Bool
isEmpty = not . UVect.or
{-# INLINE isEmpty #-}

cardinality :: BitSet -> Int
cardinality = UVect.foldl' (\acc x -> if x then acc+1 else acc) 0

-- | this function takes the vast majority of runtime
bbColour :: BitSet
         -> UVect.Vector Int
         -> Vector (BitSet)
         -> UVect.Vector Int
         -> (UVect.Vector Int,UVect.Vector Int)
bbColour bigPT bigUT invNT colourT = goOuter bigPT colourT colourClassT iT bigUT
   where
     colourClassT = 0
     iT = 0

     goOuter bigP' colour colourClass i bigU =
         if isEmpty bigP'
         then (colour,bigU)
         else
             let (bigP'',colour',i',bigU') =
                     let colourClass' = colourClass + 1
                         bigQ = bigP'
                     in goInner bigP' bigQ bigU colourClass' colour i
             in goOuter bigP'' colour' (colourClass + 1) i' bigU'

     -- the vast majority of runtime is spent here.
     goInner bigP bigQ bigU colourClass colour i =
         if isEmpty bigQ
         then (bigP,colour,i,bigU)
         else
             let v = firstSetBit bigQ
                 !bigP'  = modVecElem bigP v False
                 !bigQ'  = modVecElem bigQ v False
                 !bigQ'' = bitsetAnd bigQ' ((!) invNT v)
                 !bigU'  = modVecElem bigU i v
                 colour' = modVecElem colour i colourClass
                 !i'      = i + 1
             in goInner bigP' bigQ'' bigU' colourClass colour' i'

modVecElem :: UVect.Unbox a => UVect.Vector a -> Int -> a -> UVect.Vector a
modVecElem vec pos v = UVect.modify (\mv -> unsafeWrite mv pos v) vec
{-# INLINE modVecElem #-}

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
              -- -> Vector (BitSet)
              -- -> Vector (BitSet)
              -> Int -> GraphBitSet
              -> UVect.Vector Int
              -> (Vector Vertex,Vector (BitSet),Vector (BitSet))
orderVertices bigV {- bigN invN -} n bigAdjMatrix degree =

    let nebDegs = Prelude.map (\i ->
                         let js = Prelude.map (\j ->
                                       if (Arr.!) bigAdjMatrix (i,j)
                                       then (UVect.!) degree j
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
                newBigN = fromList $ Prelude.map (\(_,zs) -> UVect.fromList (Prelude.map snd zs)) xs

                ys :: [(Int,[(Int,Bool)])]
                ys = Prelude.map (\i ->
                          (i,Prelude.map (\j ->
                                       let u = idxV ((!) bigVSorted i)
                                           v = idxV ((!) bigVSorted j)
                                           newInvN    = (j,not ((Arr.!) bigAdjMatrix (u,v)))
                                       in newInvN) [0..n-1])) [0..n-1]
                newInvN = fromList $ Prelude.map (\(_,zs) -> UVect.fromList (Prelude.map snd zs)) ys
            in (newBigN,newInvN)
    in (bigVSorted,bigN',invN')

newVertex :: Int -> Int -> Vertex
newVertex !idx !deg = Vertex idx deg 0
{-# INLINE newVertex #-}

firstSetBit :: BitSet -> Int
firstSetBit = fromJust . UVect.findIndex id
{-# INLINE firstSetBit #-}
