{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Graph where

import           Control.Category   (Category, (<<<))
import qualified Control.Category   as Cat (id, (.))
import           Control.DeepSeq    (NFData, rnf)
import           Data.Array         (Array, array, bounds, listArray, (!))
import qualified Data.IntMap.Strict as StrictMap (findWithDefault, fromAscList)
import           Data.IntSet        (IntSet)
import qualified Data.IntSet        as VertexSet (delete, difference,
                                                  fromAscList, intersection,
                                                  member, minView, null, size)
import           Data.List          (delete, group, groupBy, sort, sortBy,
                                     stripPrefix)
import           Data.Maybe         (fromJust)
import           Data.Ord           (Down (Down), comparing)
import           Data.Serialize     (Serialize)

---------------------------------------------------------------------------
-- permutations

-- A bijection from 'a' to 'b' is a pair of functions 'eff :: a -> b' and
-- 'gee :: b -> a' that are inverse to each other, i.e. 'gee . eff = id' and
-- 'eff . gee = id'.
data Bijection a b = Bijection { eff :: a -> b, gee :: b -> a }

-- Bijections form a category.
instance Category Bijection where
  id = Bijection { eff = id
                 , gee = id}
  beta . alpha = Bijection { eff = eff beta . eff alpha
                           , gee = gee alpha . gee beta }

-- Bijections can be inverted.
inv :: Bijection a b -> Bijection b a
inv alpha = Bijection { eff = gee alpha, gee = eff alpha }

-- Bijections can be applied.
app :: Bijection a b -> a -> b
app alpha x = eff alpha x

-- Permutations are endo-bijections.
type Perm a = Bijection a a

---------------------------------------------------------------------------
-- vertices

-- Vertices are non-negative machine integers.
type Vertex = Int

-- Hilbert's Hotel permutation on vertices: 'eff' increments, 'gee' decrements.
permHH :: Perm Vertex
permHH = Bijection { eff = \ v -> v + 1,
                     gee = \ v -> if v > 0 then v - 1 else error "permHH.gee" }


---------------------------------------------------------------------------
-- undirected graphs

-- An undirected graph is represented as a list of non-empty lists of vertices
-- such that each vertex occurs exactly once as the head of a list, and no list
-- contains duplicates. The list of heads is the list of vertices of the
-- undirected graph, and the tails are the respective vertices' adjacency lists.
-- The list of vertices and all adjacency lists are sorted in ascending order.
-- Additionally, an undirected graph is symmetric, ie. u is in the adjecency
-- list of v iff v is in the adjacency list of u.
type UGraph = [[Vertex]]

-- Convert a list of edges over 'n' vertices (numbered '1' to 'n') into an
-- undirected graph.
mkUG :: Int -> [(Vertex,Vertex)] -> UGraph
mkUG n edges =
  if n == length uG then uG else error "mkUG: vertices out of bounds"
    where
      sortUniq = map head . group . sort
      refl_edges = [(v,v) | v <- [1 .. n]]
      symm_edges = concat [[(u,v), (v,u)] | (u,v) <- edges]
      all_edges = sortUniq (refl_edges ++ symm_edges)
      grouped_edges = groupBy (\ (u,_) (v,_) -> u == v) all_edges
      uG = [u : [v | (_,v) <- grp, v /= u] | grp@((u,_):_) <- grouped_edges]

-- Apply a vertex permutation to an undirected graph.
appUG :: Perm Vertex -> UGraph -> UGraph
appUG alpha =
  sortBy (comparing head) .          -- sort list of vertices
  map ((\(u:vs) -> (u : sort vs)) . -- sort all adjacency lists
  (map $ app alpha))                 -- apply alpha to all vertices

verticesUG :: UGraph -> [Vertex]
verticesUG = map head

degreesUG :: UGraph -> [(Vertex,Int)]
degreesUG = map (\ (u:vs) -> (u, length vs))

-- Check degrees are anti-monotonic wrt. vertex order, ie. whether
-- 'map snd . degreesUG' yields a non-increasing list.
isDegreesAntiMonotonicUG :: UGraph -> Bool
isDegreesAntiMonotonicUG = isAntiMonotonic . map snd . degreesUG
  where
    isAntiMonotonic (x:y:zs) = x >= y && isAntiMonotonic (y:zs)
    isAntiMonotonic [_]      = True
    isAntiMonotonic []       = True

-- Compute a permutation that transforms the given undirected graph into
-- an isomorphic one where the degrees are anti-monotonic wrt. vertex order.
-- This resulting vertex order is also known as /non-increasing degree order/.
antiMonotonizeDegreesPermUG :: UGraph -> Perm Vertex
antiMonotonizeDegreesPermUG uG =
  Bijection { eff = f, gee = g }
    where
      cmp = comparing $ \ (v, d) -> (Down d, v)
      g_assocs = zip (verticesUG uG) (map fst $ sortBy cmp $ degreesUG uG)
      f_assocs = sort [(x,y) | (y,x) <- g_assocs]
      !f_map = StrictMap.fromAscList f_assocs
      !g_map = StrictMap.fromAscList g_assocs
      f x = StrictMap.findWithDefault x x f_map
      g y = StrictMap.findWithDefault y y g_map

printGraphStatistics :: UGraph -> IO ()
printGraphStatistics g = do
  let degrees = map snd $ degreesUG g
      dlength = length degrees
      avg_deg = sum (map toFractional degrees) / toFractional dlength :: Double
  putStrLn $ "Graph Statistics:"
  putStrLn $ "Vertices: "   ++ (show (length degrees))
  putStrLn $ "Max degree: " ++ (show (maximum degrees))
  putStrLn $ "Min degree: " ++ (show (minimum degrees))
  putStrLn $ "Avg degree: " ++ (show avg_deg)
  where
    toFractional :: (Real a, Fractional b) => a -> b
    toFractional = Prelude.fromRational . Prelude.toRational

---------------------------------------------------------------------------
-- vertex sets, represented as sets of Ints

type VertexSet = IntSet
  -- Supports operations fromAscLit, null, minView, delete,
  -- intersection, difference.


---------------------------------------------------------------------------
-- graphs, representation for max clique search

-- An undirected graph is represented as an array of adjacency sets,
-- which may be viewed as an adjacency matrix (of Booleans). This matrix
-- is assumed to be symmetric and irreflexive. Vertices are numbered from 0.
newtype Graph = G (Array Vertex VertexSet)
              deriving (Eq, Ord, Show, NFData, Serialize)

-- Converts an undirected graph (whose vertices must be numbered from 0)
-- into the array of adjacency sets representation.
mkG :: UGraph -> Graph
mkG [] = error "mkG: empty graph"
mkG uG = if head (head uG) == 0
           then G $ listArray (0, n - 1) [VertexSet.fromAscList vs | _:vs <- uG]
           else error "mkG: vertices not numbered from 0"
             where
               n = length uG

verticesG :: Graph -> [Vertex]
verticesG (G g) = [l .. u] where (l,u) = bounds g

adjacentG :: Graph -> Vertex -> VertexSet
adjacentG (G g) v = g ! v

isAdjacentG :: Graph -> Vertex -> Vertex -> Bool
isAdjacentG bigG u v = VertexSet.member v $ adjacentG bigG u

degreeG :: Graph -> Vertex -> Int
degreeG bigG = VertexSet.size . adjacentG bigG


---------------------------------------------------------------------------
-- greedy colouring

-- Ordered vertex colouring, reprented as a list of vertex-colour pairs
-- (where colours are positive integers).
type ColourOrder = [(Vertex,Int)]

-- Greedy colouring of a set of vertices, as in [1]. Returns the list of
-- vertex-colour pairs in reverse order of colouring, i.e. the head of the
-- list is the vertex coloured last (with the highest colour).
colourOrder :: Graph -> VertexSet -> ColourOrder
colourOrder bigG bigP = colourFrom 1 bigP []
    where
      -- outer while loop; 'coloured' is a partial colouring
      colourFrom colour uncoloured coloured
          | VertexSet.null uncoloured = coloured
          | otherwise =
            let (uncol',col') = greedyWith colour uncoloured coloured uncoloured
            in colourFrom (colour+1) uncol' col'

      -- inner while loop; 'coloured' is a partial colouring
      greedyWith !colour uncoloured coloured colourable
          | VertexSet.null colourable = (uncoloured,coloured)
          | otherwise =
            greedyWith colour uncoloured' coloured' colourable'
              where
                 (!v,colourable_minus_v) = fromJust (VertexSet.minView colourable)
                 coloured'   = (v,colour):coloured
                 uncoloured' = VertexSet.delete v uncoloured
                 colourable' = VertexSet.difference colourable_minus_v adj
                 adj         = adjacentG bigG v
