module DIMACParser where

import           Graph (Vertex)

-- Parse DIMACS2 format and return number vertices 'n' and a list of edges;
-- vertices appearing in the list of edges are positive and bounded by 'n'.
parseDIMACS2 :: String -> (Int, [(Vertex,Vertex)])
parseDIMACS2 input =
  case stripDIMACS2Comments (map words $ lines input) of
    []   -> error "parseDIMACS2: no data"
    p:es -> (n, edges)
              where
                n = parseDIMACS2PLine p
                edges = map (parseDIMACS2ELine n) es

stripDIMACS2Comments :: [[String]] -> [[String]]
stripDIMACS2Comments = filter (\s -> not (null s || head (head s) == 'c'))

parseDIMACS2PLine :: [String] -> Int
parseDIMACS2PLine ("p":"edge":s:_) = n
  where
    n = case reads s of
          [(i,"")] -> if i > 0
                        then i
                        else error outOfBoundError
          _        -> error lineReadError
    outOfBoundError = "parseDIMACS2: \"p\" line out of bounds (vertices)"
    lineReadError   = "parseDIMACS2: \"p\" line read error"
parseDIMACS2PLine _ = error "parseDIMACS2: \"p edge\" line missing or wrong"

parseDIMACS2ELine :: Int -> [String] -> (Vertex,Vertex)
parseDIMACS2ELine n  ["e", s1, s2] = (u,v)
  where
    u = case reads s1 of
          [(i,"")] -> if 1 <= i && i <= n
                        then i
                        else error outOfBoundError
          _        -> error lineReadError
    v = case reads s2 of
          [(i,"")] -> if 1 <= i && i <= n
                        then i
                        else error outOfBoundError
          _        -> error lineReadError
    outOfBoundError = "parseDIMACS2: \"p\" line out of bounds (sink)"
    lineReadError   = "parseDIMACS2: \"p\" line read error (sink)"
parseDIMACS2ELine _ _ = error "parseDIMCAS2: \"e\" line wrong"
