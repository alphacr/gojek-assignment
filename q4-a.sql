WITH C AS (
  SELECT
    node,
    parent,
    node AS Rootnode
  FROM
    playground_ricky.node_parent_table
  WHERE
    node = parent
  UNION ALL
  SELECT
    T.node,
    T.parent,
    C.Rootnode
  FROM
    playground_ricky.node_parent_table AS T
  INNER JOIN
    C
  ON
    T.parent = C.node
  WHERE
    T.node <> T.parent 
)
SELECT
  *
FROM
  C