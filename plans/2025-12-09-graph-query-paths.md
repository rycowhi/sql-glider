# Plan: Path Tracking and Root/Leaf Detection for Graph Query

**Status:** Completed

## Overview

Add path tracking and root/leaf detection to the `graph query` command. This will show the full path from each dependency to the queried column and indicate whether nodes are roots (no upstream) or leaves (no downstream).

## Requirements Summary

1. **Path tracking**: Show ALL paths from each dependency to the queried column
   - Format: `ColA -> ColB -> ColC` for text, array for JSON/CSV
   - Include the queried column in the path
2. **Root/Leaf detection**: Boolean flags on each result
   - `is_root`: true if column has no upstream dependencies
   - `is_leaf`: true if column has no downstream dependencies
3. **All output formats**: text, JSON, CSV

## Implementation Steps

### Phase 1: Model Changes (`src/sqlglider/graph/models.py`)

- [x] Add `LineagePath` Pydantic model with:
  - `nodes: List[str]` - ordered list of column identifiers
  - `hops` property (computed from `len(nodes) - 1`)
  - `to_arrow_string()` method for display formatting

- [x] Extend `LineageNode` model with:
  - `is_root: bool = False`
  - `is_leaf: bool = False`
  - `paths: List[LineagePath] = []`

- [x] Update `LineageNode.from_graph_node()` factory to accept new optional parameters

### Phase 2: Query Algorithm (`src/sqlglider/graph/query.py`)

- [x] Add helper methods to `GraphQuerier`:
  - `_is_root(node_idx)` - check `in_degree == 0`
  - `_is_leaf(node_idx)` - check `out_degree == 0`
  - `_find_all_paths(from_idx, to_idx, use_reversed)` - wrapper for `rx.all_simple_paths()`
  - `_convert_path_to_identifiers(path, reverse)` - convert node indices to `LineagePath`

- [x] Update `find_upstream()`:
  - Keep existing dijkstra for hop counts
  - Add `all_simple_paths()` call for each reachable node
  - Compute is_root/is_leaf for each node
  - Include paths in LineageNode creation

- [x] Update `find_downstream()`:
  - Same changes as find_upstream but with forward graph direction

### Phase 3: CLI Formatters (`src/sqlglider/cli.py`)

- [x] Update `_format_query_result_text()`:
  - Add "Root" and "Leaf" columns (Y/N)
  - Add "Paths" column with arrow-formatted paths

- [x] Update `_format_query_result_json()`:
  - Include `is_root`, `is_leaf`, `paths` in output
  - Serialize paths as arrays of node identifiers

- [x] Update `_format_query_result_csv()`:
  - Add columns: `is_root`, `is_leaf`, `paths`
  - Format paths as semicolon-separated arrow strings

### Phase 4: Tests (`tests/sqlglider/graph/test_query.py`)

- [x] Add `TestLineagePathModel` class:
  - Test `hops` property calculation
  - Test `to_arrow_string()` formatting
  - Test serialization

- [x] Add `TestPathTracking` class:
  - Single path upstream/downstream
  - Multiple paths (diamond graph)
  - Path order verification
  - Paths include queried column

- [x] Add `TestRootLeafDetection` class:
  - Source columns marked as root
  - Output columns marked as leaf
  - Intermediate nodes neither root nor leaf
  - Diamond graph scenarios

- [x] Update existing tests to include new field assertions

### Phase 5: Documentation

- [x] Update `ARCHITECTURE.md` with new graph query features

## Files to Modify

| File | Changes |
|------|---------|
| `src/sqlglider/graph/models.py` | Add `LineagePath`, extend `LineageNode` |
| `src/sqlglider/graph/query.py` | Add path finding and root/leaf detection |
| `src/sqlglider/cli.py` | Update all three formatters |
| `tests/sqlglider/graph/test_query.py` | Add new test classes |
| `ARCHITECTURE.md` | Document new features |

## Technical Notes

### rustworkx APIs Used

- `rx.all_simple_paths(graph, from_, to)` - returns `list[list[int]]` of all paths
- `rx.PyDiGraph.in_degree(node_idx)` - for root detection
- `rx.PyDiGraph.out_degree(node_idx)` - for leaf detection

### Output Examples

**Text:**
```
Column  | Table  | Hops | Root | Leaf | Paths                    | File
--------|--------|------|------|------|--------------------------|------
amount  | orders |    1 | Y    | N    | orders.amount -> total   | q.sql
```

**JSON:**
```json
{
  "identifier": "orders.amount",
  "is_root": true,
  "is_leaf": false,
  "paths": [["orders.amount", "orders.total"]]
}
```

**CSV:**
```csv
identifier,table,column,hops,output_column,is_root,is_leaf,paths,file_path,query_index
"orders.amount","orders","amount",1,"orders.total",true,false,"orders.amount -> orders.total","/path/q.sql",0
```

## Backward Compatibility

All changes are additive:
- New fields in JSON output
- New columns in CSV output
- New columns in text table
- Optional parameters with defaults in `LineageNode.from_graph_node()`

No breaking changes expected.
