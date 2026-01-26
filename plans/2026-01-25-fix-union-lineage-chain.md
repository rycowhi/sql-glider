**Status:** Completed

# Fix: Generated Column Lineage Chain Breaking at UNION Queries

## Problem Summary

When building a lineage graph from SQL with INSERT statements containing UNION queries, the chain of lineage is broken. Querying upstream/downstream lineage stops at the computed column because the graph has disconnected nodes.

**Example SQL:**
```sql
INSERT OVERWRITE TABLE db.output_table_1
SELECT ... AS full_address FROM db.input_a
UNION
SELECT ... AS full_address FROM db.input_b;

INSERT OVERWRITE TABLE db.output_table_2
SELECT o.full_address AS address FROM db.output_table_1 AS o;
```

**Expected lineage chain:**
```
db.input_a.address_one -> db.output_table_1.full_address -> db.output_table_2.address
```

**Actual result:** The chain stops at `full_address` (unqualified) because:
- Edge exists: `db.input_a.address_one` -> `full_address` (unqualified node)
- Edge exists: `db.output_table_1.full_address` -> `db.output_table_2.address`
- **Missing edge:** `full_address` -> `db.output_table_1.full_address`

## Root Cause

In [analyzer.py:822-823](src/sqlglider/lineage/analyzer.py#L822-L823), the `_get_target_and_select()` method only handles `exp.Select` nodes:

```python
select_node = self.expr.expression
if isinstance(select_node, exp.Select):
    return (target_name, select_node)
```

For INSERT statements with UNION, `self.expr.expression` is an `exp.Union`, not `exp.Select`. This causes the code to fall through to the pure SELECT case (line 861), which returns `(None, select_nodes[0])` - losing the target table context.

When `target_table` is `None`, `get_output_columns()` doesn't qualify output columns with the target table, resulting in unqualified node identifiers like `full_address` instead of `db.output_table_1.full_address`.

## Implementation Plan

### Step 1: Fix `_get_target_and_select()` to Handle UNION

**File:** [src/sqlglider/lineage/analyzer.py](src/sqlglider/lineage/analyzer.py)

Modify the INSERT handling (lines 821-823) to accept both `exp.Select` and `exp.Union`:

```python
select_node = self.expr.expression
if isinstance(select_node, (exp.Select, exp.Union)):
    return (target_name, select_node)
```

Apply the same fix to CREATE TABLE/VIEW handling (lines 835-837):

```python
select_node = self.expr.expression
if isinstance(select_node, (exp.Select, exp.Union)):
    return (target_name, select_node)
```

### Step 2: Fix `get_output_columns()` to Handle UNION Expressions

**File:** [src/sqlglider/lineage/analyzer.py](src/sqlglider/lineage/analyzer.py)

When `select_node` is a UNION, `select_node.expressions` doesn't return the column projections. We need to extract them from the first SELECT branch.

Add a helper method after `get_output_columns()`:

```python
def _get_select_projections(self, node: exp.Expression) -> List[exp.Expression]:
    """
    Get the SELECT projections from a SELECT or UNION node.

    For UNION queries, returns projections from the first branch since
    all branches must have the same number of columns with compatible types.
    """
    if isinstance(node, exp.Select):
        return list(node.expressions)
    elif isinstance(node, exp.Union):
        # Recursively get from the left branch (could be nested UNION)
        return self._get_select_projections(node.left)
    return []
```

Update `get_output_columns()` to use this helper (line 158):

```python
# Before:
for projection in select_node.expressions:

# After:
projections = self._get_select_projections(select_node)
for projection in projections:
```

Also update the DQL case (line 181):

```python
# Before:
for projection in select_node.expressions:

# After:
projections = self._get_select_projections(select_node)
for projection in projections:
```

### Step 3: Add Unit Tests

**File:** [tests/sqlglider/lineage/test_analyzer.py](tests/sqlglider/lineage/test_analyzer.py)

Add tests for INSERT with UNION:

```python
class TestInsertWithUnion:
    """Tests for INSERT statements containing UNION queries."""

    def test_insert_union_qualifies_output_with_target_table(self):
        """Output columns should be qualified with the INSERT target table."""
        sql = """
        INSERT INTO db.output_table
        SELECT id, name FROM db.table_a
        UNION
        SELECT id, name FROM db.table_b
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.output_table.id" in output_names
        assert "db.output_table.name" in output_names

    def test_insert_union_with_computed_column(self):
        """Computed columns in UNION should be qualified with target table."""
        sql = """
        INSERT INTO db.output
        SELECT CONCAT(a, b) AS combined FROM db.source
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.output.combined" in output_names
```

### Step 4: Add Graph Integration Test

**File:** [tests/sqlglider/graph/test_builder.py](tests/sqlglider/graph/test_builder.py)

Add test using the existing fixture:

```python
def test_cross_query_lineage_with_union(tmp_path):
    """Test that lineage chains work across queries with UNION."""
    sql_file = Path("tests/fixtures/original_queries/test_generated_column_query.sql")

    builder = GraphBuilder(default_dialect="spark")
    builder.add_file(sql_file)

    # Query downstream from input
    querier = GraphQuerier(builder.graph, builder.node_index)
    downstream = querier.query_downstream("db.input_a.address_one")

    # Should include both intermediate and final output
    downstream_ids = {n.identifier for n in downstream}
    assert "db.output_table_1.full_address" in downstream_ids
    assert "db.output_table_2.address" in downstream_ids
```

## Files to Modify

1. [src/sqlglider/lineage/analyzer.py](src/sqlglider/lineage/analyzer.py) - Core fix (2 locations + 1 new helper)
2. [tests/sqlglider/lineage/test_analyzer.py](tests/sqlglider/lineage/test_analyzer.py) - Unit tests
3. [tests/sqlglider/graph/test_builder.py](tests/sqlglider/graph/test_builder.py) - Integration test

## Verification

1. Run the existing test suite: `uv run pytest`
2. Rebuild the graph: `uv run sqlglider graph build .\tests\fixtures\original_queries\test_generated_column_query.sql --dialect spark --output linkgraph.json`
3. Verify downstream query works: `uv run sqlglider graph query .\linkgraph.json --downstream db.input_a.address_one`
   - Should show: `db.output_table_1.full_address`, `db.output_table_2.address`
4. Verify upstream query works: `uv run sqlglider graph query .\linkgraph.json --upstream db.output_table_2.address`
   - Should show: `db.output_table_1.full_address`, `db.input_a.address_one`, `db.input_a.address_two`, etc.

---

## Implementation Notes

### Changes Made

1. **[analyzer.py:822, 837](src/sqlglider/lineage/analyzer.py#L822)** - Extended the type check to accept both `exp.Select` and `exp.Union` for INSERT and CREATE statements.

2. **[analyzer.py:237-269](src/sqlglider/lineage/analyzer.py#L237)** - Added two helper methods:
   - `_get_select_projections()` - Extracts column projections from SELECT or UNION nodes
   - `_get_first_select()` - Gets the first SELECT node for table resolution in DQL path

3. **[analyzer.py:158, 182](src/sqlglider/lineage/analyzer.py#L158)** - Updated `get_output_columns()` to use the new helpers for both DML and DQL paths.

4. **[analyzer.py:4](src/sqlglider/lineage/analyzer.py#L4)** - Added `Union` to typing imports.

5. **[analyzer.py:844](src/sqlglider/lineage/analyzer.py#L844)** - Updated return type annotation to include `exp.Union`.

### Tests Added

- **5 unit tests** in `TestInsertWithUnion` class ([test_analyzer.py](tests/sqlglider/lineage/test_analyzer.py)):
  - `test_insert_union_qualifies_output_with_target_table`
  - `test_insert_union_with_computed_column`
  - `test_insert_union_all_with_aliases`
  - `test_create_table_as_union_qualifies_output`
  - `test_multi_query_insert_union_cross_reference`

- **3 integration tests** in `TestGraphBuilderInsertWithUnion` class ([test_builder.py](tests/sqlglider/graph/test_builder.py)):
  - `test_insert_union_creates_qualified_nodes`
  - `test_cross_query_lineage_with_union`
  - `test_upstream_query_through_union`

### Test Results

- All 545 tests pass (1 skipped - unrelated permission test on Windows)
- Linting: All checks passed
- Type checking: 0 errors

### Verification Results

**Before fix:**
- Graph had disconnected nodes: `full_address` (unqualified) and `db.output_table_1.full_address` (qualified)
- Downstream query stopped at intermediate table

**After fix:**
- All nodes properly qualified with target table
- Downstream query `db.input_a.address_one` shows full chain: `db.output_table_1.full_address` (1 hop) → `db.output_table_2.address` (2 hops)
- Upstream query `db.output_table_2.address` traces back to all 5 source columns from both UNION branches
