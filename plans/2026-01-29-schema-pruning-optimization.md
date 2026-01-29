# Schema Pruning Optimization for `--resolve-schema`

**Status:** Completed

## Overview

Optimize `--resolve-schema` graph build performance by pruning the schema dict to only tables referenced in each query before passing it to `sqlglot.lineage()`. Also moved schema dumping (`--dump-schema`) to occur before graph building (between Pass 1 and Pass 2).

## Problem

`sqlglot.lineage()` performance degrades dramatically with large schema dicts. Benchmarks showed:

| Schema Size | Time (6 columns) |
|---|---|
| No schema | 2.6ms |
| 4 tables | 8.3ms |
| 204 tables | **1,041ms** |

The full accumulated schema from all files was passed to every `lineage()` call, even though each query only references a handful of tables. For projects with hundreds of files/tables, this made `--resolve-schema` unusable.

## Design Decisions

- **Prune in the analyzer, not the builder** — keeps the optimization localized and benefits all callers of `LineageAnalyzer`, not just graph builds
- **Prune once per query, not per column** — `_get_query_tables()` is called once before the column loop, and the pruned schema is reused for all columns in that query
- **Case-insensitive matching** — table names are lowered for comparison to handle mixed-case schemas
- **Moved schema dump before graph build** — `_resolved_schema` is fully populated after Pass 1, so dumping between passes is safe and gives users earlier feedback. Required exposing `extract_schemas()` as a public method on `GraphBuilder`

## Implementation

- [x] Add schema pruning in `_analyze_column_lineage_internal()` using existing `_get_query_tables()` method
- [x] Expose `extract_schemas()` as public method on `GraphBuilder`
- [x] Skip Pass 1 in `add_files()`/`add_manifest()` if `_resolved_schema` is already populated
- [x] Restructure CLI `graph_build` to call `extract_schemas()` then dump schema before `add_files()`/`add_manifest()`
- [x] Tests for `extract_schemas()` method and schema pruning correctness

## Files Modified

- `src/sqlglider/lineage/analyzer.py` — schema pruning before `lineage()` calls
- `src/sqlglider/graph/builder.py` — public `extract_schemas()`, skip Pass 1 when already resolved
- `src/sqlglider/cli.py` — restructured `graph_build` to dump schema before graph building
- `tests/sqlglider/lineage/test_analyzer.py` — `TestSchemaPruning` (2 tests)
- `tests/sqlglider/graph/test_builder.py` — `TestExtractSchemas` (3 tests)

## Benchmark Results (After)

| Schema Size | Time (6 columns) |
|---|---|
| No schema | 2.6ms |
| 4 tables | 8.3ms |
| 204 tables | **8.3ms** |

Full `analyze_queries` benchmark on complex fixture (analytics_pipeline.sql):

| Scenario | Before | After |
|---|---|---|
| No schema | ~392ms | ~392ms |
| Small schema (4 tables) | ~373ms | ~373ms |
| Big schema (204 tables) | ~1,400ms+ | **~387ms** |

## Lessons Learned

- The initial assumption was that double-parsing (Pass 1 + Pass 2 both calling `sqlglot.parse()`) was the bottleneck. Benchmarking showed `parse()` costs ~10ms, while `lineage()` with a 200-table schema costs ~1,000ms. Profiling before optimizing avoided wasted effort on AST caching.
- `sqlglot.lineage()` appears to have O(n) or worse scaling with schema size, even for tables not referenced in the query. Pruning is essential for multi-file workloads.
