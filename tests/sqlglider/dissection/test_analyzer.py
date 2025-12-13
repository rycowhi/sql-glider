"""Tests for dissection analyzer."""

import pytest
from sqlglot.errors import ParseError

from sqlglider.dissection.analyzer import DissectionAnalyzer
from sqlglider.dissection.models import ComponentType


class TestDissectionAnalyzerBasic:
    """Basic tests for DissectionAnalyzer."""

    def test_simple_select(self):
        """Test dissecting a simple SELECT."""
        sql = "SELECT id, name FROM users"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        assert len(results) == 1
        result = results[0]
        assert result.metadata.statement_type == "SELECT"
        assert len(result.components) == 1
        assert result.components[0].component_type == ComponentType.MAIN_QUERY

    def test_invalid_sql(self):
        """Test that invalid SQL raises ParseError."""
        with pytest.raises(ParseError):
            DissectionAnalyzer("INVALID SQL SYNTAX HERE")

    def test_empty_sql(self):
        """Test that empty SQL raises ParseError."""
        with pytest.raises(ParseError):
            DissectionAnalyzer("")


class TestCTEExtraction:
    """Tests for CTE extraction."""

    def test_single_cte(self):
        """Test extracting a single CTE."""
        sql = "WITH cte AS (SELECT id FROM users) SELECT * FROM cte"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        assert len(results) == 1
        result = results[0]

        ctes = result.get_components_by_type(ComponentType.CTE)
        assert len(ctes) == 1
        assert ctes[0].name == "cte"
        assert ctes[0].location == "WITH clause"
        assert ctes[0].is_executable is True

    def test_multiple_ctes(self):
        """Test extracting multiple CTEs."""
        sql = """
        WITH
            cte1 AS (SELECT id FROM users),
            cte2 AS (SELECT id FROM orders)
        SELECT * FROM cte1 JOIN cte2
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        ctes = results[0].get_components_by_type(ComponentType.CTE)
        assert len(ctes) == 2
        assert ctes[0].name == "cte1"
        assert ctes[1].name == "cte2"

    def test_cte_dependencies(self):
        """Test CTE dependency tracking."""
        sql = """
        WITH
            cte1 AS (SELECT id FROM users),
            cte2 AS (SELECT id FROM cte1)
        SELECT * FROM cte2
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        ctes = results[0].get_components_by_type(ComponentType.CTE)
        assert ctes[0].dependencies == []  # cte1 has no dependencies
        assert ctes[1].dependencies == ["cte1"]  # cte2 depends on cte1


class TestDMLExtraction:
    """Tests for DML statement extraction."""

    def test_insert_select(self):
        """Test extracting INSERT INTO SELECT."""
        sql = "INSERT INTO target SELECT id FROM source"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        assert result.metadata.statement_type == "INSERT"

        # Should have TARGET_TABLE and SOURCE_QUERY
        target = result.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(target) == 1
        assert target[0].name == "target"
        assert target[0].is_executable is False

        source = result.get_components_by_type(ComponentType.SOURCE_QUERY)
        assert len(source) == 1
        assert "SELECT id FROM source" in source[0].sql

    def test_create_table_as_select(self):
        """Test extracting CREATE TABLE AS SELECT."""
        sql = "CREATE TABLE new_table AS SELECT id FROM source"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        assert "CREATE TABLE" in result.metadata.statement_type

        target = result.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(target) == 1
        assert target[0].name == "new_table"

    def test_create_view_as_select(self):
        """Test extracting CREATE VIEW AS SELECT."""
        sql = "CREATE VIEW my_view AS SELECT id FROM source"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        assert "CREATE VIEW" in result.metadata.statement_type

        target = result.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(target) == 1
        assert target[0].name == "my_view"


class TestUnionExtraction:
    """Tests for UNION branch extraction."""

    def test_union_all(self):
        """Test extracting UNION ALL branches."""
        sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        branches = result.get_components_by_type(ComponentType.UNION_BRANCH)
        assert len(branches) == 2
        assert branches[0].name == "branch_0"
        assert branches[1].name == "branch_1"

    def test_mixed_union(self):
        """Test extracting mixed UNION and UNION ALL."""
        sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2 UNION SELECT c FROM t3"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        branches = result.get_components_by_type(ComponentType.UNION_BRANCH)
        assert len(branches) == 3

        # Check that main query is also present
        main = result.get_components_by_type(ComponentType.MAIN_QUERY)
        assert len(main) == 1

    def test_union_branches_have_parent(self):
        """Test that UNION branches reference parent MAIN_QUERY."""
        sql = "SELECT a FROM t1 UNION SELECT b FROM t2"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        main = result.get_components_by_type(ComponentType.MAIN_QUERY)[0]
        branches = result.get_components_by_type(ComponentType.UNION_BRANCH)

        for branch in branches:
            assert branch.parent_index == main.component_index
            assert branch.depth == 1


class TestScalarSubqueryExtraction:
    """Tests for scalar subquery extraction."""

    def test_scalar_in_select_list(self):
        """Test extracting scalar subquery in SELECT list."""
        sql = """
        SELECT
            id,
            (SELECT name FROM users u WHERE u.id = t.id) AS user_name
        FROM transactions t
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        scalars = result.get_components_by_type(ComponentType.SCALAR_SUBQUERY)
        assert len(scalars) == 1
        assert scalars[0].name == "user_name"
        assert "SELECT list" in scalars[0].location

    def test_scalar_in_where_clause(self):
        """Test extracting scalar subquery in WHERE clause."""
        sql = """
        SELECT * FROM orders
        WHERE amount > (SELECT AVG(amount) FROM orders)
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        scalars = result.get_components_by_type(ComponentType.SCALAR_SUBQUERY)
        assert len(scalars) == 1
        assert "WHERE clause" in scalars[0].location


class TestSubqueryExtraction:
    """Tests for FROM-clause subquery extraction."""

    def test_subquery_in_from(self):
        """Test extracting subquery in FROM clause."""
        sql = """
        SELECT * FROM (SELECT id FROM users) AS subq
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]
        subqueries = result.get_components_by_type(ComponentType.SUBQUERY)
        assert len(subqueries) == 1
        assert subqueries[0].name == "subq"
        assert "FROM clause" in subqueries[0].location


class TestMultiQueryFiles:
    """Tests for multi-query SQL files."""

    def test_multiple_queries(self):
        """Test dissecting multiple queries."""
        sql = """
        SELECT id FROM users;
        INSERT INTO archive SELECT * FROM users WHERE inactive = 1;
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        assert len(results) == 2
        assert results[0].metadata.query_index == 0
        assert results[1].metadata.query_index == 1
        assert results[0].metadata.statement_type == "SELECT"
        assert results[1].metadata.statement_type == "INSERT"


class TestComplexQueries:
    """Tests for complex query patterns."""

    def test_cte_with_insert(self):
        """Test CTE combined with INSERT."""
        sql = """
        WITH order_totals AS (
            SELECT customer_id, SUM(amount) AS total
            FROM orders
            GROUP BY customer_id
        )
        INSERT INTO customer_summary
        SELECT * FROM order_totals WHERE total > 100
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]

        # Should have CTE, TARGET_TABLE, and SOURCE_QUERY
        ctes = result.get_components_by_type(ComponentType.CTE)
        targets = result.get_components_by_type(ComponentType.TARGET_TABLE)
        sources = result.get_components_by_type(ComponentType.SOURCE_QUERY)

        assert len(ctes) == 1
        assert len(targets) == 1
        assert len(sources) == 1

        # Source query should depend on CTE
        assert "order_totals" in sources[0].dependencies

    def test_cte_with_scalar_subquery(self):
        """Test CTE containing a scalar subquery."""
        sql = """
        WITH enriched AS (
            SELECT
                id,
                (SELECT name FROM users u WHERE u.id = o.customer_id) AS customer_name
            FROM orders o
        )
        SELECT * FROM enriched
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        result = results[0]

        # Should have CTE, MAIN_QUERY, and SCALAR_SUBQUERYs
        ctes = result.get_components_by_type(ComponentType.CTE)
        scalars = result.get_components_by_type(ComponentType.SCALAR_SUBQUERY)

        assert len(ctes) == 1
        # Note: scalar subquery appears twice - once in CTE extraction, once in MAIN_QUERY
        # since MAIN_QUERY includes the full WITH clause
        assert len(scalars) >= 1

        # First scalar subquery should have CTE as parent
        cte_scalars = [s for s in scalars if s.parent_index == ctes[0].component_index]
        assert len(cte_scalars) == 1
        assert "SELECT list in CTE" in cte_scalars[0].location

    def test_original_sql_preserved(self):
        """Test that original SQL is preserved in results."""
        sql = "SELECT id FROM users"
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        assert "SELECT" in results[0].original_sql
        assert "users" in results[0].original_sql
