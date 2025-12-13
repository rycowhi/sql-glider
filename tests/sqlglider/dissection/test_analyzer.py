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


class TestMultiDMLWithCTEs:
    """Integration tests for multi-query files with DML statements and CTEs."""

    def test_multiple_insert_statements_with_ctes(self):
        """Test multiple INSERT statements, each with their own CTEs."""
        sql = """
        -- First INSERT with 2 CTEs
        WITH
            active_customers AS (
                SELECT customer_id, name
                FROM customers
                WHERE status = 'active'
            ),
            recent_orders AS (
                SELECT customer_id, SUM(amount) AS total
                FROM orders
                WHERE order_date > '2024-01-01'
                GROUP BY customer_id
            )
        INSERT INTO customer_summary
        SELECT
            ac.customer_id,
            ac.name,
            ro.total
        FROM active_customers ac
        JOIN recent_orders ro ON ac.customer_id = ro.customer_id;

        -- Second INSERT with 3 CTEs
        WITH
            product_sales AS (
                SELECT product_id, SUM(quantity) AS sold
                FROM order_items
                GROUP BY product_id
            ),
            product_info AS (
                SELECT product_id, name, category
                FROM products
            ),
            inventory AS (
                SELECT product_id, stock_count
                FROM warehouse
            )
        INSERT INTO product_report
        SELECT
            ps.product_id,
            pi.name,
            pi.category,
            ps.sold,
            inv.stock_count
        FROM product_sales ps
        JOIN product_info pi ON ps.product_id = pi.product_id
        LEFT JOIN inventory inv ON ps.product_id = inv.product_id;

        -- Third INSERT with 1 CTE that depends on another
        WITH
            base_metrics AS (
                SELECT date, region, revenue
                FROM daily_sales
            ),
            aggregated AS (
                SELECT region, SUM(revenue) AS total_revenue
                FROM base_metrics
                GROUP BY region
            )
        INSERT INTO regional_totals
        SELECT * FROM aggregated WHERE total_revenue > 10000;
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        # Should have 3 queries
        assert len(results) == 3

        # First query: 2 CTEs
        q1 = results[0]
        assert q1.metadata.query_index == 0
        assert q1.metadata.statement_type == "INSERT"
        q1_ctes = q1.get_components_by_type(ComponentType.CTE)
        assert len(q1_ctes) == 2
        assert q1_ctes[0].name == "active_customers"
        assert q1_ctes[1].name == "recent_orders"
        # Check target table
        q1_targets = q1.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(q1_targets) == 1
        assert q1_targets[0].name == "customer_summary"
        # Check source query depends on CTEs
        q1_sources = q1.get_components_by_type(ComponentType.SOURCE_QUERY)
        assert len(q1_sources) == 1
        assert "active_customers" in q1_sources[0].dependencies
        assert "recent_orders" in q1_sources[0].dependencies

        # Second query: 3 CTEs
        q2 = results[1]
        assert q2.metadata.query_index == 1
        assert q2.metadata.statement_type == "INSERT"
        q2_ctes = q2.get_components_by_type(ComponentType.CTE)
        assert len(q2_ctes) == 3
        assert q2_ctes[0].name == "product_sales"
        assert q2_ctes[1].name == "product_info"
        assert q2_ctes[2].name == "inventory"
        # Check target table
        q2_targets = q2.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(q2_targets) == 1
        assert q2_targets[0].name == "product_report"

        # Third query: 2 CTEs with dependency
        q3 = results[2]
        assert q3.metadata.query_index == 2
        assert q3.metadata.statement_type == "INSERT"
        q3_ctes = q3.get_components_by_type(ComponentType.CTE)
        assert len(q3_ctes) == 2
        assert q3_ctes[0].name == "base_metrics"
        assert q3_ctes[1].name == "aggregated"
        # Second CTE depends on first
        assert q3_ctes[0].dependencies == []
        assert q3_ctes[1].dependencies == ["base_metrics"]
        # Check target table
        q3_targets = q3.get_components_by_type(ComponentType.TARGET_TABLE)
        assert len(q3_targets) == 1
        assert q3_targets[0].name == "regional_totals"

    def test_mixed_dml_statements_with_ctes(self):
        """Test mix of INSERT, CREATE TABLE AS, and CREATE VIEW with CTEs."""
        sql = """
        -- INSERT with CTE
        WITH monthly_totals AS (
            SELECT month, SUM(sales) AS total
            FROM transactions
            GROUP BY month
        )
        INSERT INTO monthly_summary
        SELECT * FROM monthly_totals;

        -- CREATE TABLE AS with CTEs
        WITH
            top_customers AS (
                SELECT customer_id, COUNT(*) AS order_count
                FROM orders
                GROUP BY customer_id
                HAVING COUNT(*) > 10
            ),
            customer_details AS (
                SELECT customer_id, name, email
                FROM customers
            )
        CREATE TABLE vip_customers AS
        SELECT cd.*, tc.order_count
        FROM customer_details cd
        JOIN top_customers tc ON cd.customer_id = tc.customer_id;

        -- CREATE VIEW with CTE
        WITH revenue_by_region AS (
            SELECT region, SUM(amount) AS total_revenue
            FROM sales
            GROUP BY region
        )
        CREATE VIEW regional_revenue_view AS
        SELECT * FROM revenue_by_region WHERE total_revenue > 50000;
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        # Should have 3 queries
        assert len(results) == 3

        # First: INSERT with 1 CTE
        q1 = results[0]
        assert q1.metadata.statement_type == "INSERT"
        q1_ctes = q1.get_components_by_type(ComponentType.CTE)
        assert len(q1_ctes) == 1
        assert q1_ctes[0].name == "monthly_totals"
        q1_targets = q1.get_components_by_type(ComponentType.TARGET_TABLE)
        assert q1_targets[0].name == "monthly_summary"

        # Second: CREATE TABLE AS with 2 CTEs
        q2 = results[1]
        assert "CREATE TABLE" in q2.metadata.statement_type
        q2_ctes = q2.get_components_by_type(ComponentType.CTE)
        assert len(q2_ctes) == 2
        assert q2_ctes[0].name == "top_customers"
        assert q2_ctes[1].name == "customer_details"
        q2_targets = q2.get_components_by_type(ComponentType.TARGET_TABLE)
        assert q2_targets[0].name == "vip_customers"

        # Third: CREATE VIEW with 1 CTE
        q3 = results[2]
        assert "CREATE VIEW" in q3.metadata.statement_type
        q3_ctes = q3.get_components_by_type(ComponentType.CTE)
        assert len(q3_ctes) == 1
        assert q3_ctes[0].name == "revenue_by_region"
        q3_targets = q3.get_components_by_type(ComponentType.TARGET_TABLE)
        assert q3_targets[0].name == "regional_revenue_view"

    def test_dml_cte_component_indexing(self):
        """Test that component indices are correct across multi-query DML files."""
        sql = """
        WITH cte_a AS (SELECT 1 AS id)
        INSERT INTO table_a SELECT * FROM cte_a;

        WITH cte_b AS (SELECT 2 AS id)
        INSERT INTO table_b SELECT * FROM cte_b;
        """
        analyzer = DissectionAnalyzer(sql)
        results = analyzer.dissect_queries()

        # Each query should have independent component indexing starting from 0
        for result in results:
            # First component (CTE) should be index 0
            ctes = result.get_components_by_type(ComponentType.CTE)
            assert ctes[0].component_index == 0

            # All component indices should be sequential within each query
            indices = [c.component_index for c in result.components]
            assert indices == list(range(len(indices)))
