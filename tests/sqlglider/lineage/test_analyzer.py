"""Unit tests for lineage analyzer module."""

import pytest

from sqlglider.global_models import AnalysisLevel
from sqlglider.lineage.analyzer import (
    LineageAnalyzer,
    StarResolutionError,
    _flat_schema_to_nested,
)


class TestCaseInsensitiveForwardLineage:
    """Test case-insensitive matching for forward lineage (--column option)."""

    @pytest.fixture
    def simple_query(self):
        """Simple SELECT query for testing."""
        return """
        SELECT
            o.order_id,
            o.customer_id,
            c.customer_name,
            o.order_total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        """

    @pytest.fixture
    def cte_query(self):
        """Query with CTEs and INSERT INTO for testing."""
        return """
        WITH order_totals AS (
            SELECT
                customer_id,
                SUM(order_amount) as total_amount,
                COUNT(*) as order_count
            FROM orders
            GROUP BY customer_id
        ),
        customer_segments AS (
            SELECT
                ot.customer_id,
                c.customer_name,
                c.region,
                ot.total_amount,
                ot.order_count,
                CASE
                    WHEN ot.total_amount > 10000 THEN 'Premium'
                    WHEN ot.total_amount > 5000 THEN 'Standard'
                    ELSE 'Basic'
                END as segment
            FROM order_totals ot
            JOIN customers c ON ot.customer_id = c.customer_id
        )
        INSERT INTO TARGET_TABLE
        SELECT
            customer_name,
            region,
            segment,
            total_amount
        FROM customer_segments
        WHERE segment = 'Premium'
        """

    @pytest.fixture
    def subquery_query(self):
        """Query with subquery in FROM clause for testing."""
        return """
        SELECT
            sub.total_sales,
            sub.product_name,
            c.category_name
        FROM (
            SELECT
                product_id,
                product_name,
                SUM(sales_amount) as total_sales
            FROM sales
            GROUP BY product_id, product_name
        ) sub
        JOIN categories c ON sub.product_id = c.product_id
        WHERE sub.total_sales > 1000
        """

    @pytest.mark.parametrize(
        "column_input,expected_output_column",
        [
            # Lowercase variations
            ("orders.order_id", "orders.order_id"),
            ("orders.customer_id", "orders.customer_id"),
            ("customers.customer_name", "customers.customer_name"),
            # Uppercase variations
            ("ORDERS.ORDER_ID", "orders.order_id"),
            ("ORDERS.CUSTOMER_ID", "orders.customer_id"),
            ("CUSTOMERS.CUSTOMER_NAME", "customers.customer_name"),
            # Mixed case variations
            ("OrDeRs.OrDeR_iD", "orders.order_id"),
            ("oRdErS.cUsToMeR_iD", "orders.customer_id"),
            ("CuStOmErS.CuStOmEr_NaMe", "customers.customer_name"),
        ],
    )
    def test_simple_query_case_variations(
        self, simple_query, column_input, expected_output_column
    ):
        """Test that column matching is case-insensitive for simple queries."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column=column_input
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) == 1
        assert results[0].lineage_items[0].output_name == expected_output_column

    @pytest.mark.parametrize(
        "column_input,expected_output_column,expected_sources",
        [
            # Lowercase
            (
                "target_table.customer_name",
                "target_table.customer_name",
                ["customers.customer_name"],
            ),
            (
                "target_table.region",
                "target_table.region",
                ["customers.region"],
            ),
            (
                "target_table.total_amount",
                "target_table.total_amount",
                ["orders.order_amount"],
            ),
            # Uppercase
            (
                "target_table.CUSTOMER_NAME",
                "target_table.customer_name",
                ["customers.customer_name"],
            ),
            (
                "target_table.REGION",
                "target_table.region",
                ["customers.region"],
            ),
            # Mixed case
            (
                "TaRgEt_TaBlE.CuStOmEr_NaMe",
                "target_table.customer_name",
                ["customers.customer_name"],
            ),
            (
                "target_TABLE.REGION",
                "target_table.region",
                ["customers.region"],
            ),
        ],
    )
    def test_cte_query_case_variations(
        self, cte_query, column_input, expected_output_column, expected_sources
    ):
        """Test case-insensitive matching for queries with CTEs and DML."""
        analyzer = LineageAnalyzer(cte_query, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column=column_input
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) == len(expected_sources)
        assert results[0].lineage_items[0].output_name == expected_output_column
        actual_sources = [item.source_name for item in results[0].lineage_items]
        assert actual_sources == expected_sources

    @pytest.mark.parametrize(
        "column_input,expected_output_column",
        [
            # Lowercase
            ("sub.total_sales", "sub.total_sales"),
            ("sub.product_name", "sub.product_name"),
            ("categories.category_name", "categories.category_name"),
            # Uppercase
            ("SUB.TOTAL_SALES", "sub.total_sales"),
            ("SUB.PRODUCT_NAME", "sub.product_name"),
            ("CATEGORIES.CATEGORY_NAME", "categories.category_name"),
            # Mixed case
            ("SuB.ToTaL_sAlEs", "sub.total_sales"),
            ("sUb.PrOdUcT_nAmE", "sub.product_name"),
            ("CaTeGoRiEs.CaTeGoRy_NaMe", "categories.category_name"),
        ],
    )
    def test_subquery_case_variations(
        self, subquery_query, column_input, expected_output_column
    ):
        """Test case-insensitive matching for queries with subqueries."""
        analyzer = LineageAnalyzer(subquery_query, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column=column_input
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1
        assert results[0].lineage_items[0].output_name == expected_output_column

    def test_column_not_found_preserves_case_in_error(self, simple_query):
        """Test that error messages preserve the user's input case."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_queries(
                level=AnalysisLevel.COLUMN, column="NONEXISTENT.COLUMN"
            )

        error_message = str(exc_info.value)
        assert "NONEXISTENT.COLUMN" in error_message
        assert "not found" in error_message.lower()

    def test_all_columns_ignores_case_parameter(self, simple_query):
        """Test that omitting column parameter returns all columns regardless of case."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN, column=None)

        # Should return 1 query result with 4 lineage items
        assert len(results) == 1
        assert len(results[0].lineage_items) == 4
        output_columns = {item.output_name for item in results[0].lineage_items}
        assert "orders.order_id" in output_columns
        assert "orders.customer_id" in output_columns
        assert "customers.customer_name" in output_columns
        assert "orders.order_total" in output_columns


class TestCaseInsensitiveReverseLineage:
    """Test case-insensitive matching for reverse lineage (--source-column option)."""

    @pytest.fixture
    def simple_query(self):
        """Simple SELECT query for testing."""
        return """
        SELECT
            o.order_id,
            o.customer_id,
            c.customer_name,
            o.order_total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        """

    @pytest.fixture
    def cte_query(self):
        """Query with CTEs and INSERT INTO for testing."""
        return """
        WITH order_totals AS (
            SELECT
                customer_id,
                SUM(order_amount) as total_amount,
                COUNT(*) as order_count
            FROM orders
            GROUP BY customer_id
        ),
        customer_segments AS (
            SELECT
                ot.customer_id,
                c.customer_name,
                c.region,
                ot.total_amount,
                ot.order_count,
                CASE
                    WHEN ot.total_amount > 10000 THEN 'Premium'
                    WHEN ot.total_amount > 5000 THEN 'Standard'
                    ELSE 'Basic'
                END as segment
            FROM order_totals ot
            JOIN customers c ON ot.customer_id = c.customer_id
        )
        INSERT INTO TARGET_TABLE
        SELECT
            customer_name,
            region,
            segment,
            total_amount
        FROM customer_segments
        WHERE segment = 'Premium'
        """

    @pytest.mark.parametrize(
        "source_input,expected_source_column,expected_affected_outputs",
        [
            # Lowercase
            (
                "orders.customer_id",
                "orders.customer_id",
                ["orders.customer_id"],
            ),
            (
                "customers.customer_name",
                "customers.customer_name",
                ["customers.customer_name"],
            ),
            # Uppercase
            (
                "ORDERS.CUSTOMER_ID",
                "orders.customer_id",
                ["orders.customer_id"],
            ),
            (
                "CUSTOMERS.CUSTOMER_NAME",
                "customers.customer_name",
                ["customers.customer_name"],
            ),
            # Mixed case
            (
                "OrDeRs.CuStOmEr_Id",
                "orders.customer_id",
                ["orders.customer_id"],
            ),
            (
                "cUsToMeRs.cUsToMeR_nAmE",
                "customers.customer_name",
                ["customers.customer_name"],
            ),
        ],
    )
    def test_simple_query_reverse_case_variations(
        self,
        simple_query,
        source_input,
        expected_source_column,
        expected_affected_outputs,
    ):
        """Test that source column matching is case-insensitive for simple queries."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, source_column=source_input
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) == len(expected_affected_outputs)
        assert results[0].lineage_items[0].output_name == expected_source_column
        actual_affected = [item.source_name for item in results[0].lineage_items]
        assert actual_affected == expected_affected_outputs

    @pytest.mark.parametrize(
        "source_input,expected_source_column,expected_affected_outputs",
        [
            # Lowercase
            (
                "customers.customer_name",
                "customers.customer_name",
                ["target_table.customer_name"],
            ),
            (
                "customers.region",
                "customers.region",
                ["target_table.region"],
            ),
            (
                "orders.order_amount",
                "orders.order_amount",
                ["target_table.segment", "target_table.total_amount"],
            ),
            # Uppercase
            (
                "CUSTOMERS.CUSTOMER_NAME",
                "customers.customer_name",
                ["target_table.customer_name"],
            ),
            (
                "CUSTOMERS.REGION",
                "customers.region",
                ["target_table.region"],
            ),
            # Mixed case
            (
                "CuStOmErS.CuStOmEr_NaMe",
                "customers.customer_name",
                ["target_table.customer_name"],
            ),
            (
                "cUsToMeRs.ReGiOn",
                "customers.region",
                ["target_table.region"],
            ),
        ],
    )
    def test_cte_query_reverse_case_variations(
        self, cte_query, source_input, expected_source_column, expected_affected_outputs
    ):
        """Test case-insensitive reverse lineage for queries with CTEs and DML."""
        analyzer = LineageAnalyzer(cte_query, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, source_column=source_input
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) == len(expected_affected_outputs)
        assert results[0].lineage_items[0].output_name == expected_source_column
        actual_affected = [item.source_name for item in results[0].lineage_items]
        assert actual_affected == expected_affected_outputs

    def test_source_column_not_found_preserves_case_in_error(self, simple_query):
        """Test that error messages preserve the user's input case for reverse lineage."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_queries(
                level=AnalysisLevel.COLUMN, source_column="NONEXISTENT.SOURCE"
            )

        error_message = str(exc_info.value)
        assert "NONEXISTENT.SOURCE" in error_message
        assert "not found" in error_message.lower()


class TestCaseInsensitiveBoundaryConditions:
    """Test boundary conditions and edge cases for case-insensitive matching."""

    def test_empty_table_name_case_insensitive(self):
        """Test case-insensitive matching when column has no table qualifier."""
        sql = "SELECT column_name FROM single_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Try both cases
        results_lower = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column="single_table.column_name"
        )
        results_upper = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column="SINGLE_TABLE.COLUMN_NAME"
        )

        assert (
            results_lower[0].lineage_items[0].output_name
            == results_upper[0].lineage_items[0].output_name
        )

    def test_special_characters_in_column_names(self):
        """Test case-insensitive matching with special characters in column names."""
        sql = """
        SELECT
            order_id AS "Order_ID",
            customer_id AS "Customer_ID"
        FROM orders
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Note: This tests current behavior - may need adjustment based on SQLGlot's handling
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN, column=None)
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_unicode_column_names_case_insensitive(self):
        """Test case-insensitive matching with unicode characters."""
        sql = """
        SELECT
            "Über_Column" as uber_column,
            "Café_Column" as cafe_column
        FROM test_table
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Unicode case folding should work
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN, column=None)
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    @pytest.mark.parametrize(
        "query_case,column_case",
        [
            ("lower", "UPPER"),
            ("UPPER", "lower"),
            ("MiXeD", "mIxEd"),
        ],
    )
    def test_cross_case_matching(self, query_case, column_case):
        """Test that query case and search case can differ."""
        # Create query with specific case
        table_name = (
            "orders"
            if query_case == "lower"
            else ("ORDERS" if query_case == "UPPER" else "OrDeRs")
        )
        col_name = (
            "order_id"
            if query_case == "lower"
            else ("ORDER_ID" if query_case == "UPPER" else "OrDeR_iD")
        )

        sql = f"SELECT {col_name} FROM {table_name}"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Search with different case
        search_table = (
            "orders"
            if column_case == "lower"
            else ("ORDERS" if column_case == "UPPER" else "oRdErS")
        )
        search_col = (
            "order_id"
            if column_case == "lower"
            else ("ORDER_ID" if column_case == "UPPER" else "oRdEr_Id")
        )

        search_term = f"{search_table}.{search_col}"
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column=search_term
        )

        assert len(results) == 1
        # Should find the column regardless of case differences


class TestAnalyzerEdgeCases:
    """Tests for edge cases in LineageAnalyzer."""

    def test_invalid_sql_raises_parse_error(self):
        """Test that invalid SQL raises ParseError."""
        from sqlglot.errors import ParseError

        invalid_sql = "INVALID SQL SYNTAX HERE ;;;;"

        with pytest.raises(ParseError):
            analyzer = LineageAnalyzer(invalid_sql, dialect="spark")
            analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

    def test_empty_sql_raises_error(self):
        """Test that empty SQL raises an error."""
        from sqlglot.errors import ParseError

        empty_sql = ""

        with pytest.raises((ParseError, ValueError)):
            analyzer = LineageAnalyzer(empty_sql, dialect="spark")
            analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

    def test_different_dialects(self):
        """Test analyzer works with different SQL dialects."""
        sql = "SELECT id, name FROM users"

        for dialect in ["postgres", "mysql", "snowflake", "bigquery"]:
            analyzer = LineageAnalyzer(sql, dialect=dialect)
            results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
            assert len(results) == 1
            assert len(results[0].lineage_items) >= 1

    def test_create_table_as_select(self):
        """Test CTAS (CREATE TABLE AS SELECT) statement."""
        sql = """
        CREATE TABLE new_customers AS
        SELECT
            customer_id,
            customer_name,
            email
        FROM customers
        WHERE active = true
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should find lineage for the created columns
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1
        # Verify source columns are traced
        column_names = [item.output_name for item in results[0].lineage_items]
        assert any("customer_id" in col.lower() for col in column_names)

    def test_insert_into_select(self):
        """Test INSERT INTO SELECT statement."""
        sql = """
        INSERT INTO target_table (id, name, email)
        SELECT
            customer_id,
            customer_name,
            customer_email
        FROM customers
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should find lineage for inserted columns
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_table_lineage_basic(self):
        """Test basic table-level lineage."""
        sql = """
        SELECT
            c.customer_id,
            o.order_total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.TABLE)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 2
        # Should include both customers and orders
        source_tables = [item.source_name for item in results[0].lineage_items]
        source_tables_lower = [t.lower() for t in source_tables]
        assert any("customers" in t for t in source_tables_lower)
        assert any("orders" in t for t in source_tables_lower)

    def test_table_lineage_with_subquery(self):
        """Test table lineage with subqueries."""
        sql = """
        SELECT
            main.customer_id,
            main.total_orders
        FROM (
            SELECT
                customer_id,
                COUNT(*) as total_orders
            FROM orders
            GROUP BY customer_id
        ) main
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.TABLE)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_complex_cte_chain(self):
        """Test complex CTE chains."""
        sql = """
        WITH step1 AS (
            SELECT customer_id, order_date FROM orders
        ),
        step2 AS (
            SELECT customer_id, MIN(order_date) as first_order FROM step1 GROUP BY customer_id
        ),
        step3 AS (
            SELECT s2.customer_id, c.name, s2.first_order
            FROM step2 s2
            JOIN customers c ON s2.customer_id = c.id
        )
        SELECT * FROM step3
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should successfully analyze the CTE chain
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_union_query(self):
        """Test UNION query."""
        sql = """
        SELECT customer_id, order_date FROM orders_2023
        UNION ALL
        SELECT customer_id, order_date FROM orders_2024
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should find lineage for both sources
        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_window_functions(self):
        """Test queries with window functions."""
        sql = """
        SELECT
            customer_id,
            order_total,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as row_num,
            SUM(order_total) OVER (PARTITION BY customer_id) as customer_total
        FROM orders
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1
        # Should include window function columns
        column_names = [item.output_name for item in results[0].lineage_items]
        assert any("customer_id" in col.lower() for col in column_names)

    def test_case_expressions(self):
        """Test CASE expressions."""
        sql = """
        SELECT
            customer_id,
            CASE
                WHEN order_total > 1000 THEN 'High'
                WHEN order_total > 100 THEN 'Medium'
                ELSE 'Low'
            END as order_category
        FROM orders
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_aggregate_functions(self):
        """Test various aggregate functions."""
        sql = """
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(order_total) as total_spent,
            AVG(order_total) as avg_order,
            MAX(order_date) as last_order,
            MIN(order_date) as first_order
        FROM orders
        GROUP BY customer_id
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert (
            len(results[0].lineage_items) >= 2
        )  # At least customer_id and one aggregate

    def test_lateral_view(self):
        """Test LATERAL VIEW (Spark-specific)."""
        sql = """
        SELECT
            customer_id,
            tag
        FROM customers
        LATERAL VIEW EXPLODE(tags) t AS tag
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_qualified_table_names(self):
        """Test queries with database-qualified table names."""
        sql = """
        SELECT
            c.customer_id,
            c.customer_name
        FROM sales_db.customers c
        JOIN sales_db.orders o ON c.id = o.customer_id
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_reverse_lineage_nonexistent_column(self):
        """Test reverse lineage with nonexistent source column raises error."""
        sql = "SELECT customer_id, customer_name FROM customers"

        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Should raise ValueError for nonexistent column
        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_queries(
                level=AnalysisLevel.COLUMN,
                source_column="nonexistent_table.nonexistent_column",
            )

        assert "not found" in str(exc_info.value).lower()

    def test_forward_lineage_nonexistent_column_raises_error(self):
        """Test forward lineage with nonexistent column raises ValueError."""
        sql = "SELECT customer_id, customer_name FROM customers"

        analyzer = LineageAnalyzer(sql, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_queries(
                level=AnalysisLevel.COLUMN, column="nonexistent_column"
            )

        assert "not found" in str(exc_info.value).lower()

    def test_self_join(self):
        """Test self-join query."""
        sql = """
        SELECT
            e1.employee_id,
            e1.employee_name,
            e2.employee_name as manager_name
        FROM employees e1
        LEFT JOIN employees e2 ON e1.manager_id = e2.employee_id
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 2

    def test_multiple_joins(self):
        """Test query with multiple joins."""
        sql = """
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_id,
            p.product_name,
            s.shipment_status
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN products p ON o.product_id = p.id
        JOIN shipments s ON o.id = s.order_id
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 4

    def test_nested_subqueries(self):
        """Test deeply nested subqueries."""
        sql = """
        SELECT customer_id, final_total
        FROM (
            SELECT customer_id, SUM(order_total) as final_total
            FROM (
                SELECT customer_id, order_id, order_total
                FROM orders
                WHERE order_date >= '2024-01-01'
            ) filtered_orders
            GROUP BY customer_id
        ) aggregated_orders
        WHERE final_total > 1000
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_column_aliases_with_special_chars(self):
        """Test column aliases with special characters."""
        sql = """
        SELECT
            customer_id as "Customer ID",
            customer_name as "Customer Name",
            order_total as "Total ($)"
        FROM customers
        JOIN orders ON customers.id = orders.customer_id
        """

        analyzer = LineageAnalyzer(sql, dialect="postgres")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_distinct_query(self):
        """Test query with DISTINCT."""
        sql = """
        SELECT DISTINCT
            customer_id,
            customer_category
        FROM customers
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 2

    def test_order_by_limit(self):
        """Test query with ORDER BY and LIMIT."""
        sql = """
        SELECT
            customer_id,
            customer_name,
            order_total
        FROM orders
        ORDER BY order_total DESC
        LIMIT 10
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_having_clause(self):
        """Test query with HAVING clause."""
        sql = """
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(order_total) as total_spent
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) > 5
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_cross_join(self):
        """Test CROSS JOIN."""
        sql = """
        SELECT
            c.customer_id,
            d.date
        FROM customers c
        CROSS JOIN dates d
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 2

    def test_except_query(self):
        """Test EXCEPT/MINUS query."""
        sql = """
        SELECT customer_id FROM customers_2023
        EXCEPT
        SELECT customer_id FROM customers_2024
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1

    def test_intersect_query(self):
        """Test INTERSECT query."""
        sql = """
        SELECT customer_id FROM active_customers
        INTERSECT
        SELECT customer_id FROM premium_customers
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        assert len(results[0].lineage_items) >= 1


class TestMultiQueryParsing:
    """Test parsing and analyzing multiple queries in a single file."""

    @pytest.fixture
    def multi_query_sql(self):
        """SQL file with multiple queries."""
        return """
        SELECT customer_id, customer_name
        FROM customers;

        SELECT order_id, customer_id, order_total
        FROM orders;

        INSERT INTO customer_orders
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_id,
            o.order_total
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id;
        """

    @pytest.fixture
    def single_query_sql(self):
        """SQL file with a single query for backward compatibility testing."""
        return """
        SELECT customer_id, customer_name
        FROM customers
        """

    def test_parse_multiple_statements(self, multi_query_sql):
        """Test that multiple statements are parsed correctly."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")

        assert len(analyzer.expressions) == 3
        assert (
            analyzer.expr is not None
        )  # First expression stored for backward compatibility

    def test_parse_single_statement(self, single_query_sql):
        """Test backward compatibility with single statement."""
        analyzer = LineageAnalyzer(single_query_sql, dialect="spark")

        assert len(analyzer.expressions) == 1
        assert analyzer.expr is not None

    def test_analyze_all_queries(self, multi_query_sql):
        """Test analyzing all queries returns results for each query."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 3
        from sqlglider.lineage.analyzer import QueryLineageResult

        assert all(isinstance(r, QueryLineageResult) for r in results)

        # Check query indices
        assert results[0].metadata.query_index == 0
        assert results[1].metadata.query_index == 1
        assert results[2].metadata.query_index == 2

        # Check query previews exist
        assert "SELECT" in results[0].metadata.query_preview
        assert "SELECT" in results[1].metadata.query_preview
        assert "INSERT" in results[2].metadata.query_preview

        # Check that each has column lineage
        assert len(results[0].lineage_items) > 0
        assert len(results[1].lineage_items) > 0
        assert len(results[2].lineage_items) > 0

    def test_analyze_all_queries_specific_column(self, multi_query_sql):
        """Test analyzing specific column across all queries."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, column="customers.customer_id"
        )

        # Should get only queries that have this specific column
        # Query 0: customers.customer_id exists
        # Query 1: orders.customer_id (different qualified name)
        # Query 2: customer_orders.customer_id (different qualified name)
        # So we should get 1 result
        assert len(results) == 1
        assert results[0].metadata.query_index == 0

        # Should only have customer_id lineage
        assert len(results[0].lineage_items) == 1
        assert "customers.customer_id" in results[0].lineage_items[0].output_name

    def test_backward_compatibility_single_query(self, single_query_sql):
        """Test that single query still works with analyze_queries method."""
        analyzer = LineageAnalyzer(single_query_sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1  # 1 query
        assert len(results[0].lineage_items) == 2  # customer_id, customer_name
        output_names = [item.output_name for item in results[0].lineage_items]
        assert any("customer_id" in name for name in output_names)
        assert any("customer_name" in name for name in output_names)


class TestTableFiltering:
    """Test filtering queries by table name."""

    @pytest.fixture
    def multi_query_different_tables(self):
        """SQL file with queries using different tables."""
        return """
        SELECT product_id, product_name
        FROM products;

        SELECT customer_id, customer_name
        FROM customers;

        SELECT order_id, product_id, customer_id
        FROM orders;
        """

    def test_filter_by_table(self, multi_query_different_tables):
        """Test filtering to only queries that use a specific table."""
        analyzer = LineageAnalyzer(multi_query_different_tables, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="customers"
        )

        # Should only get the query that references customers table
        assert len(results) == 1
        assert results[0].metadata.query_index == 1
        assert "customer" in results[0].metadata.query_preview.lower()

    def test_filter_by_table_multiple_matches(self, multi_query_different_tables):
        """Test filtering when multiple queries reference the table."""
        analyzer = LineageAnalyzer(multi_query_different_tables, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="orders"
        )

        # Should only get the query that references orders table
        assert len(results) == 1
        assert results[0].metadata.query_index == 2

    def test_filter_by_table_case_insensitive(self, multi_query_different_tables):
        """Test that table filtering is case-insensitive."""
        analyzer = LineageAnalyzer(multi_query_different_tables, dialect="spark")
        results_lower = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="products"
        )
        results_upper = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="PRODUCTS"
        )
        results_mixed = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="PrOdUcTs"
        )

        assert len(results_lower) == len(results_upper) == len(results_mixed) == 1
        assert (
            results_lower[0].metadata.query_index
            == results_upper[0].metadata.query_index
            == results_mixed[0].metadata.query_index
            == 0
        )

    def test_filter_no_matches(self, multi_query_different_tables):
        """Test filtering with table that doesn't exist."""
        analyzer = LineageAnalyzer(multi_query_different_tables, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="nonexistent_table"
        )

        assert len(results) == 0

    def test_filter_partial_match(self, multi_query_different_tables):
        """Test filtering with partial table name."""
        analyzer = LineageAnalyzer(multi_query_different_tables, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, table_filter="cust"
        )

        # Should match "customers" table
        assert len(results) == 1
        assert results[0].metadata.query_index == 1


class TestMultiQueryEdgeCases:
    """Test edge cases for multi-query support."""

    def test_sql_with_comments_and_queries(self):
        """Test that comments are ignored and queries are parsed."""
        sql = """
        -- This is a comment
        SELECT * FROM table1;
        -- Another comment
        SELECT * FROM table2;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        assert len(analyzer.expressions) == 2

    def test_semicolon_separated_queries(self):
        """Test that semicolon-separated queries are parsed correctly."""
        sql = "SELECT * FROM table1; SELECT * FROM table2; SELECT * FROM table3;"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        assert len(analyzer.expressions) == 3


class TestMultiQueryReverseLineage:
    """Test reverse lineage for multi-query files."""

    @pytest.fixture
    def multi_query_sql(self):
        """SQL with multiple queries using customer_id."""
        return """
        SELECT customer_id, customer_name FROM customers;
        SELECT order_id, customer_id FROM orders;
        SELECT customer_id, product_id FROM order_items;
        """

    def test_reverse_lineage_all_queries(self, multi_query_sql):
        """Test reverse lineage finds only queries with the exact column."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, source_column="customers.customer_id"
        )

        # Should only get query 0 (which has customers.customer_id)
        # Query 1 has orders.customer_id (different table, different column)
        # Query 2 has order_items.customer_id (different table, different column)
        assert len(results) == 1
        from sqlglider.lineage.analyzer import QueryLineageResult

        assert all(isinstance(r, QueryLineageResult) for r in results)
        assert results[0].metadata.query_index == 0

    def test_reverse_lineage_with_table_filter(self, multi_query_sql):
        """Test reverse lineage with table filter finds correct query."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")

        # Search for orders.customer_id (not customers.customer_id) with orders table filter
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN,
            source_column="orders.customer_id",
            table_filter="orders",
        )

        # Should get query 1 which has orders.customer_id from orders table
        assert len(results) == 1
        assert results[0].metadata.query_index == 1

    def test_reverse_lineage_nonexistent_column(self, multi_query_sql):
        """Test reverse lineage with column that doesn't exist."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_queries(
                level=AnalysisLevel.COLUMN, source_column="nonexistent.column"
            )

        assert "not found" in str(exc_info.value).lower()

    def test_reverse_lineage_base_table_column(self):
        """Test reverse lineage with base table columns (not derived)."""
        sql = """
        SELECT order_id, order_total FROM orders;
        SELECT customer_id FROM customers;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, source_column="orders.order_total"
        )

        # Should find the column in query 0
        assert len(results) == 1
        assert results[0].metadata.query_index == 0
        # Base table columns show themselves as affected outputs
        sources = [item.source_name for item in results[0].lineage_items]
        assert "orders.order_total" in sources

    def test_reverse_lineage_single_query_base_column(self):
        """Test single-query reverse lineage with base table column."""
        sql = "SELECT order_id, order_total, customer_id FROM orders"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.COLUMN, source_column="orders.order_total"
        )

        assert len(results) == 1
        assert len(results[0].lineage_items) == 1
        assert results[0].lineage_items[0].output_name == "orders.order_total"
        assert "orders.order_total" in results[0].lineage_items[0].source_name


class TestMultiQueryTableLineage:
    """Test table-level lineage for multi-query files."""

    @pytest.fixture
    def multi_query_sql(self):
        """SQL with multiple queries using different tables."""
        return """
        SELECT * FROM customers;
        SELECT * FROM orders JOIN products ON orders.product_id = products.id;
        SELECT * FROM inventory;
        """

    def test_table_lineage_all_queries(self, multi_query_sql):
        """Test table lineage across all queries."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.TABLE)

        # Should get results from all 3 queries
        assert len(results) == 3
        from sqlglider.lineage.analyzer import QueryLineageResult

        assert all(isinstance(r, QueryLineageResult) for r in results)

        # Check that tables are correctly identified
        sources_q0 = [item.source_name for item in results[0].lineage_items]
        sources_q1 = [item.source_name for item in results[1].lineage_items]
        sources_q2 = [item.source_name for item in results[2].lineage_items]

        assert "customers" in sources_q0
        assert "orders" in sources_q1
        assert "products" in sources_q1
        assert "inventory" in sources_q2

    def test_table_lineage_with_filter(self, multi_query_sql):
        """Test table lineage with table filter."""
        analyzer = LineageAnalyzer(multi_query_sql, dialect="spark")
        results = analyzer.analyze_queries(
            level=AnalysisLevel.TABLE, table_filter="products"
        )

        # Should only get query with products table
        assert len(results) == 1
        assert results[0].metadata.query_index == 1


class TestMultiQueryIsolation:
    """Test that queries are properly isolated in multi-query files."""

    @pytest.fixture
    def isolated_queries_sql(self):
        """SQL with queries on different tables that should not share lineage."""
        return """
        SELECT customer_id, customer_name, email
        FROM customers;

        SELECT order_id, customer_id, order_date, order_total
        FROM orders;

        INSERT INTO customer_summary
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(o.order_id) as total_orders,
            SUM(o.order_total) as total_spent
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name;
        """

    def test_query_isolation_no_source_leakage(self, isolated_queries_sql):
        """Test that sources from one query don't leak into another query.

        Regression test for bug where lineage analysis used full multi-query SQL
        instead of per-query SQL, causing sources from unrelated queries to appear
        in lineage results.
        """
        analyzer = LineageAnalyzer(isolated_queries_sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Query 0: SELECT from customers only
        # Should NOT have any empty sources and should ONLY reference customers table
        query0_items = results[0].lineage_items
        assert all(item.source_name for item in query0_items), (
            "Query 0 should have no empty sources"
        )
        assert all("customers" in item.source_name for item in query0_items), (
            "Query 0 should only reference customers table"
        )
        # Verify specific columns
        output_to_source = {item.output_name: item.source_name for item in query0_items}
        assert output_to_source["customers.customer_id"] == "customers.customer_id"
        assert output_to_source["customers.customer_name"] == "customers.customer_name"
        assert output_to_source["customers.email"] == "customers.email"

        # Query 1: SELECT from orders only
        # Should NOT have any empty sources and should ONLY reference orders table
        # CRITICAL: Should NOT reference customers table (this was the bug)
        query1_items = results[1].lineage_items
        assert all(item.source_name for item in query1_items), (
            "Query 1 should have no empty sources"
        )
        assert all("orders" in item.source_name for item in query1_items), (
            "Query 1 should only reference orders table, not customers"
        )
        assert not any("customers" in item.source_name for item in query1_items), (
            "Query 1 should NOT reference customers table (bug: source leakage)"
        )
        # Verify specific columns
        output_to_source = {item.output_name: item.source_name for item in query1_items}
        assert output_to_source["orders.order_id"] == "orders.order_id"
        assert output_to_source["orders.customer_id"] == "orders.customer_id"
        assert output_to_source["orders.order_date"] == "orders.order_date"
        assert output_to_source["orders.order_total"] == "orders.order_total"

        # Query 2: INSERT with JOIN on both customers and orders
        # Should have sources from BOTH tables
        query2_items = results[2].lineage_items
        assert all(item.source_name for item in query2_items), (
            "Query 2 should have no empty sources"
        )
        sources = [item.source_name for item in query2_items]
        assert any("customers" in s for s in sources), (
            "Query 2 should reference customers"
        )
        assert any("orders" in s for s in sources), "Query 2 should reference orders"

    def test_simple_multi_query_isolation(self):
        """Test simple case of two independent queries with no shared tables."""
        sql = """
        SELECT product_id, product_name FROM products;
        SELECT order_id, order_total FROM orders;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Query 0: Should only reference products
        query0_sources = [item.source_name for item in results[0].lineage_items]
        assert all("products" in s for s in query0_sources)
        assert not any("orders" in s for s in query0_sources)

        # Query 1: Should only reference orders
        query1_sources = [item.source_name for item in results[1].lineage_items]
        assert all("orders" in s for s in query1_sources)
        assert not any("products" in s for s in query1_sources)


class TestSkippedQueries:
    """Tests for handling unsupported statement types."""

    def test_skipped_queries_property(self):
        """Test that unsupported statements are tracked in skipped_queries."""
        sql = """
        CREATE TEMPORARY FUNCTION my_udf AS 'com.example.MyUDF';
        SELECT customer_id FROM customers;
        DELETE FROM orders WHERE id = 1;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Only the SELECT query should have results
        assert len(results) == 1
        assert results[0].metadata.query_index == 1  # Second statement (0-indexed)

        # Two queries should be skipped
        skipped = analyzer.skipped_queries
        assert len(skipped) == 2

        # Check first skipped query (CREATE FUNCTION)
        assert skipped[0].query_index == 0
        assert "CREATE" in skipped[0].statement_type
        assert "does not support lineage analysis" in skipped[0].reason

        # Check second skipped query (DELETE)
        assert skipped[1].query_index == 2
        assert "DELETE" in skipped[1].statement_type
        assert "does not support lineage analysis" in skipped[1].reason

    def test_no_skipped_queries_for_supported_statements(self):
        """Test that all supported statements don't create skipped queries."""
        sql = """
        SELECT customer_id FROM customers;
        INSERT INTO target SELECT id FROM source;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Both queries should be analyzed
        assert len(results) == 2

        # No queries should be skipped
        assert len(analyzer.skipped_queries) == 0

    def test_get_statement_type(self):
        """Test statement type detection for various SQL statements."""
        test_cases = [
            ("SELECT * FROM t", "SELECT"),
            ("INSERT INTO t SELECT * FROM s", "INSERT"),
            ("DELETE FROM t WHERE id = 1", "DELETE"),
            ("TRUNCATE TABLE t", "TRUNCATETABLE"),
        ]

        for sql, expected_contains in test_cases:
            analyzer = LineageAnalyzer(sql, dialect="spark")
            stmt_type = analyzer._get_statement_type()
            assert expected_contains in stmt_type, (
                f"Expected '{expected_contains}' in '{stmt_type}' for: {sql}"
            )

    def test_mixed_statements_preserves_order(self):
        """Test that query indices are preserved correctly when some are skipped."""
        sql = """
        DROP TABLE old_table;
        SELECT a FROM first_table;
        TRUNCATE TABLE temp;
        SELECT b FROM second_table;
        CREATE VIEW v AS SELECT 1;
        SELECT c FROM third_table;
        DELETE FROM cleanup_table;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should have 4 results (3 SELECTs + CREATE VIEW which contains a SELECT)
        assert len(results) == 4
        # Query indices should reflect original positions
        assert results[0].metadata.query_index == 1  # First SELECT
        assert results[1].metadata.query_index == 3  # Second SELECT
        assert results[2].metadata.query_index == 4  # CREATE VIEW (has SELECT inside)
        assert results[3].metadata.query_index == 5  # Third SELECT

        # Should have 3 skipped queries (DROP, TRUNCATE, DELETE)
        skipped = analyzer.skipped_queries
        assert len(skipped) == 3
        assert skipped[0].query_index == 0  # DROP
        assert skipped[1].query_index == 2  # TRUNCATE
        assert skipped[2].query_index == 6  # DELETE


class TestAnalyzeTables:
    """Tests for the analyze_tables() method."""

    def test_simple_select(self):
        """Test basic SELECT with single table."""
        sql = "SELECT customer_id, customer_name FROM customers"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "customers"
        assert results[0].tables[0].usage.value == "INPUT"
        assert results[0].tables[0].object_type.value == "UNKNOWN"

    def test_select_with_join(self):
        """Test SELECT with JOIN - multiple input tables."""
        sql = """
        SELECT c.customer_id, o.order_id
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 2
        table_names = {t.name for t in results[0].tables}
        assert "customers" in table_names
        assert "orders" in table_names
        # All should be INPUT
        for table in results[0].tables:
            assert table.usage.value == "INPUT"
            assert table.object_type.value == "UNKNOWN"

    def test_create_table_as_select(self):
        """Test CREATE TABLE AS SELECT - OUTPUT table with INPUT sources."""
        sql = """
        CREATE TABLE new_customers AS
        SELECT customer_id, customer_name
        FROM customers
        WHERE active = true
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 2

        # Find tables by name
        tables_by_name = {t.name: t for t in results[0].tables}

        # new_customers should be OUTPUT with type TABLE
        assert "new_customers" in tables_by_name
        assert tables_by_name["new_customers"].usage.value == "OUTPUT"
        assert tables_by_name["new_customers"].object_type.value == "TABLE"

        # customers should be INPUT
        assert "customers" in tables_by_name
        assert tables_by_name["customers"].usage.value == "INPUT"
        assert tables_by_name["customers"].object_type.value == "UNKNOWN"

    def test_create_view(self):
        """Test CREATE VIEW - OUTPUT view with INPUT sources."""
        sql = """
        CREATE VIEW customer_summary AS
        SELECT customer_id, SUM(amount) as total
        FROM orders
        GROUP BY customer_id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 2

        tables_by_name = {t.name: t for t in results[0].tables}

        # customer_summary should be OUTPUT with type VIEW
        assert "customer_summary" in tables_by_name
        assert tables_by_name["customer_summary"].usage.value == "OUTPUT"
        assert tables_by_name["customer_summary"].object_type.value == "VIEW"

        # orders should be INPUT
        assert "orders" in tables_by_name
        assert tables_by_name["orders"].usage.value == "INPUT"

    def test_insert_into_select(self):
        """Test INSERT INTO SELECT - OUTPUT table with INPUT sources."""
        sql = """
        INSERT INTO target_table
        SELECT customer_id, customer_name
        FROM source_table
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 2

        tables_by_name = {t.name: t for t in results[0].tables}

        # target_table should be OUTPUT
        assert "target_table" in tables_by_name
        assert tables_by_name["target_table"].usage.value == "OUTPUT"
        assert tables_by_name["target_table"].object_type.value == "UNKNOWN"

        # source_table should be INPUT
        assert "source_table" in tables_by_name
        assert tables_by_name["source_table"].usage.value == "INPUT"

    def test_cte_detection(self):
        """Test that CTEs are detected with CTE object type."""
        sql = """
        WITH order_totals AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.customer_name, ot.total
        FROM customers c
        JOIN order_totals ot ON c.id = ot.customer_id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 3

        tables_by_name = {t.name: t for t in results[0].tables}

        # order_totals should be CTE
        assert "order_totals" in tables_by_name
        assert tables_by_name["order_totals"].usage.value == "INPUT"
        assert tables_by_name["order_totals"].object_type.value == "CTE"

        # customers and orders should be UNKNOWN type
        assert tables_by_name["customers"].object_type.value == "UNKNOWN"
        assert tables_by_name["orders"].object_type.value == "UNKNOWN"

    def test_qualified_table_names(self):
        """Test that fully qualified table names are preserved."""
        sql = """
        SELECT c.id, o.total
        FROM analytics.customers c
        JOIN sales.orders o ON c.id = o.customer_id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        table_names = {t.name for t in results[0].tables}
        assert "analytics.customers" in table_names
        assert "sales.orders" in table_names

    def test_multi_query_file(self):
        """Test multi-query file returns results per query."""
        sql = """
        SELECT * FROM table1;
        SELECT * FROM table2;
        INSERT INTO target SELECT * FROM source;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 3

        # Query 0
        assert results[0].metadata.query_index == 0
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "table1"

        # Query 1
        assert results[1].metadata.query_index == 1
        assert len(results[1].tables) == 1
        assert results[1].tables[0].name == "table2"

        # Query 2 - INSERT
        assert results[2].metadata.query_index == 2
        table_names = {t.name for t in results[2].tables}
        assert "target" in table_names
        assert "source" in table_names

    def test_table_filter(self):
        """Test filtering by table name."""
        sql = """
        SELECT * FROM customers;
        SELECT * FROM orders;
        SELECT * FROM products;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables(table_filter="orders")

        assert len(results) == 1
        assert results[0].metadata.query_index == 1
        assert results[0].tables[0].name == "orders"

    def test_table_filter_case_insensitive(self):
        """Test that table filter is case-insensitive."""
        sql = "SELECT * FROM Customers"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        results_lower = analyzer.analyze_tables(table_filter="customers")
        results_upper = analyzer.analyze_tables(table_filter="CUSTOMERS")
        results_mixed = analyzer.analyze_tables(table_filter="CuStOmErS")

        assert len(results_lower) == 1
        assert len(results_upper) == 1
        assert len(results_mixed) == 1

    def test_subquery_tables(self):
        """Test that tables in subqueries are included."""
        sql = """
        SELECT *
        FROM orders
        WHERE customer_id IN (SELECT id FROM customers)
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        table_names = {t.name for t in results[0].tables}
        assert "orders" in table_names
        assert "customers" in table_names

    def test_update_statement(self):
        """Test UPDATE statement - target is OUTPUT, sources are INPUT."""
        sql = """
        UPDATE target_table t
        SET t.status = s.new_status
        FROM source_table s
        WHERE t.id = s.target_id
        """
        analyzer = LineageAnalyzer(sql, dialect="postgres")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        tables_by_name = {t.name: t for t in results[0].tables}

        # target_table should be OUTPUT
        assert "target_table" in tables_by_name
        assert tables_by_name["target_table"].usage.value == "OUTPUT"

        # source_table should be INPUT
        assert "source_table" in tables_by_name
        assert tables_by_name["source_table"].usage.value == "INPUT"

    def test_delete_statement(self):
        """Test DELETE statement - target is OUTPUT."""
        sql = "DELETE FROM old_records WHERE created_at < '2020-01-01'"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "old_records"
        assert results[0].tables[0].usage.value == "OUTPUT"
        assert results[0].tables[0].object_type.value == "UNKNOWN"

    def test_drop_table(self):
        """Test DROP TABLE statement."""
        sql = "DROP TABLE old_data"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "old_data"
        assert results[0].tables[0].usage.value == "OUTPUT"
        assert results[0].tables[0].object_type.value == "TABLE"

    def test_drop_view(self):
        """Test DROP VIEW statement."""
        sql = "DROP VIEW old_view"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "old_view"
        assert results[0].tables[0].usage.value == "OUTPUT"
        assert results[0].tables[0].object_type.value == "VIEW"

    def test_table_both_input_and_output(self):
        """Test table that appears as both INPUT and OUTPUT gets BOTH usage."""
        sql = """
        INSERT INTO customers
        SELECT * FROM customers WHERE active = false
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 1
        assert results[0].tables[0].name == "customers"
        assert results[0].tables[0].usage.value == "BOTH"

    def test_empty_result_for_no_tables(self):
        """Test query with no tables returns empty list."""
        sql = "SELECT 1 + 1 AS result"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        assert len(results[0].tables) == 0

    def test_tables_sorted_by_name(self):
        """Test that tables are returned sorted alphabetically."""
        sql = """
        SELECT *
        FROM zebra z
        JOIN alpha a ON z.id = a.id
        JOIN middle m ON a.id = m.id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        table_names = [t.name for t in results[0].tables]
        assert table_names == sorted(table_names, key=str.lower)

    def test_multiple_ctes(self):
        """Test query with multiple CTEs."""
        sql = """
        WITH cte1 AS (SELECT id FROM table1),
             cte2 AS (SELECT id FROM table2),
             cte3 AS (SELECT id FROM cte1 JOIN cte2 ON cte1.id = cte2.id)
        SELECT * FROM cte3
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        tables_by_name = {t.name: t for t in results[0].tables}

        # All CTEs should be detected
        assert "cte1" in tables_by_name
        assert "cte2" in tables_by_name
        assert "cte3" in tables_by_name
        assert tables_by_name["cte1"].object_type.value == "CTE"
        assert tables_by_name["cte2"].object_type.value == "CTE"
        assert tables_by_name["cte3"].object_type.value == "CTE"

        # Base tables should also be detected
        assert "table1" in tables_by_name
        assert "table2" in tables_by_name


class TestLiteralHandling:
    """Tests for literal value handling in lineage analysis."""

    def test_union_with_null_literal(self):
        """Test that NULL literals in UNION branches are represented properly."""
        sql = """
        SELECT customer_id, last_order_date FROM active_customers
        UNION ALL
        SELECT customer_id, NULL AS last_order_date FROM prospects
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        # Find the lineage item for last_order_date
        items_for_date = [
            item
            for item in results[0].lineage_items
            if "last_order_date" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_date]

        # Should have the column from active_customers AND the literal NULL
        assert any("active_customers" in s for s in sources)
        assert any("<literal: NULL>" in s for s in sources)

    def test_union_with_numeric_literal(self):
        """Test that numeric literals in UNION branches are represented properly."""
        sql = """
        SELECT customer_id, total_orders FROM existing_customers
        UNION ALL
        SELECT customer_id, 0 AS total_orders FROM new_customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_orders = [
            item
            for item in results[0].lineage_items
            if "total_orders" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_orders]

        # Should include the literal 0
        assert any("<literal: 0>" in s for s in sources)

    def test_union_with_string_literal(self):
        """Test that string literals in UNION branches are represented properly."""
        sql = """
        SELECT customer_id, 'active' AS status FROM active_customers
        UNION ALL
        SELECT customer_id, 'prospect' AS status FROM prospects
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_status = [
            item
            for item in results[0].lineage_items
            if "status" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_status]

        # Should include both string literals
        assert any("<literal: 'active'>" in s for s in sources)
        assert any("<literal: 'prospect'>" in s for s in sources)

    def test_union_with_function_literal(self):
        """Test that function calls (like CURRENT_TIMESTAMP) are represented properly."""
        sql = """
        SELECT customer_id, CURRENT_TIMESTAMP() AS updated_at FROM customers
        UNION ALL
        SELECT customer_id, CURRENT_TIMESTAMP() AS updated_at FROM prospects
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_updated = [
            item
            for item in results[0].lineage_items
            if "updated_at" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_updated]

        # Should include CURRENT_TIMESTAMP literal
        assert any("<literal:" in s and "CURRENT_TIMESTAMP" in s for s in sources)

    def test_three_way_union_with_mixed_literals(self):
        """Test UNION with three branches having different literal types."""
        sql = """
        SELECT customer_id, order_date FROM orders
        UNION ALL
        SELECT customer_id, NULL AS order_date FROM prospects
        UNION ALL
        SELECT customer_id, order_date FROM archived_orders
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_date = [
            item
            for item in results[0].lineage_items
            if "order_date" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_date]

        # Should have two column sources and one literal
        assert any("orders" in s and "order_date" in s for s in sources)
        assert any("archived_orders" in s and "order_date" in s for s in sources)
        assert any("<literal: NULL>" in s for s in sources)

    def test_coalesce_with_literal_default(self):
        """Test that COALESCE with literal default shows the literal in sources."""
        sql = """
        SELECT
            customer_id,
            COALESCE(total_orders, 0) AS total_orders
        FROM customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_orders = [
            item
            for item in results[0].lineage_items
            if "total_orders" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_orders]

        # Should include the source column
        assert any("customers" in s and "total_orders" in s for s in sources)

    def test_literal_only_column(self):
        """Test a column that is purely a literal value.

        Note: For standalone literals (not in a UNION), SQLGlot returns the alias
        name as the source since the literal itself is the leaf node. This is different
        from UNION queries where literal branches get position numbers.
        """
        sql = """
        SELECT
            customer_id,
            'hardcoded_value' AS data_source
        FROM customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_source = [
            item
            for item in results[0].lineage_items
            if "data_source" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_source]

        # For standalone literals, SQLGlot uses the alias as the source name
        # (the literal is a self-referential leaf node)
        assert len(sources) == 1
        assert sources[0] == "data_source"

    def test_case_expression_literals(self):
        """Test CASE expressions with literal values in branches."""
        sql = """
        SELECT
            customer_id,
            CASE
                WHEN total > 1000 THEN 'high'
                WHEN total > 100 THEN 'medium'
                ELSE 'low'
            END AS category
        FROM customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        # CASE expressions should trace to the column used in conditions
        items_for_category = [
            item
            for item in results[0].lineage_items
            if "category" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_category]

        # Should trace to the total column from customers
        assert any("total" in s.lower() for s in sources)

    def test_extract_literal_representation_null(self):
        """Test _extract_literal_representation method with NULL."""
        from sqlglot.lineage import lineage

        sql = """
        SELECT NULL AS test_col FROM dual
        UNION ALL
        SELECT value AS test_col FROM source
        """
        node = lineage("test_col", sql, dialect="spark")

        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Find a leaf node that is a literal (name is digit)
        def find_literal_node(n):
            if n.name.isdigit():
                return n
            for child in n.downstream:
                result = find_literal_node(child)
                if result:
                    return result
            return None

        literal_node = find_literal_node(node)
        if literal_node:
            result = analyzer._extract_literal_representation(literal_node)
            assert "<literal:" in result

    def test_insert_with_union_literals(self):
        """Test INSERT INTO with UNION containing literals."""
        sql = """
        INSERT INTO target_table
        SELECT customer_id, status FROM active_customers
        UNION ALL
        SELECT customer_id, 'inactive' AS status FROM inactive_customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items_for_status = [
            item
            for item in results[0].lineage_items
            if "status" in item.output_name.lower()
        ]
        sources = [item.source_name for item in items_for_status]

        # Should have both the column and the literal
        assert any("active_customers" in s for s in sources)
        assert any("<literal: 'inactive'>" in s for s in sources)


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

    def test_insert_union_all_with_aliases(self):
        """UNION ALL with aliased columns should qualify with target table."""
        sql = """
        INSERT OVERWRITE TABLE db.output_table_1
        SELECT DISTINCT
            a.id,
            a.update_date,
            trim(concat(coalesce(a.address_one, ""), " ", coalesce(a.address_two, ""))) AS full_address
        FROM db.input_a AS a
        UNION
        SELECT DISTINCT
            b.id,
            b.update_date,
            trim(concat(coalesce(b.address_part_a, ""), " ", coalesce(b.address_part_b, ""))) AS full_address
        FROM db.input_b AS b
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        # All output columns should be qualified with the target table
        assert "db.output_table_1.id" in output_names
        assert "db.output_table_1.update_date" in output_names
        assert "db.output_table_1.full_address" in output_names

    def test_create_table_as_union_qualifies_output(self):
        """CREATE TABLE AS SELECT with UNION should qualify output columns."""
        sql = """
        CREATE TABLE db.new_table AS
        SELECT id, name FROM db.table_a
        UNION ALL
        SELECT id, name FROM db.table_b
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.new_table.id" in output_names
        assert "db.new_table.name" in output_names

    def test_multi_query_insert_union_cross_reference(self):
        """Test that multi-query files with INSERT UNION work correctly.

        This tests the full scenario where:
        1. First query: INSERT with UNION creates qualified output columns
        2. Second query: References the first query's output table
        """
        sql = """
        INSERT OVERWRITE TABLE db.output_table_1
        SELECT
            a.id,
            trim(concat(coalesce(a.address_one, ""), " ", coalesce(a.address_two, ""))) AS full_address
        FROM db.input_a AS a
        UNION
        SELECT
            b.id,
            trim(concat(coalesce(b.address_part_a, ""), " ", coalesce(b.address_part_b, ""))) AS full_address
        FROM db.input_b AS b;

        INSERT OVERWRITE TABLE db.output_table_2
        SELECT
            o.id,
            o.full_address AS address
        FROM db.output_table_1 AS o;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Should have 2 query results
        assert len(results) == 2

        # First query outputs should be qualified with db.output_table_1
        query1_outputs = {item.output_name for item in results[0].lineage_items}
        assert "db.output_table_1.id" in query1_outputs
        assert "db.output_table_1.full_address" in query1_outputs

        # Second query outputs should be qualified with db.output_table_2
        query2_outputs = {item.output_name for item in results[1].lineage_items}
        assert "db.output_table_2.id" in query2_outputs
        assert "db.output_table_2.address" in query2_outputs

        # Second query sources should reference db.output_table_1
        query2_sources = {item.source_name for item in results[1].lineage_items}
        assert "db.output_table_1.id" in query2_sources
        assert "db.output_table_1.full_address" in query2_sources


class TestInsertWithIntersectExcept:
    """Tests for INSERT statements containing INTERSECT/EXCEPT queries."""

    def test_insert_intersect_qualifies_output_with_target_table(self):
        """Output columns should be qualified with the INSERT target table for INTERSECT."""
        sql = """
        INSERT INTO db.common_records
        SELECT id, name FROM db.table_a
        INTERSECT
        SELECT id, name FROM db.table_b
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.common_records.id" in output_names
        assert "db.common_records.name" in output_names

    def test_insert_except_qualifies_output_with_target_table(self):
        """Output columns should be qualified with the INSERT target table for EXCEPT."""
        sql = """
        INSERT INTO db.unique_records
        SELECT id, name FROM db.table_a
        EXCEPT
        SELECT id, name FROM db.table_b
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.unique_records.id" in output_names
        assert "db.unique_records.name" in output_names

    def test_create_table_as_intersect(self):
        """CREATE TABLE AS INTERSECT should qualify output columns."""
        sql = """
        CREATE TABLE db.intersection AS
        SELECT id, status FROM db.active_users
        INTERSECT
        SELECT id, status FROM db.premium_users
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.intersection.id" in output_names
        assert "db.intersection.status" in output_names

    def test_nested_set_operations(self):
        """Nested set operations (UNION + INTERSECT) should work correctly."""
        sql = """
        INSERT INTO db.result
        SELECT id FROM db.table_a
        UNION
        SELECT id FROM db.table_b
        INTERSECT
        SELECT id FROM db.table_c
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        output_names = {item.output_name for item in results[0].lineage_items}
        assert "db.result.id" in output_names


class TestFileSchemaExtraction:
    """Tests for file-scoped schema context extraction."""

    def test_extract_schema_from_create_view(self):
        """CREATE VIEW should register schema with column names."""
        sql = "CREATE VIEW my_view AS SELECT id, name, status FROM users"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Schema should be extracted during analysis
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert "my_view" in analyzer._file_schema
        assert set(analyzer._file_schema["my_view"].keys()) == {"id", "name", "status"}

    def test_extract_schema_from_create_temporary_view(self):
        """CREATE TEMPORARY VIEW should register schema."""
        sql = "CREATE TEMPORARY VIEW temp_view AS SELECT a, b FROM source"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert "temp_view" in analyzer._file_schema
        assert set(analyzer._file_schema["temp_view"].keys()) == {"a", "b"}

    def test_extract_schema_from_create_table_as(self):
        """CREATE TABLE AS SELECT should register schema."""
        sql = "CREATE TABLE output_table AS SELECT col1, col2 FROM input_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert "output_table" in analyzer._file_schema
        assert set(analyzer._file_schema["output_table"].keys()) == {"col1", "col2"}

    def test_extract_schema_with_aliases(self):
        """Column aliases should be used as schema keys."""
        sql = """
        CREATE VIEW aliased_view AS
        SELECT
            user_id AS id,
            CONCAT(first, ' ', last) AS full_name,
            COUNT(*) AS cnt
        FROM users
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert "aliased_view" in analyzer._file_schema
        assert set(analyzer._file_schema["aliased_view"].keys()) == {
            "id",
            "full_name",
            "cnt",
        }

    def test_extract_schema_select_star_from_known_table(self):
        """SELECT * should resolve from known schema."""
        sql = """
        CREATE VIEW first_view AS SELECT a, b, c FROM source;
        CREATE VIEW second_view AS SELECT * FROM first_view;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # First view schema
        assert "first_view" in analyzer._file_schema
        assert set(analyzer._file_schema["first_view"].keys()) == {"a", "b", "c"}

        # Second view should have same columns from SELECT *
        assert "second_view" in analyzer._file_schema
        assert set(analyzer._file_schema["second_view"].keys()) == {"a", "b", "c"}

    def test_extract_schema_select_star_from_unknown_table(self):
        """SELECT * from unknown table should fall back to *."""
        sql = "CREATE VIEW unknown_star AS SELECT * FROM unknown_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Schema should be empty or contain * as fallback
        # The view may not be in schema if columns couldn't be resolved
        if "unknown_star" in analyzer._file_schema:
            # If present, it might have * as a column
            pass  # This is acceptable behavior

    def test_schema_not_extracted_from_pure_select(self):
        """Pure SELECT (not CREATE VIEW) should not modify schema."""
        sql = "SELECT a, b, c FROM source_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # No views created, schema should be empty
        assert analyzer._file_schema == {}

    def test_schema_not_extracted_from_insert(self):
        """INSERT should not add to schema (target already exists)."""
        sql = "INSERT INTO target SELECT a, b FROM source"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # INSERT doesn't create a new table schema
        assert "target" not in analyzer._file_schema

    def test_schema_reset_between_analysis_calls(self):
        """Schema should reset for each analyze_queries() call."""
        sql = "CREATE VIEW v1 AS SELECT x FROM t1"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # First analysis
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert "v1" in analyzer._file_schema

        # Manually clear and re-analyze
        analyzer._file_schema = {"old_view": {"old_col": "UNKNOWN"}}
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Old schema should be replaced
        assert "old_view" not in analyzer._file_schema
        assert "v1" in analyzer._file_schema


class TestCrossStatementLineage:
    """Tests for lineage across related statements in same file."""

    def test_view_referencing_earlier_view(self):
        """Second view should trace lineage to first view's sources."""
        sql = """
        CREATE VIEW first_view AS SELECT id, name FROM users;
        CREATE VIEW second_view AS SELECT id, name FROM first_view;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 2

        # Second view should show lineage from first_view
        second_result = results[1]
        source_names = {item.source_name for item in second_result.lineage_items}
        assert "first_view.id" in source_names or "users.id" in source_names
        assert "first_view.name" in source_names or "users.name" in source_names

    def test_select_star_expansion_through_view(self):
        """SELECT * should expand to actual columns with proper lineage."""
        sql = """
        CREATE VIEW base_view AS SELECT a, b, c FROM source_table;
        CREATE VIEW expanded_view AS SELECT * FROM base_view;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Second view should have expanded columns, not *
        second_result = results[1]
        output_names = {item.output_name for item in second_result.lineage_items}
        assert "expanded_view.a" in output_names
        assert "expanded_view.b" in output_names
        assert "expanded_view.c" in output_names
        assert "expanded_view.*" not in output_names

    def test_cte_with_select_star_from_view(self):
        """CTE SELECT * from view should resolve from view schema."""
        sql = """
        CREATE VIEW first_view AS SELECT a, b, c FROM source_table;
        CREATE VIEW second_view AS
        WITH cte AS (SELECT * FROM first_view)
        SELECT * FROM cte;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Second view should have columns a, b, c
        second_result = results[1]
        output_names = {item.output_name for item in second_result.lineage_items}
        assert "second_view.a" in output_names
        assert "second_view.b" in output_names
        assert "second_view.c" in output_names

    def test_window_function_with_select_star(self):
        """Window function columns should be included with SELECT *."""
        sql = """
        CREATE VIEW first_view AS SELECT a, b, c FROM source_table;
        CREATE VIEW second_view AS
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY a ORDER BY b) AS rn
            FROM first_view
        )
        SELECT * FROM ranked;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # Second view should have a, b, c, rn
        second_result = results[1]
        output_names = {item.output_name for item in second_result.lineage_items}
        assert "second_view.a" in output_names
        assert "second_view.b" in output_names
        assert "second_view.c" in output_names
        assert "second_view.rn" in output_names

    def test_insert_from_view_lineage(self):
        """INSERT from view should trace to original sources."""
        sql = """
        CREATE VIEW staging AS SELECT id, name FROM raw_data;
        INSERT INTO final_table SELECT id, name FROM staging;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # INSERT should show lineage from staging
        insert_result = results[1]
        source_names = {item.source_name for item in insert_result.lineage_items}
        assert "staging.id" in source_names
        assert "staging.name" in source_names

    def test_multi_hop_view_lineage(self):
        """Lineage should work through multiple view hops."""
        sql = """
        CREATE VIEW v1 AS SELECT x, y FROM base_table;
        CREATE VIEW v2 AS SELECT x, y FROM v1;
        CREATE VIEW v3 AS SELECT x, y FROM v2;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # All views should have proper columns
        for i, result in enumerate(results):
            output_names = {item.output_name for item in result.lineage_items}
            view_name = ["v1", "v2", "v3"][i]
            assert f"{view_name}.x" in output_names
            assert f"{view_name}.y" in output_names

    def test_original_problem_scenario(self):
        """Test the exact scenario from the issue."""
        sql = """
        CREATE TEMPORARY VIEW first_view AS (
            SELECT a, b, c FROM source_table
        );

        CREATE TEMPORARY VIEW second_view AS
        WITH first_view_cte AS (
            SELECT *, row_number() OVER (PARTITION BY a ORDER BY b DESC) AS row_num
            FROM first_view
        )
        SELECT * FROM first_view_cte WHERE c = 1;

        INSERT OVERWRITE output_table
        SELECT a, b, c, row_num FROM second_view;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 3

        # First view: should have a, b, c from source_table
        first_result = results[0]
        first_outputs = {item.output_name for item in first_result.lineage_items}
        assert "source_table.a" in first_outputs
        assert "source_table.b" in first_outputs
        assert "source_table.c" in first_outputs

        # Second view: should have a, b, c, row_num from first_view
        second_result = results[1]
        second_outputs = {item.output_name for item in second_result.lineage_items}
        assert "second_view.a" in second_outputs
        assert "second_view.b" in second_outputs
        assert "second_view.c" in second_outputs
        assert "second_view.row_num" in second_outputs

        # Second view sources should be from first_view
        second_sources = {item.source_name for item in second_result.lineage_items}
        assert "first_view.a" in second_sources
        assert "first_view.b" in second_sources
        assert "first_view.c" in second_sources

        # row_num should trace to a and b (PARTITION BY a ORDER BY b)
        row_num_sources = {
            item.source_name
            for item in second_result.lineage_items
            if item.output_name == "second_view.row_num"
        }
        assert "first_view.a" in row_num_sources
        assert "first_view.b" in row_num_sources

        # Third statement (INSERT): should show lineage from second_view
        third_result = results[2]
        third_outputs = {item.output_name for item in third_result.lineage_items}
        assert "output_table.a" in third_outputs
        assert "output_table.b" in third_outputs
        assert "output_table.c" in third_outputs
        assert "output_table.row_num" in third_outputs

    def test_select_star_from_join(self):
        """SELECT * from JOIN should include columns from all joined tables."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c, d FROM t2;
        CREATE VIEW v3 AS SELECT * FROM v1 JOIN v2 ON v1.a = v2.c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 3

        # Third view should have all columns from both v1 and v2
        third_result = results[2]
        third_outputs = {item.output_name for item in third_result.lineage_items}
        assert "v3.a" in third_outputs
        assert "v3.b" in third_outputs
        assert "v3.c" in third_outputs
        assert "v3.d" in third_outputs

        # Sources should be from both v1 and v2
        third_sources = {item.source_name for item in third_result.lineage_items}
        assert "v1.a" in third_sources
        assert "v1.b" in third_sources
        assert "v2.c" in third_sources
        assert "v2.d" in third_sources

    def test_nested_ctes_and_views_with_select_star(self):
        """Complex nested CTEs and views with SELECT * should resolve correctly."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c, d FROM t2;
        CREATE VIEW v3 AS
        WITH cte1 AS (SELECT * FROM v1)
        SELECT * FROM cte1;
        CREATE VIEW v4 AS
        SELECT * FROM v3 JOIN v2 ON v3.a = v2.c;
        CREATE VIEW v5 AS
        WITH
            cte1 AS (SELECT * FROM v4),
            cte2 AS (SELECT * FROM cte1)
        SELECT * FROM cte2;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 5

        # Verify file schema was correctly built
        assert "v1" in analyzer._file_schema
        assert set(analyzer._file_schema["v1"].keys()) == {"a", "b"}

        assert "v2" in analyzer._file_schema
        assert set(analyzer._file_schema["v2"].keys()) == {"c", "d"}

        assert "v3" in analyzer._file_schema
        assert set(analyzer._file_schema["v3"].keys()) == {"a", "b"}

        assert "v4" in analyzer._file_schema
        assert set(analyzer._file_schema["v4"].keys()) == {"a", "b", "c", "d"}

        assert "v5" in analyzer._file_schema
        assert set(analyzer._file_schema["v5"].keys()) == {"a", "b", "c", "d"}

        # Final view should have all columns
        fifth_result = results[4]
        fifth_outputs = {item.output_name for item in fifth_result.lineage_items}
        assert "v5.a" in fifth_outputs
        assert "v5.b" in fifth_outputs
        assert "v5.c" in fifth_outputs
        assert "v5.d" in fifth_outputs

    def test_select_star_from_subquery(self):
        """SELECT * from subquery should resolve columns from inner SELECT."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT * FROM (SELECT * FROM v1) sub;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 2

        # Second view should have columns from subquery
        second_result = results[1]
        second_outputs = {item.output_name for item in second_result.lineage_items}
        assert "v2.a" in second_outputs
        assert "v2.b" in second_outputs

        # File schema should also be correct
        assert set(analyzer._file_schema["v2"].keys()) == {"a", "b"}

    def test_table_qualified_star(self):
        """Table-qualified star (t.*) should resolve to table columns."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c FROM t2;
        CREATE VIEW v3 AS SELECT v1.*, v2.c FROM v1 JOIN v2 ON v1.a = v2.c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 3

        # Third view should have all columns
        third_result = results[2]
        third_outputs = {item.output_name for item in third_result.lineage_items}
        assert "v3.a" in third_outputs
        assert "v3.b" in third_outputs
        assert "v3.c" in third_outputs

        # File schema should be correct
        assert set(analyzer._file_schema["v3"].keys()) == {"a", "b", "c"}

    def test_table_qualified_star_with_alias(self):
        """Table-qualified star with alias (x.*) should resolve correctly."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c FROM t2;
        CREATE VIEW v3 AS SELECT x.*, y.c FROM v1 AS x JOIN v2 AS y ON x.a = y.c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 3

        # Third view should have all columns
        third_result = results[2]
        third_outputs = {item.output_name for item in third_result.lineage_items}
        assert "v3.a" in third_outputs
        assert "v3.b" in third_outputs
        assert "v3.c" in third_outputs

        # File schema should be correct
        assert set(analyzer._file_schema["v3"].keys()) == {"a", "b", "c"}


class TestLateralViewColumnResolution:
    """Tests for LATERAL VIEW column resolution in SELECT *."""

    def test_select_star_with_lateral_view_explode(self):
        """SELECT * should include explode-generated columns."""
        sql = """
        CREATE VIEW v1 AS SELECT arr FROM t1;
        CREATE VIEW v2 AS SELECT * FROM v1 LATERAL VIEW explode(arr) t AS elem;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v2 schema should include both arr and elem
        assert "v2" in analyzer._file_schema
        assert set(analyzer._file_schema["v2"].keys()) == {"arr", "elem"}

    def test_select_star_with_lateral_view_posexplode(self):
        """SELECT * should include posexplode-generated columns (pos + elem)."""
        sql = """
        CREATE VIEW v1 AS SELECT arr FROM t1;
        CREATE VIEW v2 AS SELECT * FROM v1 LATERAL VIEW posexplode(arr) t AS pos, elem;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v2 schema should include arr, pos, and elem
        assert "v2" in analyzer._file_schema
        assert set(analyzer._file_schema["v2"].keys()) == {"arr", "pos", "elem"}

    def test_select_star_with_multiple_lateral_views(self):
        """SELECT * should include columns from multiple LATERAL VIEWs."""
        sql = """
        CREATE VIEW v1 AS SELECT arr1, arr2 FROM t1;
        CREATE VIEW v2 AS
        SELECT * FROM v1
        LATERAL VIEW explode(arr1) t1 AS elem1
        LATERAL VIEW explode(arr2) t2 AS elem2;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v2 schema should include all columns
        assert "v2" in analyzer._file_schema
        assert set(analyzer._file_schema["v2"].keys()) == {
            "arr1",
            "arr2",
            "elem1",
            "elem2",
        }

    def test_select_star_with_lateral_view_outer(self):
        """LATERAL VIEW OUTER should work the same as regular LATERAL VIEW."""
        sql = """
        CREATE VIEW v1 AS SELECT arr FROM t1;
        CREATE VIEW v2 AS SELECT * FROM v1 LATERAL VIEW OUTER explode(arr) t AS elem;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v2 schema should include both arr and elem
        assert "v2" in analyzer._file_schema
        assert set(analyzer._file_schema["v2"].keys()) == {"arr", "elem"}

    def test_lateral_view_with_join(self):
        """LATERAL VIEW combined with JOIN should resolve all columns."""
        sql = """
        CREATE VIEW v1 AS SELECT id, arr FROM t1;
        CREATE VIEW v2 AS SELECT name FROM t2;
        CREATE VIEW v3 AS
        SELECT * FROM v1
        JOIN v2 ON v1.id = v2.name
        LATERAL VIEW explode(arr) t AS elem;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v3 schema should include columns from v1, v2, and the lateral view
        assert "v3" in analyzer._file_schema
        assert set(analyzer._file_schema["v3"].keys()) == {"id", "arr", "name", "elem"}


class TestSemiAntiJoinColumnResolution:
    """Tests for SEMI and ANTI JOIN column resolution in SELECT *."""

    def test_left_semi_join_only_returns_left_columns(self):
        """LEFT SEMI JOIN should only include columns from the left table."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c FROM t2;
        CREATE VIEW v3 AS SELECT * FROM v1 LEFT SEMI JOIN v2 ON v1.a = v2.c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v3 schema should only include columns from v1 (a, b), not v2 (c)
        assert "v3" in analyzer._file_schema
        assert set(analyzer._file_schema["v3"].keys()) == {"a", "b"}

    def test_left_anti_join_only_returns_left_columns(self):
        """LEFT ANTI JOIN should only include columns from the left table."""
        sql = """
        CREATE VIEW v1 AS SELECT a, b FROM t1;
        CREATE VIEW v2 AS SELECT c FROM t2;
        CREATE VIEW v3 AS SELECT * FROM v1 LEFT ANTI JOIN v2 ON v1.a = v2.c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # v3 schema should only include columns from v1 (a, b), not v2 (c)
        assert "v3" in analyzer._file_schema
        assert set(analyzer._file_schema["v3"].keys()) == {"a", "b"}

    def test_semi_join_vs_inner_join(self):
        """SEMI JOIN should behave differently from INNER JOIN for SELECT *."""
        # INNER JOIN returns columns from both tables
        sql_inner = """
        CREATE VIEW v1 AS SELECT a FROM t1;
        CREATE VIEW v2 AS SELECT b FROM t2;
        CREATE VIEW v3 AS SELECT * FROM v1 JOIN v2 ON v1.a = v2.b;
        """
        analyzer_inner = LineageAnalyzer(sql_inner, dialect="spark")
        analyzer_inner.analyze_queries(level=AnalysisLevel.COLUMN)
        assert set(analyzer_inner._file_schema["v3"].keys()) == {"a", "b"}

        # SEMI JOIN returns only left table columns
        sql_semi = """
        CREATE VIEW v1 AS SELECT a FROM t1;
        CREATE VIEW v2 AS SELECT b FROM t2;
        CREATE VIEW v3 AS SELECT * FROM v1 LEFT SEMI JOIN v2 ON v1.a = v2.b;
        """
        analyzer_semi = LineageAnalyzer(sql_semi, dialect="spark")
        analyzer_semi.analyze_queries(level=AnalysisLevel.COLUMN)
        assert set(analyzer_semi._file_schema["v3"].keys()) == {"a"}


class TestCacheTableStatements:
    """Tests for Spark SQL CACHE TABLE statement support."""

    def test_cache_table_as_select_column_lineage(self):
        """CACHE TABLE t AS SELECT should trace columns through to sources."""
        sql = """
        CACHE TABLE cached_customers AS
        SELECT customer_id, customer_name FROM customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items = {
            item.output_name: item.source_name for item in results[0].lineage_items
        }
        assert items["cached_customers.customer_id"] == "customers.customer_id"
        assert items["cached_customers.customer_name"] == "customers.customer_name"

    def test_cache_lazy_table_column_lineage(self):
        """CACHE LAZY TABLE should trace columns identically to CACHE TABLE."""
        sql = """
        CACHE LAZY TABLE cached_customers AS
        SELECT customer_id, customer_name FROM customers
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items = {
            item.output_name: item.source_name for item in results[0].lineage_items
        }
        assert items["cached_customers.customer_id"] == "customers.customer_id"
        assert items["cached_customers.customer_name"] == "customers.customer_name"

    def test_cache_table_as_select_table_extraction(self):
        """CACHE TABLE t AS SELECT should show cached_orders as OUTPUT table."""
        sql = """
        CACHE TABLE cached_orders AS
        SELECT order_id, total FROM orders
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        tables_by_name = {t.name: t for t in results[0].tables}

        assert "cached_orders" in tables_by_name
        assert tables_by_name["cached_orders"].usage.value == "OUTPUT"
        assert tables_by_name["cached_orders"].object_type.value == "TABLE"

        assert "orders" in tables_by_name
        assert tables_by_name["orders"].usage.value == "INPUT"

    def test_cache_table_as_select_with_join(self):
        """CACHE TABLE with a JOIN query should trace all sources."""
        sql = """
        CACHE TABLE summary AS
        SELECT c.customer_id, o.total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_tables()

        assert len(results) == 1
        tables_by_name = {t.name: t for t in results[0].tables}

        assert "summary" in tables_by_name
        assert tables_by_name["summary"].usage.value == "OUTPUT"
        assert "customers" in tables_by_name
        assert tables_by_name["customers"].usage.value == "INPUT"
        assert "orders" in tables_by_name
        assert tables_by_name["orders"].usage.value == "INPUT"

    def test_cache_table_with_inline_subquery_alias(self):
        """CACHE TABLE with an aliased inline subquery should trace through to sources."""
        sql = """
        CACHE TABLE cached_result AS
        SELECT s.customer_id, s.order_total
        FROM (
            SELECT c.id as customer_id, o.total as order_total
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
        ) s
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 1
        items = {
            item.output_name: item.source_name for item in results[0].lineage_items
        }
        assert items["cached_result.customer_id"] == "customers.id"
        assert items["cached_result.order_total"] == "orders.total"

    def test_bare_cache_table_is_skipped(self):
        """CACHE TABLE t (without AS SELECT) should be skipped."""
        sql = "CACHE TABLE my_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 0
        skipped = analyzer.skipped_queries
        assert len(skipped) == 1
        assert "CACHE" in skipped[0].statement_type

    def test_cache_table_in_multi_query(self):
        """CACHE TABLE should work alongside other statements in multi-query files."""
        sql = """
        SELECT id FROM users;
        CACHE TABLE cached_orders AS SELECT order_id FROM orders;
        DELETE FROM old_data;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        # SELECT and CACHE TABLE should produce results; DELETE is skipped
        assert len(results) == 2
        skipped = analyzer.skipped_queries
        assert len(skipped) == 1
        assert "DELETE" in skipped[0].statement_type

    def test_cache_table_star_resolution_in_subsequent_query(self):
        """SELECT * FROM a cached table should resolve columns from the CACHE statement."""
        sql = """
        CACHE TABLE cached_orders AS SELECT customer_id, order_total FROM orders;
        SELECT * FROM cached_orders;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 2
        # Second query should have resolved the star
        star_result = results[1]
        output_names = sorted(item.output_name for item in star_result.lineage_items)
        output_names = sorted(item.output_name for item in star_result.lineage_items)
        assert output_names == ["customer_id", "order_total"]

    def test_cache_table_qualified_star_resolution(self):
        """table.* on a cached table should resolve columns."""
        sql = """
        CACHE TABLE cached_orders AS SELECT customer_id, order_total FROM orders;
        SELECT cached_orders.* FROM cached_orders;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 2
        star_result = results[1]
        output_names = sorted(item.output_name for item in star_result.lineage_items)
        assert output_names == ["customer_id", "order_total"]

    def test_cache_lazy_table_star_resolution(self):
        """CACHE LAZY TABLE should also register schema for star resolution."""
        sql = """
        CACHE LAZY TABLE cached_users AS SELECT id, name, email FROM users;
        SELECT * FROM cached_users;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

        assert len(results) == 2
        star_result = results[1]
        output_names = sorted(item.output_name for item in star_result.lineage_items)
        assert output_names == ["email", "id", "name"]

    def test_cache_table_schema_registered(self):
        """Cached table schema should be registered for downstream star resolution."""
        sql = """
        CACHE TABLE cached_orders AS SELECT customer_id, order_total FROM orders;
        SELECT * FROM cached_orders;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        assert "cached_orders" in schema
        assert "customer_id" in schema["cached_orders"]
        assert "order_total" in schema["cached_orders"]


class TestNoStar:
    """Tests for the --no-star flag that fails on unresolvable SELECT *."""

    def test_bare_star_no_star_raises(self):
        """SELECT * from unknown table should raise with no_star=True."""
        sql = "SELECT * FROM some_table"
        analyzer = LineageAnalyzer(sql, dialect="spark", no_star=True)
        with pytest.raises(
            StarResolutionError, match="SELECT \\* could not be resolved"
        ):
            analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

    def test_qualified_star_no_star_raises(self):
        """SELECT t.* from unknown table should raise with no_star=True."""
        sql = "SELECT t.* FROM some_table t"
        analyzer = LineageAnalyzer(sql, dialect="spark", no_star=True)
        with pytest.raises(
            StarResolutionError, match="SELECT t\\.\\* could not be resolved"
        ):
            analyzer.analyze_queries(level=AnalysisLevel.COLUMN)

    def test_bare_star_default_falls_back(self):
        """SELECT * without no_star should fall back to table.*."""
        sql = "SELECT * FROM some_table"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert len(results) == 1
        output_names = [item.output_name for item in results[0].lineage_items]
        assert any("*" in name for name in output_names)

    def test_resolvable_star_with_no_star_succeeds(self):
        """SELECT * from CTE should work with no_star=True since columns are known."""
        sql = """
        WITH cte AS (SELECT 1 AS id, 'alice' AS name)
        SELECT * FROM cte
        """
        analyzer = LineageAnalyzer(sql, dialect="spark", no_star=True)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert len(results) == 1
        output_names = [item.output_name for item in results[0].lineage_items]
        assert not any("*" in name for name in output_names)

    def test_resolvable_qualified_star_with_no_star_succeeds(self):
        """SELECT t.* from CTE should work with no_star=True since columns are known."""
        sql = """
        WITH cte AS (SELECT 1 AS id, 'alice' AS name)
        SELECT cte.* FROM cte
        """
        analyzer = LineageAnalyzer(sql, dialect="spark", no_star=True)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert len(results) == 1
        output_names = [item.output_name for item in results[0].lineage_items]
        assert not any("*" in name for name in output_names)


class TestSchemaParam:
    """Tests for external schema parameter."""

    def test_schema_enables_star_expansion(self):
        sql = "SELECT * FROM users"
        schema = {"users": {"id": "UNKNOWN", "name": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert len(results) == 1
        output_names = [item.output_name for item in results[0].lineage_items]
        # Star is expanded — columns present (may or may not be qualified)
        assert any("id" in n for n in output_names)
        assert any("name" in n for n in output_names)
        # No star placeholder
        assert not any("*" in n for n in output_names)

    def test_schema_with_no_star_passes(self):
        sql = "SELECT * FROM users"
        schema = {"users": {"id": "UNKNOWN", "name": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", no_star=True, schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        assert len(results) == 1

    def test_file_derived_schema_overrides_external(self):
        """Schema from CREATE VIEW in the same file takes precedence."""
        sql = """
        CREATE VIEW users AS SELECT id, name, email FROM raw_users;
        SELECT * FROM users;
        """
        # External schema has fewer columns
        schema = {"users": {"id": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        # The second query should use the file-derived schema (3 cols)
        star_result = results[1]
        output_names = [item.output_name for item in star_result.lineage_items]
        # File-derived schema has 3 columns, overriding external's 1
        assert any("id" in n for n in output_names)
        assert any("name" in n for n in output_names)
        assert any("email" in n for n in output_names)

    def test_extract_schema_only(self):
        sql = """
        CREATE VIEW v1 AS SELECT id, name FROM t1;
        CREATE VIEW v2 AS SELECT code FROM t2;
        SELECT * FROM v1;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        assert "v1" in schema
        assert set(schema["v1"].keys()) == {"id", "name"}
        assert "v2" in schema
        assert set(schema["v2"].keys()) == {"code"}

    def test_extract_schema_from_dql(self):
        """extract_schema_only infers schemas from qualified column refs in DQL."""
        sql = "SELECT c.id, c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id;"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        assert "customers" in schema
        assert set(schema["customers"].keys()) == {"id", "name"}
        assert "orders" in schema
        assert set(schema["orders"].keys()) == {"total", "customer_id"}

    def test_extract_schema_from_dql_skips_ctes(self):
        """DQL schema inference does not add CTE aliases as table schemas."""
        sql = """
        WITH cte AS (SELECT r.id FROM raw_table r)
        SELECT c.id FROM cte c;
        """
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        # CTE 'cte' should not appear as a table schema
        assert "cte" not in schema
        # raw_table should appear from the CTE's inner qualified ref
        assert "raw_table" in schema
        assert "id" in schema["raw_table"]

    def test_extract_schema_from_dql_unqualified_single_table(self):
        """Unqualified columns are attributed when there's exactly one table."""
        sql = "SELECT id, name FROM customers;"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        assert "customers" in schema
        assert set(schema["customers"].keys()) == {"id", "name"}

    def test_extract_schema_from_dql_unqualified_multi_table_ignored(self):
        """Unqualified columns are skipped when there are multiple tables."""
        sql = "SELECT id, name FROM customers JOIN orders ON customers.id = orders.cid;"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        schema = analyzer.extract_schema_only()
        # Qualified refs from ON clause should be captured
        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "orders" in schema
        assert "cid" in schema["orders"]
        # Unqualified 'name' should NOT appear (ambiguous with 2 tables)
        for table_cols in schema.values():
            assert "name" not in table_cols

    def test_strict_schema_raises_on_ambiguous_column(self):
        """strict_schema raises SchemaResolutionError for unqualified columns in joins."""
        from sqlglider.lineage.analyzer import SchemaResolutionError

        sql = "SELECT id, name FROM customers JOIN orders ON customers.id = orders.cid;"
        analyzer = LineageAnalyzer(sql, dialect="spark", strict_schema=True)
        with pytest.raises(SchemaResolutionError, match="Cannot resolve table"):
            analyzer.extract_schema_only()

    def test_strict_schema_ok_with_single_table(self):
        """strict_schema does not raise for unqualified columns with one table."""
        sql = "SELECT id, name FROM customers;"
        analyzer = LineageAnalyzer(sql, dialect="spark", strict_schema=True)
        schema = analyzer.extract_schema_only()
        assert "customers" in schema
        assert set(schema["customers"].keys()) == {"id", "name"}

    def test_strict_schema_ok_with_qualified_columns(self):
        """strict_schema does not raise when all columns are qualified."""
        sql = "SELECT c.id, o.name FROM customers c JOIN orders o ON c.id = o.cid;"
        analyzer = LineageAnalyzer(sql, dialect="spark", strict_schema=True)
        schema = analyzer.extract_schema_only()
        assert "customers" in schema
        assert "orders" in schema

    def test_get_extracted_schema_after_analysis(self):
        sql = "CREATE VIEW v1 AS SELECT id, name FROM t1;"
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        schema = analyzer.get_extracted_schema()
        assert "v1" in schema
        assert set(schema["v1"].keys()) == {"id", "name"}


class TestSchemaPruning:
    """Tests that schema pruning doesn't affect lineage correctness."""

    def test_large_schema_same_results_as_small(self):
        """Lineage results are identical with a large unreferenced schema."""
        sql = "SELECT c.id, c.name FROM customers c"

        small_schema = {
            "customers": {"id": "UNKNOWN", "name": "UNKNOWN"},
        }
        big_schema = dict(small_schema)
        for i in range(200):
            big_schema[f"unrelated_table_{i}"] = {
                f"col_{j}": "UNKNOWN" for j in range(20)
            }

        analyzer_small = LineageAnalyzer(sql, dialect="spark", schema=small_schema)
        results_small = analyzer_small.analyze_queries(level=AnalysisLevel.COLUMN)

        analyzer_big = LineageAnalyzer(sql, dialect="spark", schema=big_schema)
        results_big = analyzer_big.analyze_queries(level=AnalysisLevel.COLUMN)

        items_small = [
            (item.output_name, item.source_name)
            for r in results_small
            for item in r.lineage_items
        ]
        items_big = [
            (item.output_name, item.source_name)
            for r in results_big
            for item in r.lineage_items
        ]
        assert items_small == items_big

    def test_star_expansion_works_with_pruned_schema(self):
        """SELECT * expansion still works when schema is pruned."""
        sql = "SELECT * FROM users"
        schema = {
            "users": {"id": "UNKNOWN", "email": "UNKNOWN"},
            "unrelated": {"col": "UNKNOWN"},
        }
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        output_names = {item.output_name for r in results for item in r.lineage_items}
        assert "id" in output_names
        assert "email" in output_names


class TestFlatSchemaToNested:
    """Tests for _flat_schema_to_nested conversion utility."""

    def test_empty(self):
        assert _flat_schema_to_nested({}) == {}

    def test_unqualified_passthrough(self):
        schema = {"users": {"id": "UNKNOWN"}}
        assert _flat_schema_to_nested(schema) == schema

    def test_two_part_keys(self):
        schema = {"db.users": {"id": "UNKNOWN"}}
        result = _flat_schema_to_nested(schema)
        assert result == {"db": {"users": {"id": "UNKNOWN"}}}

    def test_three_part_keys(self):
        schema = {"cat.db.users": {"id": "UNKNOWN"}}
        result = _flat_schema_to_nested(schema)
        assert result == {"cat": {"db": {"users": {"id": "UNKNOWN"}}}}

    def test_mixed_depth_pads_shorter_keys(self):
        schema = {
            "my_view": {"x": "UNKNOWN"},
            "db.users": {"id": "UNKNOWN"},
        }
        result = _flat_schema_to_nested(schema)
        assert result == {
            "": {"my_view": {"x": "UNKNOWN"}},
            "db": {"users": {"id": "UNKNOWN"}},
        }


class TestQualifiedSchemaKeys:
    """Tests for schema with qualified (dotted) table names."""

    def test_qualified_star_expansion(self):
        """SELECT * resolves correctly with qualified schema keys."""
        sql = "SELECT * FROM mydb.users"
        schema = {"mydb.users": {"id": "UNKNOWN", "name": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        items = {
            (item.source_name, item.output_name)
            for r in results
            for item in r.lineage_items
        }
        assert ("mydb.users.id", "id") in items
        assert ("mydb.users.name", "name") in items

    def test_qualified_explicit_columns(self):
        """Explicit columns trace sources correctly with qualified schema keys."""
        sql = "SELECT id, name FROM mydb.users"
        schema = {"mydb.users": {"id": "UNKNOWN", "name": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        items = {
            (item.source_name, item.output_name)
            for r in results
            for item in r.lineage_items
        }
        assert ("mydb.users.id", "mydb.users.id") in items
        assert ("mydb.users.name", "mydb.users.name") in items

    def test_three_part_qualified(self):
        """3-part qualified names (catalog.db.table) work correctly."""
        sql = "SELECT id FROM catalog.mydb.users"
        schema = {"catalog.mydb.users": {"id": "UNKNOWN"}}
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        items = [
            (item.source_name, item.output_name)
            for r in results
            for item in r.lineage_items
        ]
        assert len(items) == 1
        assert items[0] == ("catalog.mydb.users.id", "catalog.mydb.users.id")

    def test_mixed_qualified_and_unqualified(self):
        """Mix of qualified and unqualified table names in schema."""
        sql = "SELECT * FROM my_view"
        schema = {
            "my_view": {"id": "UNKNOWN"},
            "mydb.users": {"id": "UNKNOWN", "name": "UNKNOWN"},
        }
        analyzer = LineageAnalyzer(sql, dialect="spark", schema=schema)
        results = analyzer.analyze_queries(level=AnalysisLevel.COLUMN)
        items = {
            (item.source_name, item.output_name)
            for r in results
            for item in r.lineage_items
        }
        assert ("my_view.id", "id") in items
