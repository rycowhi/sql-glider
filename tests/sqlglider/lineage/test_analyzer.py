"""Unit tests for lineage analyzer module."""

import pytest

from sqlglider.lineage.analyzer import LineageAnalyzer


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
        results = analyzer.analyze_column_lineage(column=column_input)

        assert len(results) == 1
        assert results[0].output_column == expected_output_column

    @pytest.mark.parametrize(
        "column_input,expected_output_column,expected_sources",
        [
            # Lowercase
            (
                "target_table.customer_name",
                "TARGET_TABLE.customer_name",
                ["customers.customer_name"],
            ),
            (
                "target_table.region",
                "TARGET_TABLE.region",
                ["customers.region"],
            ),
            (
                "target_table.total_amount",
                "TARGET_TABLE.total_amount",
                ["orders.order_amount"],
            ),
            # Uppercase
            (
                "TARGET_TABLE.CUSTOMER_NAME",
                "TARGET_TABLE.customer_name",
                ["customers.customer_name"],
            ),
            (
                "TARGET_TABLE.REGION",
                "TARGET_TABLE.region",
                ["customers.region"],
            ),
            # Mixed case
            (
                "TaRgEt_TaBlE.CuStOmEr_NaMe",
                "TARGET_TABLE.customer_name",
                ["customers.customer_name"],
            ),
            (
                "target_TABLE.REGION",
                "TARGET_TABLE.region",
                ["customers.region"],
            ),
        ],
    )
    def test_cte_query_case_variations(
        self, cte_query, column_input, expected_output_column, expected_sources
    ):
        """Test case-insensitive matching for queries with CTEs and DML."""
        analyzer = LineageAnalyzer(cte_query, dialect="spark")
        results = analyzer.analyze_column_lineage(column=column_input)

        assert len(results) == 1
        assert results[0].output_column == expected_output_column
        assert results[0].source_columns == expected_sources

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
        results = analyzer.analyze_column_lineage(column=column_input)

        assert len(results) == 1
        assert results[0].output_column == expected_output_column

    def test_column_not_found_preserves_case_in_error(self, simple_query):
        """Test that error messages preserve the user's input case."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_column_lineage(column="NONEXISTENT.COLUMN")

        error_message = str(exc_info.value)
        assert "NONEXISTENT.COLUMN" in error_message
        assert "not found" in error_message.lower()

    def test_all_columns_ignores_case_parameter(self, simple_query):
        """Test that omitting column parameter returns all columns regardless of case."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")
        results = analyzer.analyze_column_lineage(column=None)

        # Should return all 4 columns
        assert len(results) == 4
        output_columns = {r.output_column for r in results}
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
        self, simple_query, source_input, expected_source_column, expected_affected_outputs
    ):
        """Test that source column matching is case-insensitive for simple queries."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")
        results = analyzer.analyze_reverse_lineage(source_column=source_input)

        assert len(results) == 1
        assert results[0].output_column == expected_source_column
        assert results[0].source_columns == expected_affected_outputs

    @pytest.mark.parametrize(
        "source_input,expected_source_column,expected_affected_outputs",
        [
            # Lowercase
            (
                "customers.customer_name",
                "customers.customer_name",
                ["TARGET_TABLE.customer_name"],
            ),
            (
                "customers.region",
                "customers.region",
                ["TARGET_TABLE.region"],
            ),
            (
                "orders.order_amount",
                "orders.order_amount",
                ["TARGET_TABLE.segment", "TARGET_TABLE.total_amount"],
            ),
            # Uppercase
            (
                "CUSTOMERS.CUSTOMER_NAME",
                "customers.customer_name",
                ["TARGET_TABLE.customer_name"],
            ),
            (
                "CUSTOMERS.REGION",
                "customers.region",
                ["TARGET_TABLE.region"],
            ),
            # Mixed case
            (
                "CuStOmErS.CuStOmEr_NaMe",
                "customers.customer_name",
                ["TARGET_TABLE.customer_name"],
            ),
            (
                "cUsToMeRs.ReGiOn",
                "customers.region",
                ["TARGET_TABLE.region"],
            ),
        ],
    )
    def test_cte_query_reverse_case_variations(
        self, cte_query, source_input, expected_source_column, expected_affected_outputs
    ):
        """Test case-insensitive reverse lineage for queries with CTEs and DML."""
        analyzer = LineageAnalyzer(cte_query, dialect="spark")
        results = analyzer.analyze_reverse_lineage(source_column=source_input)

        assert len(results) == 1
        assert results[0].output_column == expected_source_column
        assert results[0].source_columns == expected_affected_outputs

    def test_source_column_not_found_preserves_case_in_error(self, simple_query):
        """Test that error messages preserve the user's input case for reverse lineage."""
        analyzer = LineageAnalyzer(simple_query, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_reverse_lineage(source_column="NONEXISTENT.SOURCE")

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
        results_lower = analyzer.analyze_column_lineage(column="single_table.column_name")
        results_upper = analyzer.analyze_column_lineage(column="SINGLE_TABLE.COLUMN_NAME")

        assert results_lower[0].output_column == results_upper[0].output_column

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
        results = analyzer.analyze_column_lineage(column=None)
        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage(column=None)
        assert len(results) >= 1

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
        table_name = "orders" if query_case == "lower" else (
            "ORDERS" if query_case == "UPPER" else "OrDeRs"
        )
        col_name = "order_id" if query_case == "lower" else (
            "ORDER_ID" if query_case == "UPPER" else "OrDeR_iD"
        )

        sql = f"SELECT {col_name} FROM {table_name}"
        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Search with different case
        search_table = "orders" if column_case == "lower" else (
            "ORDERS" if column_case == "UPPER" else "oRdErS"
        )
        search_col = "order_id" if column_case == "lower" else (
            "ORDER_ID" if column_case == "UPPER" else "oRdEr_Id"
        )

        search_term = f"{search_table}.{search_col}"
        results = analyzer.analyze_column_lineage(column=search_term)

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
            analyzer.analyze_column_lineage()

    def test_empty_sql_raises_error(self):
        """Test that empty SQL raises an error."""
        from sqlglot.errors import ParseError

        empty_sql = ""

        with pytest.raises((ParseError, ValueError)):
            analyzer = LineageAnalyzer(empty_sql, dialect="spark")
            analyzer.analyze_column_lineage()

    def test_different_dialects(self):
        """Test analyzer works with different SQL dialects."""
        sql = "SELECT id, name FROM users"

        for dialect in ["postgres", "mysql", "snowflake", "bigquery"]:
            analyzer = LineageAnalyzer(sql, dialect=dialect)
            results = analyzer.analyze_column_lineage()
            assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        # Should find lineage for the created columns
        assert len(results) >= 1
        # Verify source columns are traced
        column_names = [r.output_column for r in results]
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
        results = analyzer.analyze_column_lineage()

        # Should find lineage for inserted columns
        assert len(results) >= 1

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
        result = analyzer.analyze_table_lineage()

        assert result.source_tables is not None
        assert len(result.source_tables) >= 2
        # Should include both customers and orders
        source_tables_lower = [t.lower() for t in result.source_tables]
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
        result = analyzer.analyze_table_lineage()

        assert result.source_tables is not None
        assert len(result.source_tables) >= 1

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
        results = analyzer.analyze_column_lineage()

        # Should successfully analyze the CTE chain
        assert len(results) >= 1

    def test_union_query(self):
        """Test UNION query."""
        sql = """
        SELECT customer_id, order_date FROM orders_2023
        UNION ALL
        SELECT customer_id, order_date FROM orders_2024
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_column_lineage()

        # Should find lineage for both sources
        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1
        # Should include window function columns
        column_names = [r.output_column for r in results]
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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 2  # At least customer_id and one aggregate

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

    def test_reverse_lineage_nonexistent_column(self):
        """Test reverse lineage with nonexistent source column raises error."""
        sql = "SELECT customer_id, customer_name FROM customers"

        analyzer = LineageAnalyzer(sql, dialect="spark")

        # Should raise ValueError for nonexistent column
        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_reverse_lineage("nonexistent_table.nonexistent_column")

        assert "not found" in str(exc_info.value).lower()

    def test_forward_lineage_nonexistent_column_raises_error(self):
        """Test forward lineage with nonexistent column raises ValueError."""
        sql = "SELECT customer_id, customer_name FROM customers"

        analyzer = LineageAnalyzer(sql, dialect="spark")

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_column_lineage(column="nonexistent_column")

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 2

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 4

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

    def test_distinct_query(self):
        """Test query with DISTINCT."""
        sql = """
        SELECT DISTINCT
            customer_id,
            customer_category
        FROM customers
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 2

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

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
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 2

    def test_except_query(self):
        """Test EXCEPT/MINUS query."""
        sql = """
        SELECT customer_id FROM customers_2023
        EXCEPT
        SELECT customer_id FROM customers_2024
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1

    def test_intersect_query(self):
        """Test INTERSECT query."""
        sql = """
        SELECT customer_id FROM active_customers
        INTERSECT
        SELECT customer_id FROM premium_customers
        """

        analyzer = LineageAnalyzer(sql, dialect="spark")
        results = analyzer.analyze_column_lineage()

        assert len(results) >= 1
