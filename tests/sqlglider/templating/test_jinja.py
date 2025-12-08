"""Tests for the Jinja2 templater."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from sqlglider.templating.base import Templater, TemplaterError
from sqlglider.templating.jinja import JinjaTemplater


class TestJinjaTemplater:
    """Tests for JinjaTemplater."""

    def test_name_is_jinja(self):
        """Test that JinjaTemplater name is 'jinja'."""
        templater = JinjaTemplater()
        assert templater.name == "jinja"

    def test_is_templater_subclass(self):
        """Test that JinjaTemplater is a Templater subclass."""
        assert issubclass(JinjaTemplater, Templater)


class TestJinjaVariableSubstitution:
    """Tests for Jinja2 variable substitution."""

    def test_simple_variable(self):
        """Test simple variable substitution."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table }}"
        result = templater.render(sql, variables={"table": "users"})
        assert result == "SELECT * FROM users"

    def test_multiple_variables(self):
        """Test multiple variable substitution."""
        templater = JinjaTemplater()
        sql = "SELECT {{ column }} FROM {{ schema }}.{{ table }}"
        result = templater.render(
            sql,
            variables={"column": "id", "schema": "public", "table": "users"},
        )
        assert result == "SELECT id FROM public.users"

    def test_variable_in_where_clause(self):
        """Test variable in WHERE clause."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM orders WHERE status = '{{ status }}'"
        result = templater.render(sql, variables={"status": "active"})
        assert result == "SELECT * FROM orders WHERE status = 'active'"

    def test_numeric_variable(self):
        """Test numeric variable substitution."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM orders LIMIT {{ limit }}"
        result = templater.render(sql, variables={"limit": 100})
        assert result == "SELECT * FROM orders LIMIT 100"

    def test_no_variables_needed(self):
        """Test SQL with no template variables."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM users"
        result = templater.render(sql, variables={})
        assert result == "SELECT * FROM users"

    def test_empty_variables_dict(self):
        """Test with empty variables dictionary."""
        templater = JinjaTemplater()
        sql = "SELECT 1"
        result = templater.render(sql, variables={})
        assert result == "SELECT 1"

    def test_none_variables(self):
        """Test with None variables."""
        templater = JinjaTemplater()
        sql = "SELECT 1"
        result = templater.render(sql, variables=None)
        assert result == "SELECT 1"


class TestJinjaConditionals:
    """Tests for Jinja2 conditionals."""

    def test_if_true(self):
        """Test if statement with true condition."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM orders{% if include_total %}, SUM(total){% endif %}"
        result = templater.render(sql, variables={"include_total": True})
        assert result == "SELECT * FROM orders, SUM(total)"

    def test_if_false(self):
        """Test if statement with false condition."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM orders{% if include_total %}, SUM(total){% endif %}"
        result = templater.render(sql, variables={"include_total": False})
        assert result == "SELECT * FROM orders"

    def test_if_else(self):
        """Test if-else statement."""
        templater = JinjaTemplater()
        sql = "SELECT {% if use_star %}*{% else %}id, name{% endif %} FROM users"
        result_star = templater.render(sql, variables={"use_star": True})
        result_cols = templater.render(sql, variables={"use_star": False})
        assert result_star == "SELECT * FROM users"
        assert result_cols == "SELECT id, name FROM users"

    def test_if_elif_else(self):
        """Test if-elif-else statement."""
        templater = JinjaTemplater()
        sql = """SELECT {% if agg == 'sum' %}SUM(value){% elif agg == 'avg' %}AVG(value){% else %}value{% endif %} FROM data"""
        assert "SUM(value)" in templater.render(sql, variables={"agg": "sum"})
        assert "AVG(value)" in templater.render(sql, variables={"agg": "avg"})
        assert "value FROM" in templater.render(sql, variables={"agg": "none"})


class TestJinjaLoops:
    """Tests for Jinja2 loops."""

    def test_for_loop(self):
        """Test for loop."""
        templater = JinjaTemplater()
        sql = "SELECT {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %} FROM users"
        result = templater.render(sql, variables={"columns": ["id", "name", "email"]})
        assert result == "SELECT id, name, email FROM users"

    def test_for_loop_empty_list(self):
        """Test for loop with empty list."""
        templater = JinjaTemplater()
        sql = "SELECT {% for col in columns %}{{ col }}, {% endfor %}* FROM users"
        result = templater.render(sql, variables={"columns": []})
        assert result == "SELECT * FROM users"


class TestJinjaFilters:
    """Tests for Jinja2 filters."""

    def test_upper_filter(self):
        """Test upper filter."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table | upper }}"
        result = templater.render(sql, variables={"table": "users"})
        assert result == "SELECT * FROM USERS"

    def test_lower_filter(self):
        """Test lower filter."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table | lower }}"
        result = templater.render(sql, variables={"table": "USERS"})
        assert result == "SELECT * FROM users"

    def test_default_filter(self):
        """Test default filter with missing variable."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table | default('users') }}"
        result = templater.render(sql, variables={})
        assert result == "SELECT * FROM users"


class TestJinjaComments:
    """Tests for Jinja2 comments."""

    def test_comment_removed(self):
        """Test that Jinja comments are removed."""
        templater = JinjaTemplater()
        sql = "SELECT * {# this is a comment #}FROM users"
        result = templater.render(sql)
        assert result == "SELECT * FROM users"


class TestJinjaIncludes:
    """Tests for Jinja2 includes."""

    def test_include_file(self):
        """Test including another template file."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create include file
            include_file = tmppath / "common.sql"
            include_file.write_text("customer_id, customer_name")

            # Create main template file (must exist for is_file() check)
            main_file = tmppath / "query.sql"
            main_sql = "SELECT {% include 'common.sql' %} FROM customers"
            main_file.write_text(main_sql)

            templater = JinjaTemplater()
            result = templater.render(main_sql, source_path=main_file)
            assert result == "SELECT customer_id, customer_name FROM customers"

    def test_include_missing_file_raises_error(self):
        """Test that including a missing file raises TemplaterError."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            # Create main file so is_file() returns True
            main_file = tmppath / "query.sql"
            main_file.write_text("placeholder")

            templater = JinjaTemplater()
            sql = "SELECT {% include 'missing.sql' %} FROM users"

            with pytest.raises(TemplaterError) as exc_info:
                templater.render(sql, source_path=main_file)
            assert "not found" in str(exc_info.value).lower()

    def test_include_without_source_path_raises_error(self):
        """Test that include without source_path raises TemplaterError."""
        templater = JinjaTemplater()
        sql = "SELECT {% include 'common.sql' %} FROM users"

        with pytest.raises(TemplaterError):
            templater.render(sql)
        # Error message can vary - just ensure it raises TemplaterError


class TestJinjaErrorHandling:
    """Tests for Jinja2 error handling."""

    def test_undefined_variable_raises_error(self):
        """Test that undefined variable raises TemplaterError."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table }}"

        with pytest.raises(TemplaterError) as exc_info:
            templater.render(sql, variables={})
        assert "undefined" in str(exc_info.value).lower()

    def test_syntax_error_raises_error(self):
        """Test that Jinja syntax error raises TemplaterError."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {{ table"  # Missing closing braces

        with pytest.raises(TemplaterError) as exc_info:
            templater.render(sql, variables={"table": "users"})
        assert "error" in str(exc_info.value).lower()

    def test_invalid_expression_raises_error(self):
        """Test that invalid expression raises TemplaterError."""
        templater = JinjaTemplater()
        sql = "SELECT * FROM {% invalid_tag %}"

        with pytest.raises(TemplaterError):
            templater.render(sql)


class TestJinjaWhitespacePreservation:
    """Tests for whitespace preservation."""

    def test_preserves_sql_formatting(self):
        """Test that SQL formatting is preserved."""
        templater = JinjaTemplater()
        sql = """SELECT
    {{ column }}
FROM
    {{ table }}
WHERE
    id = 1"""
        result = templater.render(sql, variables={"column": "name", "table": "users"})
        assert "SELECT\n    name\nFROM\n    users" in result

    def test_preserves_indentation(self):
        """Test that indentation is preserved."""
        templater = JinjaTemplater()
        sql = "    SELECT * FROM {{ table }}"
        result = templater.render(sql, variables={"table": "users"})
        assert result == "    SELECT * FROM users"
