"""Tests for templating base classes."""

from pathlib import Path

import pytest

from sqlglider.templating.base import NoOpTemplater, Templater, TemplaterError


class TestTemplaterError:
    """Tests for TemplaterError exception."""

    def test_templater_error_message(self):
        """Test that TemplaterError carries the message."""
        error = TemplaterError("Test error message")
        assert str(error) == "Test error message"

    def test_templater_error_is_exception(self):
        """Test that TemplaterError is an Exception."""
        assert issubclass(TemplaterError, Exception)


class TestTemplaterABC:
    """Tests for Templater abstract base class."""

    def test_cannot_instantiate_abc(self):
        """Test that Templater cannot be instantiated directly."""
        with pytest.raises(TypeError):
            Templater()

    def test_subclass_must_implement_name(self):
        """Test that subclass must implement name property."""

        class IncompleteTemplater(Templater):
            def render(self, sql, variables=None, source_path=None):
                return sql

        with pytest.raises(TypeError):
            IncompleteTemplater()

    def test_subclass_must_implement_render(self):
        """Test that subclass must implement render method."""

        class IncompleteTemplater(Templater):
            @property
            def name(self):
                return "incomplete"

        with pytest.raises(TypeError):
            IncompleteTemplater()

    def test_complete_subclass_works(self):
        """Test that a complete subclass can be instantiated."""

        class CompleteTemplater(Templater):
            @property
            def name(self):
                return "complete"

            def render(self, sql, variables=None, source_path=None):
                return sql

        templater = CompleteTemplater()
        assert templater.name == "complete"
        assert templater.render("SELECT 1") == "SELECT 1"


class TestNoOpTemplater:
    """Tests for NoOpTemplater."""

    def test_name_is_none(self):
        """Test that NoOpTemplater name is 'none'."""
        templater = NoOpTemplater()
        assert templater.name == "none"

    def test_render_returns_sql_unchanged(self):
        """Test that render returns SQL unchanged."""
        templater = NoOpTemplater()
        sql = "SELECT * FROM {{ table }}"
        result = templater.render(sql)
        assert result == sql

    def test_render_ignores_variables(self):
        """Test that render ignores variables."""
        templater = NoOpTemplater()
        sql = "SELECT * FROM {{ table }}"
        result = templater.render(sql, variables={"table": "users"})
        assert result == sql

    def test_render_ignores_source_path(self):
        """Test that render ignores source_path."""
        templater = NoOpTemplater()
        sql = "SELECT 1"
        result = templater.render(sql, source_path=Path("/some/path.sql"))
        assert result == sql

    def test_render_with_all_parameters(self):
        """Test render with all parameters provided."""
        templater = NoOpTemplater()
        sql = "SELECT {{ col }} FROM {{ schema }}.{{ table }}"
        result = templater.render(
            sql,
            variables={"col": "id", "schema": "public", "table": "users"},
            source_path=Path("/test/query.sql"),
        )
        assert result == sql

    def test_is_templater_subclass(self):
        """Test that NoOpTemplater is a Templater subclass."""
        assert issubclass(NoOpTemplater, Templater)

    def test_instance_is_templater(self):
        """Test that NoOpTemplater instance is a Templater."""
        templater = NoOpTemplater()
        assert isinstance(templater, Templater)
