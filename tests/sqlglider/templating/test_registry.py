"""Tests for templater registry."""

import pytest

from sqlglider.templating.base import NoOpTemplater, Templater, TemplaterError
from sqlglider.templating.jinja import JinjaTemplater
from sqlglider.templating.registry import (
    clear_registry,
    get_templater,
    list_templaters,
    register_templater,
)


class TestGetTemplater:
    """Tests for get_templater function."""

    def test_get_jinja_templater(self):
        """Test getting the Jinja templater."""
        templater = get_templater("jinja")
        assert isinstance(templater, JinjaTemplater)
        assert templater.name == "jinja"

    def test_get_none_templater(self):
        """Test getting the NoOp templater."""
        templater = get_templater("none")
        assert isinstance(templater, NoOpTemplater)
        assert templater.name == "none"

    def test_unknown_templater_raises_error(self):
        """Test that unknown templater raises TemplaterError."""
        with pytest.raises(TemplaterError) as exc_info:
            get_templater("unknown_templater")
        assert "unknown" in str(exc_info.value).lower()
        assert "available" in str(exc_info.value).lower()

    def test_returns_new_instance_each_call(self):
        """Test that get_templater returns a new instance each call."""
        templater1 = get_templater("jinja")
        templater2 = get_templater("jinja")
        assert templater1 is not templater2


class TestListTemplaters:
    """Tests for list_templaters function."""

    def test_list_includes_jinja(self):
        """Test that list includes jinja templater."""
        templaters = list_templaters()
        assert "jinja" in templaters

    def test_list_includes_none(self):
        """Test that list includes none templater."""
        templaters = list_templaters()
        assert "none" in templaters

    def test_list_is_sorted(self):
        """Test that list is sorted."""
        templaters = list_templaters()
        assert templaters == sorted(templaters)

    def test_list_returns_list(self):
        """Test that list_templaters returns a list."""
        result = list_templaters()
        assert isinstance(result, list)


class TestRegisterTemplater:
    """Tests for register_templater function."""

    def test_register_custom_templater(self):
        """Test registering a custom templater."""

        class CustomTemplater(Templater):
            @property
            def name(self):
                return "custom"

            def render(self, sql, variables=None, source_path=None):
                return f"-- Custom\n{sql}"

        register_templater("custom", CustomTemplater)
        try:
            templater = get_templater("custom")
            assert isinstance(templater, CustomTemplater)
            assert "custom" in list_templaters()
        finally:
            # Clean up
            clear_registry()

    def test_register_invalid_class_raises_error(self):
        """Test that registering non-Templater class raises ValueError."""

        class NotATemplater:
            pass

        with pytest.raises(ValueError):
            register_templater("invalid", NotATemplater)

    def test_register_non_class_raises_error(self):
        """Test that registering non-class raises ValueError."""
        with pytest.raises(ValueError):
            register_templater("invalid", "not a class")


class TestClearRegistry:
    """Tests for clear_registry function."""

    def test_clear_registry(self):
        """Test clearing the registry."""

        class TempTemplater(Templater):
            @property
            def name(self):
                return "temp"

            def render(self, sql, variables=None, source_path=None):
                return sql

        register_templater("temp", TempTemplater)
        assert "temp" in list_templaters()

        clear_registry()

        # After clearing, built-in templaters should be rediscovered
        # but custom ones should be gone
        templaters = list_templaters()
        assert "temp" not in templaters
        # Built-ins should be rediscovered via entry points
        assert "jinja" in templaters
        assert "none" in templaters
