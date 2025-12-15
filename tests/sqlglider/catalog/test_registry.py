"""Tests for catalog registry."""

from typing import Dict, List

import pytest

from sqlglider.catalog.base import Catalog, CatalogError
from sqlglider.catalog.registry import (
    clear_registry,
    get_catalog,
    list_catalogs,
    register_catalog,
)


class TestGetCatalog:
    """Tests for get_catalog function."""

    def test_unknown_catalog_raises_error(self):
        """Test that unknown catalog raises CatalogError."""
        with pytest.raises(CatalogError) as exc_info:
            get_catalog("unknown_catalog")
        assert "unknown" in str(exc_info.value).lower()
        assert "available" in str(exc_info.value).lower()

    def test_returns_new_instance_each_call(self):
        """Test that get_catalog returns a new instance each call."""

        class TestCatalog(Catalog):
            @property
            def name(self) -> str:
                return "test"

            def get_ddl(self, table_name: str) -> str:
                return ""

            def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
                return {}

        register_catalog("test_instance", TestCatalog)
        try:
            catalog1 = get_catalog("test_instance")
            catalog2 = get_catalog("test_instance")
            assert catalog1 is not catalog2
        finally:
            clear_registry()


class TestListCatalogs:
    """Tests for list_catalogs function."""

    def test_list_is_sorted(self):
        """Test that list is sorted."""
        catalogs = list_catalogs()
        assert catalogs == sorted(catalogs)

    def test_list_returns_list(self):
        """Test that list_catalogs returns a list."""
        result = list_catalogs()
        assert isinstance(result, list)


class TestRegisterCatalog:
    """Tests for register_catalog function."""

    def test_register_custom_catalog(self):
        """Test registering a custom catalog."""

        class CustomCatalog(Catalog):
            @property
            def name(self) -> str:
                return "custom"

            def get_ddl(self, table_name: str) -> str:
                return f"-- Custom DDL for {table_name}"

            def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
                return {name: self.get_ddl(name) for name in table_names}

        register_catalog("custom", CustomCatalog)
        try:
            catalog = get_catalog("custom")
            assert isinstance(catalog, CustomCatalog)
            assert "custom" in list_catalogs()
        finally:
            clear_registry()

    def test_register_invalid_class_raises_error(self):
        """Test that registering non-Catalog class raises ValueError."""

        class NotACatalog:
            pass

        with pytest.raises(ValueError):
            register_catalog("invalid", NotACatalog)  # type: ignore

    def test_register_non_class_raises_error(self):
        """Test that registering non-class raises ValueError."""
        with pytest.raises(ValueError):
            register_catalog("invalid", "not a class")  # type: ignore


class TestClearRegistry:
    """Tests for clear_registry function."""

    def test_clear_registry_removes_custom_catalogs(self):
        """Test clearing the registry removes custom catalogs."""

        class TempCatalog(Catalog):
            @property
            def name(self) -> str:
                return "temp"

            def get_ddl(self, table_name: str) -> str:
                return ""

            def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
                return {}

        register_catalog("temp", TempCatalog)
        assert "temp" in list_catalogs()

        clear_registry()

        # After clearing, custom catalogs should be gone
        catalogs = list_catalogs()
        assert "temp" not in catalogs
