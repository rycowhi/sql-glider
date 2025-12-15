"""Tests for catalog base classes."""

from typing import Dict, List

import pytest

from sqlglider.catalog.base import Catalog


class TestCatalogABC:
    """Tests for Catalog abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that Catalog cannot be instantiated directly."""
        with pytest.raises(TypeError):
            Catalog()  # type: ignore

    def test_concrete_implementation_can_be_instantiated(self):
        """Test that a concrete implementation can be instantiated."""

        class ConcreteCatalog(Catalog):
            @property
            def name(self) -> str:
                return "test"

            def get_ddl(self, table_name: str) -> str:
                return f"CREATE TABLE {table_name}"

            def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
                return {name: self.get_ddl(name) for name in table_names}

        catalog = ConcreteCatalog()
        assert catalog.name == "test"

    def test_configure_has_default_implementation(self):
        """Test that configure has a default no-op implementation."""

        class SimpleCatalog(Catalog):
            @property
            def name(self) -> str:
                return "simple"

            def get_ddl(self, table_name: str) -> str:
                return ""

            def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
                return {}

        catalog = SimpleCatalog()
        # Should not raise - default implementation is a no-op
        catalog.configure({"key": "value"})
        catalog.configure(None)
        catalog.configure()
