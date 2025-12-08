"""Jinja2-based SQL templater.

This module provides a Jinja2 implementation of the Templater interface,
supporting full Jinja2 template syntax including variables, conditionals,
loops, and includes.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from jinja2 import (
    BaseLoader,
    Environment,
    StrictUndefined,
    TemplateError,
    TemplateNotFound,
    UndefinedError,
)

from sqlglider.templating.base import Templater, TemplaterError


class RelativeFileSystemLoader(BaseLoader):
    """A Jinja2 loader that resolves paths relative to the source file.

    This loader allows templates to include other templates using paths
    relative to the including file's location.
    """

    def __init__(self, base_path: Optional[Path] = None):
        """Initialize the loader.

        Args:
            base_path: The base path for resolving relative includes.
                      If None, includes will not work.
        """
        self.base_path = base_path

    def get_source(self, environment, template):
        """Load a template from the file system.

        Args:
            environment: The Jinja2 environment.
            template: The template name/path to load.

        Returns:
            A tuple of (source, filename, uptodate_func).

        Raises:
            TemplateNotFound: If the template cannot be found.
        """
        if self.base_path is None:
            raise TemplateNotFound(template)

        # Resolve the template path relative to base_path
        template_path = self.base_path / template

        if not template_path.exists():
            raise TemplateNotFound(template)

        try:
            source = template_path.read_text(encoding="utf-8")
            # Return source, filename, and a function that returns whether
            # the template is still up to date
            return source, str(template_path), lambda: True
        except (OSError, IOError) as e:
            raise TemplateNotFound(template) from e


class JinjaTemplater(Templater):
    """Jinja2-based SQL templater.

    Supports the full Jinja2 template syntax:
    - Variable substitution: {{ variable }}
    - Conditionals: {% if condition %}...{% endif %}
    - Loops: {% for item in items %}...{% endfor %}
    - Includes: {% include 'other.sql' %}
    - Filters: {{ value | upper }}
    - Comments: {# comment #}

    Example:
        >>> templater = JinjaTemplater()
        >>> sql = '''
        ... SELECT
        ...     {{ column }},
        ...     {% if include_total %}SUM(amount) as total{% endif %}
        ... FROM {{ schema }}.{{ table }}
        ... {% if conditions %}WHERE {{ conditions }}{% endif %}
        ... '''
        >>> variables = {
        ...     "column": "customer_id",
        ...     "include_total": True,
        ...     "schema": "sales",
        ...     "table": "orders",
        ...     "conditions": "status = 'active'"
        ... }
        >>> print(templater.render(sql, variables))
    """

    @property
    def name(self) -> str:
        """Return the templater name."""
        return "jinja"

    def render(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        source_path: Optional[Path] = None,
    ) -> str:
        """Render a SQL template using Jinja2.

        Args:
            sql: The SQL template string with Jinja2 syntax.
            variables: Template variables to substitute. If None, an empty
                      dict is used.
            source_path: Optional path to the source file. If provided,
                        enables {% include %} directives relative to this path.

        Returns:
            The rendered SQL string.

        Raises:
            TemplaterError: If the template has syntax errors or references
                           undefined variables.
        """
        variables = variables or {}

        # Set up the Jinja2 environment
        if source_path is not None:
            # Use a loader that resolves includes relative to the source file
            base_path = source_path.parent if source_path.is_file() else source_path
            loader = RelativeFileSystemLoader(base_path)
        else:
            loader = None

        env = Environment(
            loader=loader,
            # Keep whitespace to preserve SQL formatting
            trim_blocks=False,
            lstrip_blocks=False,
            # Enable autoescape for safety (though less relevant for SQL)
            autoescape=False,
            # Undefined variables should raise an error by default
            undefined=StrictUndefined,
        )

        try:
            # Compile and render the template
            template = env.from_string(sql)
            return template.render(**variables)

        except UndefinedError as e:
            raise TemplaterError(f"Undefined variable in template: {e}") from e

        except TemplateNotFound as e:
            raise TemplaterError(f"Template include not found: {e}") from e

        except TemplateError as e:
            raise TemplaterError(f"Template error: {e}") from e

        except Exception as e:
            raise TemplaterError(f"Failed to render template: {e}") from e
