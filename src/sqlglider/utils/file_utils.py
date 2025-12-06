"""File utility functions for SQL Glider."""

from pathlib import Path


def read_sql_file(file_path: Path) -> str:
    """
    Read a SQL file and return its contents as a string.

    Args:
        file_path: Path to the SQL file to read

    Returns:
        The contents of the SQL file as a string

    Raises:
        FileNotFoundError: If the file does not exist
        PermissionError: If the file cannot be read
        UnicodeDecodeError: If the file encoding is not UTF-8
    """
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"Path is not a file: {file_path}")

    try:
        return file_path.read_text(encoding="utf-8")
    except PermissionError as e:
        raise PermissionError(f"Cannot read file {file_path}: {e}") from e
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(
            e.encoding,
            e.object,
            e.start,
            e.end,
            f"File {file_path} is not valid UTF-8: {e.reason}",
        ) from e
