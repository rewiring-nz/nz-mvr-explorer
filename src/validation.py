from typing import List


def validate_columns(cols: List[str], allowed: List[str]) -> bool:
    """Validate that all columns are in the allowed list."""
    return all(col in allowed for col in cols)


def is_numeric(value: str) -> bool:
    """Check if a string can be converted to a number."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False
