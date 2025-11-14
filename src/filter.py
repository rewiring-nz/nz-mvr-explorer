from typing import Optional, List

from src.validation import is_numeric


def build_filter_condition(
    col: str, op: str, val: Optional[str], params: List
) -> Optional[str]:
    """Build a single filter condition with parameterisation.

    Returns the condition string and appends parameters to the params list.
    """
    if op == "is null":
        return f'"{col}" IS NULL'
    elif op == "not null":
        return f'"{col}" IS NOT NULL'
    elif op == "contains":
        params.append(f"%{val}%")
        # ILIKE is case insensitive matching
        return f'CAST("{col}" AS VARCHAR) ILIKE ${len(params)}'
    elif op == "equals":
        params.append(val)
        return f'CAST("{col}" AS VARCHAR) = ${len(params)}'
    elif op == "is one of":
        # Split by newlines or commas, strip whitespace
        values = []
        for line in val.split("\n"):
            values.extend([v.strip() for v in line.split(",") if v.strip()])

        if not values:
            return None

        # Add each value as a parameter
        placeholders = []
        for v in values:
            params.append(v)
            placeholders.append(f"${len(params)}")

        placeholders_str = ", ".join(placeholders)
        return f'CAST("{col}" AS VARCHAR) IN ({placeholders_str})'
    elif op in [">", "<", ">=", "<="]:
        # Try numeric comparison first, fall back to string comparison
        if is_numeric(val):
            params.append(float(val))
            return f'TRY_CAST("{col}" AS DOUBLE) {op} ${len(params)}'
        else:
            params.append(val)
            return f'CAST("{col}" AS VARCHAR) {op} ${len(params)}'
    return None
