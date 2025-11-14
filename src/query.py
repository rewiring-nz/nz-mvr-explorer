from typing import List, Tuple, Optional
from app import DB_TABLE
from src.filter import build_filter_condition
from src.validation import validate_columns


def build_query(
    query_mode: str,
    group_by_col: Optional[str],
    count_col: Optional[str],
    selected_columns: List[str],
    sort_col: Optional[str],
    sort_order: Optional[str],
    filters: List[Tuple[str, str, Optional[str]]],
    limit: int,
    available_columns: List[str],
) -> Tuple[str, List]:
    """Build SQL query with parameterization to prevent SQL injection.

    Returns (query_string, parameters_list).
    """
    params = []

    # Validate all column names
    columns_to_validate = []
    if group_by_col:
        columns_to_validate.append(group_by_col)
    if count_col and count_col != "*":
        columns_to_validate.append(count_col)
    columns_to_validate.extend(selected_columns)
    if sort_col and sort_col != "(no sorting)":
        columns_to_validate.append(sort_col)

    # Add filter columns
    for col, _, _ in filters:
        columns_to_validate.append(col)

    if not validate_columns(columns_to_validate, available_columns):
        raise ValueError("Invalid column name detected")

    # Build WHERE clause
    where_clause = ""
    if filters:
        conditions = []
        for col, op, val in filters:
            condition = build_filter_condition(col, op, val, params)
            if condition:
                conditions.append(condition)

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

    # Build query based on mode
    if query_mode == "Grouped (summary)":
        # Use COALESCE to convert NULL to a readable string for grouping
        # But count all rows including nulls
        if count_col == "*":
            count_expr = "COUNT(*)"
        else:
            # Count all rows in the group, not just non-null values
            count_expr = "COUNT(*)"

        query = f"""
            SELECT 
                COALESCE(CAST("{group_by_col}" AS VARCHAR), '(null)') as "{group_by_col}",
                {count_expr} as count 
            FROM {DB_TABLE}
            {where_clause}
            GROUP BY "{group_by_col}"
            ORDER BY count DESC
            LIMIT {limit}
        """
    else:
        # Raw mode query
        if not selected_columns:
            raise ValueError("No columns selected for raw mode")

        cols = ", ".join([f'"{col}"' for col in selected_columns])

        # Build ORDER BY clause
        order_clause = ""
        if sort_col and sort_col != "(no sorting)":
            order_direction = "ASC" if sort_order == "Ascending" else "DESC"
            order_clause = f'ORDER BY "{sort_col}" {order_direction}'

        query = f"""
            SELECT {cols}
            FROM {DB_TABLE}
            {where_clause}
            {order_clause}
            LIMIT {limit}
        """

    return query, params
