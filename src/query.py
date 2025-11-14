from typing import List, Tuple, Optional
from src.constants import DB_TABLE
from src.filter import build_filter_condition
from src.validation import validate_columns


def build_query(
    query_mode: str,
    group_by_cols: Optional[List[str]],
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
    if group_by_cols:
        columns_to_validate.extend(group_by_cols)
    if count_col and count_col != "*":
        columns_to_validate.append(count_col)
    columns_to_validate.extend(selected_columns)
    if sort_col and sort_col != "(no sorting)":
        columns_to_validate.append(sort_col)
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

        group_by_clause = ", ".join([f'"{col}"' for col in group_by_cols])
        select_clause = ", ".join([f'"{col}"' for col in group_by_cols])

        query = f"""
            WITH counts AS (
                SELECT 
                    {select_clause},
                    COUNT(*) as count 
                FROM {DB_TABLE}
                {where_clause}
                GROUP BY {group_by_clause}
            )
            SELECT * FROM counts
            ORDER BY count DESC, {group_by_clause}
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
