from src.constants import CACHE_TTL, DB_TABLE
from src.query import build_query
import streamlit as st
import duckdb
import pandas as pd
from typing import List, Optional, Tuple
import time

# Default columns for raw mode (will be validated)
DEFAULT_RAW_COLUMNS = [
    "MAKE",
    "MODEL",
    "VEHICLE_YEAR",
    "MOTIVE_POWER",
    "VEHICLE_TYPE",
    "BODY_TYPE",
    "TLA",
]

st.set_page_config(page_title="NZ Vehicle Register Query", layout="wide")

st.title("🚗 NZ Motor Vehicle Register Query Tool")


def get_motherduck_token() -> str:
    """Retrieve MotherDuck token from Streamlit secrets"""
    try:
        return st.secrets["motherduck"]["token"]
    except KeyError as e:
        st.error(f"❌ MotherDuck token not configured. Missing key: {str(e)}")
        st.info(
            "Go to your app settings → Secrets, and add:\n\n[motherduck]\ntoken = 'your_token_here'"
        )
        st.stop()
    except Exception as e:
        st.error(f"❌ Error accessing secrets: {str(e)}")
        st.stop()


MOTHERDUCK_TOKEN = get_motherduck_token()


@st.cache_resource
def get_motherduck_connection():
    """Create and cache MotherDuck connection"""
    try:
        con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
        return con
    except duckdb.Error as e:
        st.error(f"Failed to connect to MotherDuck: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Unexpected error connecting to MotherDuck: {str(e)}")
        return None


con = get_motherduck_connection()

if con is None:
    st.stop()

st.success("✅ Connected to MotherDuck cloud database")


# Get column names
@st.cache_data(ttl=CACHE_TTL)
def get_columns() -> List[str]:
    """Fetch and cache column names from table"""
    try:
        result = con.execute(f"DESCRIBE {DB_TABLE}").fetchall()
        return [row[0] for row in result]
    except duckdb.Error as e:
        st.error(f"DuckDB error: {str(e)}")
        return []
    except Exception as e:
        st.error(f"Unexpected error reading table: {str(e)}")
        return []


@st.cache_data(ttl=CACHE_TTL)
def get_row_count() -> Optional[int]:
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {DB_TABLE}").fetchone()
        return result[0]
    except Exception as e:
        st.warning(f"Could not retrieve row count: {str(e)}")
        return None


st.write(
    "📋 Available columns in dataset: [MVR Data Dictionary](https://docs.google.com/spreadsheets/d/153bzOAGHSAmMhO3kRpc8Phu2sF21YPtu0c2WJ9Hl6Q0/edit?gid=315789064#gid=315789064)"
)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(query: str, params: List) -> pd.DataFrame:
    """Execute query with caching."""
    try:
        # Convert params list to tuple for caching
        return con.execute(query, tuple(params)).fetchdf()
    except duckdb.Error as e:
        raise RuntimeError(f"Database error: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Query execution failed: {str(e)}")


# Load dataset info
with st.spinner("📋 Loading table schema..."):
    available_columns = get_columns()

if not available_columns:
    st.error("❌ Could not read table from MotherDuck.")
    st.info(f"Have you uploaded your data and named it {DB_TABLE}?")
    st.stop()

# Get row count
total_rows = get_row_count()
if total_rows:
    st.sidebar.success(f"✅ Total vehicles: {total_rows:,}")

st.info("💡 Queries should take 1-5 seconds")

# Sidebar for query building
st.sidebar.header("Query builder")

# Query mode selection
query_mode = st.sidebar.radio(
    "Query mode:",
    ["Grouped (summary)", "Raw (individual records)"],
    help="Grouped: Summarises data. Raw: Shows individual vehicle records.",
)

# Group by selection (only if in grouped mode)
if query_mode == "Grouped (summary)":
    default_idx = (
        available_columns.index("MOTIVE_POWER")
        if "MOTIVE_POWER" in available_columns
        else 0
    )
    group_by_cols = st.sidebar.multiselect(
        "Group by (select 1-5 columns):",
        available_columns,
        default=[available_columns[default_idx]] if available_columns else [],
        max_selections=5,
        help="Groups data to keep results manageable. Null values will be shown as '(null)'.",
    )
    # Validate at least one column selected
    if not group_by_cols:
        st.sidebar.warning("⚠️ Please select at least one column to group by")
else:
    group_by_col = None
    st.sidebar.info("💡 Raw mode shows individual vehicle records")

# Multiple filters
st.sidebar.subheader("Filters (optional)")
if query_mode == "Grouped (summary)":
    st.sidebar.caption("Filter data before grouping for faster results")
num_filters = st.sidebar.number_input(
    "Number of filters:", min_value=0, max_value=10, value=0
)

# Tuple[col, operation, value]
filters: List[Tuple[str, str, Optional[str]]] = []
for i in range(num_filters):
    col1, col2, col3 = st.sidebar.columns([2, 1, 2])
    with col1:
        filter_col = st.selectbox(
            f"Column {i+1}:",
            available_columns,
            key=f"filter_col_{i}",
            # label_visibility="collapsed",
        )
    with col2:
        filter_op = st.selectbox(
            f"Op {i+1}:",
            [
                "equals",
                "contains",
                "is one of",
                ">",
                "<",
                ">=",
                "<=",
                "is null",
                "not null",
            ],
            key=f"filter_op_{i}",
            # label_visibility="collapsed",
        )
    with col3:
        if filter_op not in ["is null", "not null"]:
            if filter_op == "is one of":
                filter_val = st.text_area(
                    f"Values {i+1}:",
                    key=f"filter_val_{i}",
                    # label_visibility="collapsed",
                    placeholder="FORD\nTOYOTA\nHONDA",
                    help="Enter one value per line or separate with commas",
                    height=5,
                )
            else:
                filter_val = st.text_input(
                    f"Value {i+1}:",
                    key=f"filter_val_{i}",
                    # label_visibility="collapsed",
                    placeholder="Value",
                )
        else:
            filter_val = None

    if filter_op in ["is null", "not null"] or filter_val:
        filters.append((filter_col, filter_op, filter_val))

# Additional options
st.sidebar.subheader("Display options")

if query_mode == "Grouped (summary)":
    count_col = st.sidebar.selectbox(
        "Count column (or * for all):",
        ["*"] + available_columns,
        help="'*' counts all rows including nulls in the group",
    )
    limit = st.sidebar.slider(
        "Maximum results to show:", 10, 10000, 100, help="Number of groups to return"
    )
else:
    # Raw mode - select which columns to show
    # Validate default columns exist
    default_cols = [c for c in DEFAULT_RAW_COLUMNS if c in available_columns]
    if not default_cols:
        default_cols = available_columns[:7]  # First 7 columns as fallback

    selected_columns = st.sidebar.multiselect(
        "Columns to display:",
        available_columns,
        default=default_cols,
        help="Select which columns to show",
    )

    # Sorting options for raw mode
    sort_col = st.sidebar.selectbox(
        "Sort by:",
        ["(no sorting)"] + available_columns,
        help="Choose a column to sort results by",
    )

    if sort_col != "(no sorting)":
        sort_order = st.sidebar.radio(
            "Sort order:", ["Ascending", "Descending"], horizontal=True
        )
    else:
        sort_order = None

    limit = st.sidebar.slider(
        "Maximum records to show:",
        10,
        5000,
        100,
        help="Number of individual records to return",
    )
    count_col = None


# Build and display query
try:
    query, params = build_query(
        query_mode=query_mode,
        group_by_cols=group_by_cols,
        count_col=count_col,
        selected_columns=(
            selected_columns if query_mode == "Raw (individual records)" else []
        ),
        sort_col=sort_col if query_mode == "Raw (individual records)" else None,
        sort_order=sort_order if query_mode == "Raw (individual records)" else None,
        filters=filters,
        limit=limit,
        available_columns=available_columns,
    )

    # Show query (with parameters displayed separately)
    with st.expander("📝 View SQL query"):
        st.code(query, language="sql")
        if params:
            st.caption("Parameters:")
            for i, param in enumerate(params, 1):
                st.code(f"${i}: {repr(param)}")

except ValueError as e:
    st.error(f"❌ Query building error: {str(e)}")
    st.stop()


# Run query button
if st.sidebar.button("🔍 Run Query", type="primary"):
    # Validate inputs before running
    if query_mode == "Raw (individual records)" and not selected_columns:
        st.error("⚠️ Please select at least one column to display")
        st.stop()

    try:
        with st.spinner("Running query on MotherDuck..."):
            start = time.time()
            df = run_query(query, params)
            elapsed = time.time() - start

        if df.empty:
            st.warning("No results found. Try adjusting your filters.")
        else:
            # Display results
            st.success(f"✅ Query completed in {elapsed:.2f} seconds!")

            if query_mode == "Grouped (summary)":
                st.subheader(f"Results: {len(df):,} groups")

                # Metrics at the top
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total vehicles matched", f"{df['count'].sum():,}")
                with col2:
                    st.metric("Unique groups", f"{len(df):,}")
                with col3:
                    st.metric("Average per group", f"{df['count'].mean():.0f}")

                # Table
                st.dataframe(df, width="stretch", height=400)

                # Chart
                st.subheader("Visualisation")
                # For multi-column grouping, create a combined label
                if len(group_by_cols) == 1:
                    chart_index = df.columns[0]
                    chart_df = df.set_index(chart_index)["count"]
                else:
                    # Combine multiple columns into a single label
                    df_chart = df.copy()
                    df_chart["_combined_label"] = df_chart[group_by_cols].apply(
                        lambda row: " | ".join(str(v) for v in row), axis=1
                    )
                    chart_df = df_chart.set_index("_combined_label")["count"]

                if len(df) <= 50:
                    st.bar_chart(chart_df, sort="-count")
                else:
                    st.info(
                        f"📊 Showing top 50 of {len(df)} groups in chart (sorted by count, descending)"
                    )
                    st.bar_chart(chart_df.head(50), sort="-count")
            else:
                # Raw mode display
                st.subheader(f"Results")

                # Show record count
                st.metric("Records returned", f"{len(df):,}")

                # Table with all columns
                st.dataframe(df, width="stretch", height=500)

                # No chart for raw data

            # Download button
            csv = df.to_csv(index=False)
            filename = (
                "grouped_query_results.csv"
                if query_mode == "Grouped (summary)"
                else "raw_query_results.csv"
            )
            st.download_button(
                label="📥 Download results as CSV",
                data=csv,
                file_name=filename,
                mime="text/csv",
            )

    except RuntimeError as e:
        st.error(f"❌ {str(e)}")
        st.info("💡 Troubleshooting tips:")
        st.markdown(
            """
        - Try a simpler query first (fewer filters, lower limit)
        - Ensure filter values don't contain special characters that could cause issues
        - Check that numeric comparisons use valid numbers (e.g. no negatives)
        """
        )
    except Exception as e:
        st.error(f"❌ Unexpected error: {str(e)}")
        st.info("Please try again or contact support if the issue persists.")


# Info section
with st.expander("ℹ️ How to use this tool"):
    st.markdown(
        """
    ### Tips for best results
    - ✅ Use filters to focus on what you need
    - ✅ In Raw mode, select only the columns you care about
    - ✅ Start with lower limits and increase if needed
    - ✅ Use Grouped mode for analysis, Raw mode for finding specific vehicles
    
    ### Query modes
    
    **Grouped (summary)**: Counts data by a chosen column
    - Example: Count vehicles by make, fuel type (`MOTIVE_POWER`), year, etc.
    - Best for: Getting totals, distributions, and overviews
    - Note: Null values are included and shown as '(null)'. This matches pandas' `value_counts(dropna=False)` behaviour.

    
    **Raw (individual records)**: Shows actual vehicle records
    - Example: List the top 100 Ford Rangers submodel names with the highest CC values
    - Best for: Finding specific vehicles, previewing what the underlying data looks like
    - Limited to 5,000 records to keep things performant.
    
    ### Quick start
    1. **Choose query mode** - Grouped for summaries, Raw for individual records
    2. **Add filters** (optional) - Narrow down to specific vehicles
    3. **Configure display** - Select what to show
    4. **Click 'Run Query'** - View results, charts, and download
    
    ### Examples (Grouped mode)
    - **Group by**: `MAKE`, **No filters** → Count all vehicles by manufacturer
    - **Group by**: `MOTIVE_POWER`, **Filter**: `BODY_TYPE equals UTILITY` → Utes by fuel type
    - **Group by**: `SUBMODEL` and `VEHICLE_YEAR`, **Filter**: `MAKE equals FORD` and `MODEL equals RANGER → Ford Rangers by submodel and year
    - **Group by**: `VEHICLE_TYPE` and `BODY_TYPE` and `MOTIVE_POWER`, **Filter**: `VEHICLE_YEAR > 2020` → Modern vehicles by type
    
    ### Examples (Raw mode)
    - **Filter**: `MAKE equals FORD` and `VEHICLE_YEAR equals 2020`, **Sort by**: `MODEL` → All 2020 Fords alphabetically
    - **Filter**: `MOTIVE_POWER equals DIESEL` and `BODY_TYPE contains UTE` → All diesel utes
    - **Filter**: `VEHICLE_YEAR > 2020` → All vehicles newer than 2020
    - **Filter**: `NUMBER_OF_SEATS >= 7` and `MOTIVE_POWER equals ELECTRIC` → BEVs with 7+ seats
    - **No filters**, **Limit**: 1000, **Sort by**: `FIRST_NZ_REGISTRATION_YEAR` descending → 1000 most recently registered vehicles
    
    ### Available columns
    See the [MVR Data Dictionary](https://docs.google.com/spreadsheets/d/10OqmyPzWYq6Eai9qsuEMZAAgL3cWrLorIpjSGbW3d2g/edit?gid=649340362#gid=649340362) for columns and their values.
    
    ### Filter operators
    - **contains**: Case-insensitive partial match (e.g., "TOY" matches "TOYOTA")
    - **equals**: Exact match (case-sensitive)
    - **>, <, >=, <=**: Numeric or alphabetical comparison
    - **is null / not null**: Check for missing/present values

    ### Performance
    - All queries run in 1-5 seconds on MotherDuck's cloud infrastructure
    - Queries have a 30-second timeout for safety
    - You can safely request up to 10,000 grouped results or 5,000 raw records
    - Data never leaves MotherDuck until query results are returned
    """
    )

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("☁️ Powered by MotherDuck")
st.sidebar.caption("Built with DuckDB + Streamlit")
st.sidebar.caption("[View on GitHub](https://github.com/rewiring-nz/nz-mvr-explorer)")
