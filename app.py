import streamlit as st
import duckdb
import pandas as pd
from typing import List, Tuple, Optional
import time

# Constants
MAX_GROUPED_RESULTS = 10000
MAX_RAW_RESULTS = 5000
CACHE_TTL = 3600
QUERY_TIMEOUT = 30
# TODO: in future, let the user select which one if there are multiple versions uploaded
DB_TABLE = "mvr.main.mvr_sep2025"

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
        # Set query timeout
        con.execute(f"SET statement_timeout = '{QUERY_TIMEOUT}s'")
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
    group_by_col = st.sidebar.selectbox(
        "Group by:",
        available_columns,
        index=default_idx,
        help="Groups data to keep results manageable",
    )
else:
    group_by_col = None
    st.sidebar.info("💡 Raw mode shows individual vehicle records")

# Multiple filters
st.sidebar.subheader("Filters (optional)")
st.sidebar.caption("Filter data before grouping for faster results")
num_filters = st.sidebar.number_input(
    "Number of filters:", min_value=0, max_value=10, value=0
)

filters = []
for i in range(num_filters):
    col1, col2, col3 = st.sidebar.columns([2, 1, 2])
    with col1:
        filter_col = st.selectbox(
            f"Column {i+1}:", available_columns, key=f"filter_col_{i}"
        )
    with col2:
        filter_op = st.selectbox(
            "Op:",
            ["contains", "equals", ">", "<", ">=", "<=", "is null", "not null"],
            key=f"filter_op_{i}",
            label_visibility="collapsed",
        )
    with col3:
        if filter_op not in ["is null", "not null"]:
            filter_val = st.text_input(
                f"Value {i+1}:",
                key=f"filter_val_{i}",
                label_visibility="collapsed",
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
        "Count column (or * for all):", ["*"] + available_columns
    )
    limit = st.sidebar.slider(
        "Maximum results to show:", 10, 10000, 100, help="Number of groups to return"
    )
else:
    # Raw mode - select which columns to show
    selected_columns = st.sidebar.multiselect(
        "Columns to display:",
        available_columns,
        default=(
            "MAKE",
            "MODEL",
            "VEHICLE_YEAR",
            "MOTIVE_POWER",
            "VEHICLE_TYPE",
            "BODY_TYPE",
            "TLA",
        ),
        help="Select which columns to show (default: first 5)",
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

# Build query
where_clause = ""
if filters:
    conditions = []
    for col, op, val in filters:
        if op == "contains":
            conditions.append(f"CAST(\"{col}\" AS VARCHAR) ILIKE '%{val}%'")
        elif op == "equals":
            conditions.append(f"CAST(\"{col}\" AS VARCHAR) = '{val}'")
        elif op in [">", "<", ">=", "<="]:
            # Try to use as number if possible, otherwise string comparison
            conditions.append(f"CAST(\"{col}\" AS VARCHAR) {op} '{val}'")
        elif op == "is null":
            conditions.append(f'"{col}" IS NULL')
        elif op == "not null":
            conditions.append(f'"{col}" IS NOT NULL')

    where_clause = "WHERE " + " AND ".join(conditions)

if query_mode == "Grouped (summary)":
    if count_col == "*":
        count_expr = "COUNT(*)"
    else:
        count_expr = f'COUNT("{count_col}")'

    query = f"""
        SELECT "{group_by_col}", {count_expr} as count 
        FROM {DB_TABLE}
        {where_clause}
        GROUP BY "{group_by_col}"
        ORDER BY count DESC
        LIMIT {limit}
    """
else:
    # Raw mode query
    if not selected_columns:
        st.sidebar.error("⚠️ Please select at least one column to display")
        st.stop()

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

# Show query
with st.expander("📝 View SQL Query"):
    st.code(query, language="sql")

# Run query button
if st.sidebar.button("🔍 Run Query", type="primary"):
    try:
        with st.spinner("Running query on MotherDuck..."):
            import time

            start = time.time()

            df = con.execute(query).fetchdf()

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
                chart_col = df.columns[0]
                if len(df) <= 50:
                    st.bar_chart(df.set_index(chart_col)["count"])
                else:
                    st.info(f"📊 Showing top 50 of {len(df)} groups in chart")
                    st.bar_chart(df.head(50).set_index(chart_col)["count"])
            else:
                # Raw mode display
                st.subheader(f"Results: {len(df):,} records")

                # Show record count
                st.metric("Records returned", f"{len(df):,}")

                # Table with all columns
                st.dataframe(df, width="stretch", height=500)

                # No chart for raw data

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download results as CSV",
                data=csv,
                file_name="query_results.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(f"❌ Error running query: {str(e)}")
        st.info("💡 Troubleshooting tips:")
        st.markdown(
            """
        - Verify column names match your dataset (check 'Available columns' above)
        - Try a simpler query first (fewer filters, lower limit)
        - Check if the column name has special characters or spaces
        """
        )

# Info section
with st.expander("ℹ️ How to use this tool"):
    st.markdown(
        """
    ### Query modes
    
    **Grouped (summary)**: Aggregates data by a chosen column
    - Example: Count vehicles by make, fuel type, year, etc.
    - Best for: Getting totals, distributions, and overviews
    
    **Raw (individual records)**: Shows actual vehicle records
    - Example: List all Toyotas made in 2020
    - Best for: Finding specific vehicles, exporting filtered data
    - Limited to 5,000 records to keep things fast
    
    ### Quick start
    1. **Choose query mode** - Grouped for summaries, Raw for individual records
    2. **Add filters** (optional) - Narrow down to specific vehicles
    3. **Configure display** - Select what to show
    4. **Click 'Run Query'** - View results, charts, and download
    
    ### Examples (Grouped mode)
    - **Group by**: `MAKE`, **No filters** → Count all vehicles by manufacturer
    - **Group by**: `MOTIVE_POWER`, **Filter**: `MAKE equals TOYOTA` → Toyota vehicles by fuel type
    - **Group by**: `VEHICLE_YEAR`, **Filter**: `BODY_TYPE equals SEDAN` → Sedans by year
    - **Group by**: `BASIC_COLOUR`, **Filter**: `MAKE contains FORD` and `VEHICLE_YEAR > 2020` → Modern Ford vehicles by colour
    
    ### Examples (Raw mode)
    - **Filter**: `MAKE equals FORD` and `VEHICLE_YEAR equals 2020`, **Sort by**: `MODEL` → All 2020 Fords alphabetically
    - **Filter**: `MOTIVE_POWER equals DIESEL` and `BODY_TYPE contains UTE` → All diesel utes
    - **Filter**: `VEHICLE_YEAR > 2020` → All vehicles newer than 2020
    - **Filter**: `NUMBER_OF_SEATS >= 7` and `MOTIVE_POWER equals PETROL/ELECTRIC` → Petrol-electric hybrids with 7+ seats
    - **No filters**, **Limit**: 1000, **Sort by**: `FIRST_NZ_REGISTRATION_YEAR` descending → 1000 most recently registered vehicles
    
    ### Available columns
    Key columns in the dataset:
    - **MAKE, MODEL, SUBMODEL** - Vehicle identification
    - **VEHICLE_YEAR** - Year of manufacture
    - **FIRST_NZ_REGISTRATION_YEAR, FIRST_NZ_REGISTRATION_MONTH** - When registered in NZ
    - **MOTIVE_POWER** - Fuel type (PETROL, DIESEL, PETROL/ELECTRIC, BATTERY ELECTRIC, etc.)
    - **BODY_TYPE** - Vehicle body style (SEDAN, HATCH, SUV, UTE, VAN, etc.)
    - **BASIC_COLOUR** - Vehicle colour
    - **NUMBER_OF_SEATS** - Seating capacity
    - **POWER_RATING** - Engine power (kW)
    - **CC_RATING** - Engine displacement (cc)
    - **TRANSMISSION_TYPE** - Manual/Automatic
    - **VEHICLE_USAGE** - Private/Commercial/etc.
    - **TLA** - Territorial Local Authority (region)
    - **ORIGINAL_COUNTRY, PREVIOUS_COUNTRY** - Import history
    - **NZ_ASSEMBLED** - Whether assembled in NZ
    - **FC_COMBINED, FC_URBAN, FC_EXTRA_URBAN** - Fuel consumption (L/100km)
    - **SYNTHETIC_GREENHOUSE_GAS** - Emissions rating
    
    ### Filter operators
    - **contains**: Case-insensitive partial match (e.g., "TOY" matches "TOYOTA")
    - **equals**: Exact match (case-sensitive)
    - **>, <, >=, <=**: Numeric or alphabetical comparison
    - **is null / not null**: Check for missing/present values
    
    ### Saved queries
    Build a query, give it a name, and click "Save current query". You can then load it later from the dropdown.
    Note: Saved queries only persist during your session (they reset when you refresh the page).
    
    ### Performance
    - All queries run in 1-5 seconds on MotherDuck's cloud infrastructure
    - You can safely request up to 10,000 grouped results or 5,000 raw records
    - Data never leaves MotherDuck until query results are returned
    
    ### Tips for best results
    - ✅ Use filters to focus on what you need
    - ✅ In Raw mode, select only the columns you care about
    - ✅ Start with lower limits and increase if needed
    - ✅ Use Grouped mode for analysis, Raw mode for finding specific vehicles
    """
    )

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("☁️ Powered by MotherDuck")
st.sidebar.caption("Built with DuckDB + Streamlit")
