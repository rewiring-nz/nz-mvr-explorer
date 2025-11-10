import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="NZ Vehicle Register Query", layout="wide")

st.title("🚗 NZ Motor Vehicle Register Query Tool")

# Get MotherDuck token from Streamlit secrets
try:
    MOTHERDUCK_TOKEN = st.secrets["motherduck"]["token"]
except:
    st.error("❌ MotherDuck token not configured. Please add it to Streamlit secrets.")
    st.info(
        "Go to your app settings → Secrets, and add: [motherduck]\ntoken = 'your_token_here'"
    )
    st.stop()


# Initialize MotherDuck connection
@st.cache_resource
def get_motherduck_connection():
    try:
        con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
        return con
    except Exception as e:
        st.error(f"Failed to connect to MotherDuck: {str(e)}")
        return None


con = get_motherduck_connection()

if con is None:
    st.stop()

st.success("✅ Connected to MotherDuck cloud database")

# Database and table name
# TODO: in future, let the user select which one if there are multiple versions uploaded
DB_TABLE = "mvr.main.mvr_sep2025"


# Get column names
@st.cache_data(ttl=3600)
def get_columns():
    try:
        result = con.execute(f"DESCRIBE {DB_TABLE}").fetchall()
        return [row[0] for row in result]
    except Exception as e:
        st.error(f"Error reading table: {str(e)}")
        st.info(
            "Make sure you've uploaded the data to MotherDuck using the setup script."
        )
        return []


@st.cache_data(ttl=3600)
def get_row_count():
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {DB_TABLE}").fetchone()
        return result[0]
    except:
        return None


# Load dataset info
with st.spinner("📋 Loading table schema..."):
    available_columns = get_columns()

if not available_columns:
    st.error("❌ Could not read table from MotherDuck.")
    st.info("Have you uploaded your data? Run the setup script first.")
    st.stop()

# Show column list
with st.expander("📋 Available columns in dataset"):
    st.write(available_columns)

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
    group_by_col = st.sidebar.selectbox(
        "Group by:",
        available_columns,
        help="Groups data to keep results manageable",
        index=14,
    )
else:
    group_by_col = None
    st.sidebar.info("💡 Raw mode shows individual vehicle records")

# Multiple filters
st.sidebar.subheader("Filters (optional)")
st.sidebar.caption("Filter data before grouping for faster results")
num_filters = st.sidebar.number_input(
    "Number of filters:", min_value=0, max_value=5, value=0
)

filters = []
for i in range(num_filters):
    col1, col2 = st.sidebar.columns(2)
    with col1:
        filter_col = st.selectbox(
            f"Column {i+1}:", available_columns, key=f"filter_col_{i}"
        )
    with col2:
        filter_val = st.text_input(
            f"Value {i+1}:",
            key=f"filter_val_{i}",
            help="Use partial matches (case-insensitive)",
        )
    if filter_val:
        filters.append((filter_col, filter_val))

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
    conditions = [f"CAST(\"{col}\" AS VARCHAR) ILIKE '%{val}%'" for col, val in filters]
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
    query = f"""
        SELECT {cols}
        FROM {DB_TABLE}
        {where_clause}
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
                st.dataframe(df, use_container_width=True, height=400)

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
                st.dataframe(df, use_container_width=True, height=500)

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
    - **Group by**: `make`, **No filters** → Count all vehicles by manufacturer
    - **Group by**: `fuel_type`, **Filter**: `make = TOYOTA` → Toyota vehicles by fuel type
    - **Group by**: `year_of_manufacture`, **Filter**: `body_style = SEDAN` → Sedans by year
    
    ### Examples (Raw mode)
    - **Filter**: `make = FORD` and `year_of_manufacture = 2020` → All 2020 Fords
    - **Filter**: `fuel_type = DIESEL` and `body_style = UTE` → All diesel utes
    - **No filters**, **Limit**: 1000 → First 1000 vehicles in database
    
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
