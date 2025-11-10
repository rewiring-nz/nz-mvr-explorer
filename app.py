import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="NZ Vehicle Register Query", layout="wide")


# Initialize DuckDB connection
@st.cache_resource
def get_connection():
    return duckdb.connect()


con = get_connection()

st.title("🚗 NZ Motor Vehicle Register Query Tool")

# File path - adjust this to where your CSV is
CSV_PATH = "vehicle_data.csv"  # Can be local or URL


# Get column names (you'll need to update this list based on your actual CSV)
# Or we can detect them automatically
@st.cache_data
def get_columns():
    result = con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto('{CSV_PATH}', sample_size=1000)"
    ).fetchall()
    return [row[0] for row in result]


try:
    available_columns = get_columns()
except:
    # Fallback if CSV not found yet
    available_columns = ["make", "model", "body_type", "fuel_type", "year", "colour"]
    st.warning(
        "CSV file not found. Using example columns. Please upload your vehicle_data.csv file."
    )

# Sidebar for query building
st.sidebar.header("Query builder")

# Group by selection
group_by_col = st.sidebar.selectbox("Group by:", available_columns)

# Multiple filters
st.sidebar.subheader("Filters")
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
        filter_val = st.text_input(f"Value {i+1}:", key=f"filter_val_{i}")
    if filter_val:
        filters.append((filter_col, filter_val))

# Additional options
count_col = st.sidebar.selectbox(
    "Count column (or * for all):", ["*"] + available_columns
)
limit = st.sidebar.slider("Limit results:", 10, 1000, 100)

# Build query
where_clause = ""
if filters:
    conditions = [f"{col} ILIKE '%{val}%'" for col, val in filters]
    where_clause = "WHERE " + " AND ".join(conditions)

if count_col == "*":
    count_expr = "COUNT(*)"
else:
    count_expr = f"COUNT({count_col})"

query = f"""
    SELECT {group_by_col}, {count_expr} as count 
    FROM read_csv_auto('{CSV_PATH}')
    {where_clause}
    GROUP BY {group_by_col}
    ORDER BY count DESC
    LIMIT {limit}
"""

# Show query
with st.expander("📝 View SQL Query"):
    st.code(query, language="sql")

# Run query button
if st.sidebar.button("🔍 Run Query", type="primary"):
    try:
        with st.spinner("Running query..."):
            df = con.execute(query).fetchdf()

        if df.empty:
            st.warning("No results found. Try adjusting your filters.")
        else:
            # Display results
            st.subheader(f"Results: {len(df)} rows")

            # Metrics at the top
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Count", f"{df['count'].sum():,}")
            with col2:
                st.metric("Unique Values", len(df))
            with col3:
                st.metric("Average per Group", f"{df['count'].mean():.0f}")

            # Table
            st.dataframe(df, use_container_width=True, height=400)

            # Chart
            st.subheader("Visualisation")
            if len(df) <= 50:  # Only show chart if reasonable number of bars
                st.bar_chart(df.set_index(group_by_col)["count"])
            else:
                st.info("Too many groups to chart effectively. Showing top 50.")
                st.bar_chart(df.head(50).set_index(group_by_col)["count"])

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name="query_results.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(f"Error running query: {str(e)}")
        st.info("Make sure your CSV file is uploaded and the column names are correct.")

# Info section
with st.expander("ℹ️ How to use this tool"):
    st.markdown(
        """
    1. **Select a column to group by** - This is like a pivot table
    2. **Add filters** (optional) - Narrow down your results
    3. **Click 'Run Query'** - View results, charts, and download
    
    **Examples:**
    - Group by: `make`, Filter: `fuel_type = 'PETROL'` → See petrol vehicles by manufacturer
    - Group by: `body_type`, no filters → Count of each body type
    - Group by: `year`, Filter: `make = 'TOYOTA'` → Toyota registrations by year
    """
    )
