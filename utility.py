# utility.py

import pandas as pd
import numpy as np
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Placeholder function to create schema & insert data (to be implemented)
def create_table_schema_from_df(df, table_name, database_name):
    """
    Creates a SQL table schema based on the Pandas DataFrame.
    This function should generate the table dynamically.
    """
    session = get_active_session()
    schema = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            sql_type = "INTEGER"
        elif "float" in str(dtype):
            sql_type = "FLOAT"
        elif "datetime" in str(dtype):
            sql_type = "TIMESTAMP"
        else:
            sql_type = "VARCHAR(255)"
        schema.append(f"{col} {sql_type}")
    
    create_table_sql = f"CREATE OR REPLACE TABLE {database_name}.STG.{table_name} ({', '.join(schema)})"
    
    # This should execute the SQL command in Snowflake
    st.write(f"Generated Schema:\n{create_table_sql}")  # Debugging: Show the generated SQL

    # Execute the SQL in your Snowflake session
    session.sql(create_table_sql).collect()

def insert_row_into_table(df, table_name, database_name):
    """
    Inserts the DataFrame rows into the specified Snowflake table,
    handling NaN values properly.
    """
    session = get_active_session()  # Ensure you have an active Snowflake session
    st.write(f"Inserting {len(df)} rows into `{database_name}.STG.{table_name}`...")

    # 🔹 Replace NaN values with None (NULL in Snowflake)
    df = df.replace({np.nan: None})

    # 🔹 Convert DataFrame rows into formatted SQL values
    values_list = []
    for row in df.itertuples(index=False, name=None):
        values = ', '.join([f"'{str(value)}'" if isinstance(value, str) else str(value) if value is not None else "NULL" for value in row])
        values_list.append(f"({values})")

    # 🔹 Construct full INSERT SQL statement
    columns = ", ".join(df.columns)
    values_sql = ",\n".join(values_list)  # Join multiple row values
    insert_sql = f"INSERT INTO {database_name}.STG.{table_name} ({columns}) VALUES\n{values_sql}"

    st.write(f"Generated Insert SQL:\n{insert_sql}")  # Debugging

    # 🔹 Execute the SQL in Snowflake using .collect()
    try:
        session.sql(insert_sql).collect()
        st.success(f"Successfully inserted {len(df)} rows into `{database_name}.STG.{table_name}`!")
        if st.button("Refresh"):
            st.session_state.clear()
            st.experimental_rerun()
    except Exception as e:
        st.error(f"Error inserting data: {e}")

def fetch_data(query_type, database_name=None, table_name=None, case=None):
    session = get_active_session()  # Start a session
    
    # Fetch table list
    if query_type == 'table_list':
        if database_name:
            query = f"""
            SELECT * 
            FROM {database_name}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'STG'
            """
        else:
            query = "SHOW TABLES"
        result = session.sql(query).to_pandas() # Collect results as a list of Row objects
        return result
    
    # Fetch table data
    elif query_type == 'table_data':
        if table_name and database_name:
            full_table_name = f"{database_name}.STG.{table_name}"
            if case == 'ALL':
                query = f"SELECT * FROM {full_table_name}"
            else:  # Default to fetching top 1000 rows if case is not 'ALL'
                query = f"SELECT TOP 1000 * FROM {full_table_name}"
            return session.sql(query).to_pandas()
        else:
            raise ValueError("Both 'table_name' and 'database_name' must be provided.")
    
    # Fetch stored procedures
    elif query_type == 'sp':
        if database_name:
            query = f"""SELECT PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,
            PROCEDURE_OWNER,CREATED,LAST_ALTERED
            FROM {database_name}.INFORMATION_SCHEMA.PROCEDURES"""
            return session.sql(query).to_pandas()
        else:
            raise ValueError("'database_name' must be provided for stored procedures.")
    
    else:
        raise ValueError("Invalid 'query_type'. Choose from 'table_list', 'table_data', or 'sp'.")

# Define a class to handle DataFrame styling
class StyledDataFrame:
    def __init__(self, df):
        self.df = df

    def apply_styles(self):
        """Apply styles to the DataFrame"""
        return self.df.style.set_table_styles(
            [
                {
                    'selector': 'thead th',
                    'props': [
                        ('background-color', '#f0f0f0'),
                        ('color', '#333'),
                        ('font-family', 'Arial, sans-serif'),
                        ('font-weight', 'bold'),
                        ('font-size', '14px')
                    ]
                },
                {
                    'selector': 'tbody td',
                    'props': [
                        ('background-color', '#ffffff'),
                        ('color', '#333'),
                        ('font-size', '14px'),
                        ('border', '1px solid #ddd')
                    ]
                },
                {
                    'selector': 'table',
                    'props': [
                        ('width', '100%'),
                        ('border-collapse', 'collapse'),
                    ]
                }
            ]
        )

    def to_html(self):
        """Convert the styled DataFrame to HTML"""
        styled_df = self.apply_styles()
        return styled_df.to_html(escape=False)  # Ensure HTML is not escaped

    def render(self):
        """Render the DataFrame with specific CSS"""
        html_table = self.to_html()
        return f'<div class="table-container">{html_table}</div>'

def load_file (db_name):
    st.write("Upload a File and Load into DataFrame")
    uploaded_file = st.file_uploader("Add File", type=["csv", "xlsx", "txt"])
    return uploaded_file
