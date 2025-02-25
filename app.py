# app.py

import streamlit as st
from utility import fetch_data, create_table_schema_from_df, insert_row_into_table, StyledDataFrame, load_file
import pandas as pd

# Set the page layout to wide
st.set_page_config(layout="wide")

# Load CSS styles from an external file
with open("style.css", "r") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def main():
    db_name = "PRD_DWH_RUNNING" 
    # Sidebar: Add real tabs for navigation
    
    with st.sidebar:
        st.image("img/logo.png")
        selected_tab = st.radio(
            "",
            ["Dashboards", "Load Data", "Task runs"], index=1,
        )
        
        if selected_tab == "Dashboards":  # Main Information Tab
            st.write("OPTIONS")

        elif selected_tab == "Load Data":
            st.write("OPTIONS")
            uploaded_file = load_file(db_name)

        elif selected_tab == "Task runs":
            st.write("OPTIONS")

    if selected_tab == "Load Data":
        st.title(":blue[Load excel file - Snowflake Application]")
        st.write("""Welcome to the Home page """)
        with st.container():
            # Create tabs
            tab1, tab2, tab3 = st.tabs(["🏠 Tables", "📊 Schema", "⚙️ Stage"])
            
            # Content inside each tab
            with tab1:
                with st.container():
                    st.write("List of existing table(s) in schema STG")
                    
                    # Fetching the stored procedure data
                    df = fetch_data('table_list', db_name)
                    
                    df['TABLE_NAME'] = df['TABLE_NAME'].apply(
                        lambda x: f'<a href="#{x}" target="_blank">{x}</a>'
                    )
                    # Create an instance of StyledDataFrame with the fetched data
                    styled_df_instance = StyledDataFrame(df)
                    
                    # Render and display the DataFrame with applied styles
                    st.write(styled_df_instance.render(), unsafe_allow_html=True)

                with st.container():
                    if uploaded_file is not None:
                        # Save uploaded file to session_state to prevent re-upload
                        st.session_state.uploaded_file = uploaded_file

                    
                    # If there's an uploaded file in session state, process and display it
                    if "uploaded_file" in st.session_state:
                        uploaded_file = st.session_state.uploaded_file
                        st.write("PREVIEW OF UPLOADED FILES")
                        # Check if the DataFrame is already in session_state
                        if 'df' not in st.session_state:
                            
                            try:
                                # Process the uploaded file
                                if uploaded_file.name.endswith(".csv"):
                                    st.session_state.df = pd.read_csv(uploaded_file)
                                elif uploaded_file.name.endswith(".xlsx"):
                                    st.session_state.df = pd.read_excel(uploaded_file) 
                                elif uploaded_file.name.endswith(".txt"):
                                    st.session_state.df = pd.read_csv(uploaded_file, delimiter="\t")
                                else:
                                    st.error("Unsupported file format!")
            
                                # Display the DataFrame
                                st.write("File Uploaded Successfully!")
                            except Exception as e:
                                st.error(f"Error loading file: {e}")
            
                        # Display the DataFrame if it is in session_state
                        if 'df' in st.session_state:
                            st.dataframe(st.session_state.df)
                            # Table & DB input
                            table_name = st.text_input("Enter Table Name:", value="my_table")

            
                            # Load Data Button
                            if st.button("Load Data"):
                                create_table_schema_from_df(st.session_state.df, table_name, db_name)
                                insert_row_into_table(st.session_state.df, table_name, db_name)

            with tab2:
                st.write("Analytics Data Here.")
            
            with tab3:
                st.write("Settings Page.")

if __name__ == "__main__":
    main()
