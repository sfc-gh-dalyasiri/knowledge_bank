import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

# Reuse the Snowflake session
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

st.title("🧠 Manage Knowledge Entry")
    
    # Load existing names
try:
    names_df = session.table("employee_knowledge_bank").select("name").to_pandas()
    names = names_df["NAME"].dropna().tolist()
except Exception as e:
    st.error(f"Error fetching names: {e}")
    names = []

selected = st.selectbox("Select existing or add new:", ["Add new"] + names)
is_new = selected == "Add new"
name_input = st.text_input("Name", value="" if is_new else selected)

row = {}
if not is_new and name_input:
    safe_name = name_input.replace("'", "''")
    try:
        df = session.sql(f"SELECT * FROM employee_knowledge_bank WHERE name = '{safe_name}'").to_pandas()
        if not df.empty:
            row = df.iloc[0]
    except Exception as e:
        st.warning(f"Could not fetch entry: {e}")

# ---- Form Start ----
with st.form("entry_form", clear_on_submit=False):
    designation = st.text_input("Designation", value=row.get("DESIGNATION", "") or "")
    
    joined_year_raw = row.get("JOINED_YEAR")
    joined_year = st.number_input(
        "Joined Year", step=1,
        value=int(joined_year_raw) if not pd.isna(joined_year_raw) else 2024
    )

    office = st.text_input("Office", value=row.get("OFFICE", "") or "")
    workplace1 = st.text_input("Previous Workplace 1", value=row.get("PREVIOUS_WORKPLACE_1", "") or "")
    workplace2 = st.text_input("Previous Workplace 2", value=row.get("PREVIOUS_WORKPLACE_2", "") or "")
    certification = st.text_input("College or Certification", value=row.get("COLLEGE_CERTIFICATION", "") or "")
    topic1 = st.text_input("Help Topic 1", value=row.get("HELP_TOPIC_1", "") or "")
    topic2 = st.text_input("Help Topic 2", value=row.get("HELP_TOPIC_2", "") or "")

    col1, col2 = st.columns(2)
    submit = col1.form_submit_button("💾 Save")
    delete = col2.form_submit_button("🗑️ Delete")

# ---- Submit Handler ----
if submit and name_input:
    safe_name = name_input.replace("'", "''")
    try:
        session.sql(f"""
            MERGE INTO employee_knowledge_bank t
            USING (SELECT 1) s
            ON t.name = '{safe_name}'
            WHEN MATCHED THEN UPDATE SET
                designation = '{designation}',
                joined_year = {joined_year},
                office = '{office}',
                previous_workplace_1 = '{workplace1}',
                previous_workplace_2 = '{workplace2}',
                college_certification = '{certification}',
                help_topic_1 = '{topic1}',
                help_topic_2 = '{topic2}'
            WHEN NOT MATCHED THEN INSERT (
                name, designation, joined_year, office,
                previous_workplace_1, previous_workplace_2,
                college_certification, help_topic_1, help_topic_2
            ) VALUES (
                '{safe_name}', '{designation}', {joined_year}, '{office}',
                '{workplace1}', '{workplace2}', '{certification}', '{topic1}', '{topic2}'
            );
        """).collect()
        #session.sql("ALTER CORTEX SEARCH SERVICE knowledge_search_service REFRESH").collect()
        st.success("✅ Entry saved, index will refresh daily.")
    except Exception as e:
        st.error(f"Error saving: {e}")

# ---- Delete Handler ----
if delete and not is_new:
    safe_name = selected.replace("'", "''")
    try:
        session.sql(f"DELETE FROM employee_knowledge_bank WHERE name = '{safe_name}'").collect()
        session.sql("ALTER CORTEX SEARCH SERVICE knowledge_search_service REFRESH;").collect()
        st.success("🗑️ Entry deleted and index refreshed.")
    except Exception as e:
        st.error(f"Delete failed: {e}")
