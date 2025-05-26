import streamlit as st
import pandas as pd
import json

from snowflake.snowpark import Session
import streamlit as st

connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()



st.set_page_config(page_title="Employee Directory", layout="wide")

st.title("📘 Employee Directory Portal")
st.write("Explore expertise, connect with teammates, or add a new Snowflake legend!")

st.title("📋 Full Directory")
    
try:
    df = session.table("employee_knowledge_bank").to_pandas()

    # Ensure all expected columns exist
    for col in [
        "name", "summary", "office", "help_topic_1", "help_topic_2",
        "previous_workplace_1", "previous_workplace_2", "joined_year", "college_certification"
    ]:
        if col not in df.columns:
            df[col] = ""

    # Reorder and format to match search output
    formatted_df = pd.DataFrame({
        "Name": df.get("NAME", ""),
        "Summary": df.get("SUMMARY", ""),
        "Office": df.get("OFFICE", ""),
        "Help Topics": df.get("HELP_TOPIC_1", "").fillna('') + " / " + df.get("HELP_TOPIC_2", "").fillna(''),
        "Previous Workplaces": df.get("PREVIOUS_WORKPLACE_1", "").fillna('') + " / " + df.get("PREVIOUS_WORKPLACE_2", "").fillna(''),
        "College or Certification": df.get("COLLEGE_CERTIFICATION", ""),
        "Joined Year": df.get("JOINED_YEAR", "").fillna(0).astype(int).astype(str)
    })[
        ["Name", "Summary", "Office", "Help Topics", "Previous Workplaces", "College or Certification", "Joined Year"]
    ]

    st.dataframe(formatted_df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error("Failed to load directory.")
    st.exception(e)

