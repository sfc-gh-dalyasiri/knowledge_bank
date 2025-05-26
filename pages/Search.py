import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

# Reuse the Snowflake session
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

# 🔍 Streamlit UI
st.title("🔍 Search the Knowledge Bank using Cortex")
query = st.text_input("Search the knowledge base (name, topic, cert, etc):")

if query:
    try:
        # Build payload to search ONLY the summary
        payload = {
            "query": query,
            "columns": [
                "name", "summary", "office",
                "help_topic_1", "help_topic_2",
                "previous_workplace_1", "previous_workplace_2",
                "joined_year"
            ],
            "limit": 5
        }
        raw_payload = json.dumps(payload)
        escaped_payload = raw_payload.replace("'", "''")  # Escape for SQL

        sql_query = f"""
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'knowledge_search_service',
                    '{escaped_payload}'
                )
            )['results'] AS result
        """


        df = session.sql(sql_query).to_pandas()

        if not df.empty and df['RESULT'][0]:
            flat_df = pd.json_normalize(json.loads(df['RESULT'][0]))
            
            # Ensure all expected columns exist
            for col in [
                "name", "summary", "office", "help_topic_1", "help_topic_2",
                "previous_workplace_1", "previous_workplace_2", "joined_year",
                "college_certification"
            ]:
                if col not in flat_df.columns:
                    flat_df[col] = ""
            
            # Reorder and format
            formatted_df = pd.DataFrame({
                "Name": flat_df["name"],
                "Summary": flat_df["summary"],
                "Office": flat_df["office"],
                "Help Topics": flat_df["help_topic_1"].fillna('') + " / " + flat_df["help_topic_2"].fillna(''),
                "Previous Workplaces": flat_df["previous_workplace_1"].fillna('') + " / " + flat_df["previous_workplace_2"].fillna(''),
                "College or Certification": flat_df["college_certification"],
                "Joined Year": flat_df["joined_year"]
            })[
                ["Name", "Summary", "Office", "Help Topics", "Previous Workplaces", "College or Certification", "Joined Year"]
            ]
            
            # Display
            st.dataframe(formatted_df, use_container_width=True, hide_index=True)

        else:
            st.warning("No results found. Try a different search term.")

    except Exception as e:
        st.error("❌ Cortex search failed.")
        st.exception(e)