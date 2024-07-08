import pandas as pd
from google.oauth2 import service_account
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import json
from google.cloud import bigquery


def get_query_size(query):
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials)
    
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    # Start the query, passing in the extra configuration.
    query_job = client.query(
        query,
        job_config=job_config,
    )  # Make an API request.
    
    # A dry run query completes immediately.
    st.markdown("This query will process {:.2f} gbytes. 1 Tb = 5$".format(query_job.total_bytes_processed / 1024 / 1024 / 1024))

def run_bq_query(sql):
    get_query_size(sql)

    return pd.read_gbq(sql)


if __name__ == '__main__':
    with st.form("user-search"):
        n_days_before = st.selectbox("Search, days ago", [1, 2, 3, 7, 14])
        user_id = st.text_input("User ID", value='474359692429099081')
        events_filter = st.text_input("Events", value='')
        submitted = st.form_submit_button("Extract")

    credentials = service_account.Credentials.from_service_account_info(st.secrets["google"])
    events_filter_str = ", ".join([f"'{e.strip()}'" for e in events_filter.split(',')])

    pd.read_gbq("""
    SELECT 'test'
    """, project_id='lonely-expeditions-275719', credentials=credentials)

    with st.spinner():
        events_df = run_bq_query(f"""
        -- Query From Streamlit, Users
        SELECT EventTime, EventName, EventId, ParamName, StringValue, IntValue, FloatValue
        FROM Stoppers.EventsParams
        WHERE UserId = {user_id}
          AND DATE(EventTime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {n_days_before} DAY) AND CURRENT_DATE()
          AND (
            EventName IN ({events_filter_str})
            OR '' IN ({events_filter_str})
          )
        ORDER BY 1 DESC, 2, 3, 4
        """)

    st.dataframe(events_df, use_container_width=True, height=1000)