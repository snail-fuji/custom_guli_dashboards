import pandas as pd
from google.oauth2 import service_account
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import json

rename_config = {
    '0': 'Control',
    "1": 'Test',
    "diff": "Difference"
}

def highlight_values(val):
    cmap = plt.get_cmap('RdYlGn')  # Red to Green colormap
    norm = mcolors.Normalize(vmin=-10, vmax=10)  # Normalize between 0 and 100
    color = cmap(norm(val))
    return f'background-color: rgb({int(color[0]*255)}, {int(color[1]*255)}, {int(color[2]*255)})'

def highlight_time_values(val):
    cmap = plt.get_cmap('RdYlGn')  # Red to Green colormap
    norm = mcolors.Normalize(vmin=-500, vmax=500)  # Normalize between 0 and 100
    color = cmap(norm(-val))
    return f'background-color: rgb({int(color[0]*255)}, {int(color[1]*255)}, {int(color[2]*255)})'

def get_height(df):
    return (df.data.shape[0] + 1) * 35 + 3

if __name__ == '__main__':
    with st.form("abtest"):
        n_days = st.selectbox("Retention Day", [3, 7, 14])
        n_additional_offers = st.selectbox("Offers to compare", [5, 10, 20])
        platform = st.selectbox("Platform", ["All", "Android", "iOS"])
        country_group = st.selectbox("Country Group", ["All", "T0", "T1, T2"])
        submitted = st.form_submit_button("Extract")

    credentials = service_account.Credentials.from_service_account_info(st.secrets["google"])

    pd.read_gbq("""
    SELECT 'test'
    """, project_id='lonely-expeditions-275719', credentials=credentials)

    offers_df = pd.read_gbq(f"""
    WITH users AS (
        SELECT u.*, f.ab_group
        FROM (
            SELECT *, cast(UserId AS string) AS user_id
            FROM Analytics.Users
        ) u
        INNER JOIN Analytics.FirebaseABTests f USING (user_id)
        WHERE not IsTestUser
            AND (
                '{platform}' = 'All'
                OR Platform = '{platform}'
            )
            AND (
                '{country_group}' LIKE concat('%', Tier, '%')
                OR (
                    ('{country_group}' = 'All')
                )
            )
            AND UserRegistrationDate BETWEEN '2024-06-07' AND '2024-07-05'
            AND experiment_id IN ('firebase_exp_24', 'firebase_exp_25')
    ), inapps AS (
        SELECT Platform, Tier, ab_group, UserId, IapId, 
            IapUSDValue,
            ROW_NUMBER() OVER (
                PARTITION BY UserId
                ORDER BY EventTime
            ) AS IapNumber, DATE_DIFF(
                DATE(EventTime), 
                UserRegistrationDate, 
                DAY
            ) AS RetentionDay,
            DATE_DIFF(
                CURRENT_DATE(), 
                UserRegistrationDate, 
                DAY
            ) - 1 AS MaxRetentionDay,
            u.UserRegistrationTime, 
            EventTime
        FROM Stoppers.Iap
        INNER JOIN users u USING (UserId)
        WHERE DATE(EventTime) >= '2024-06-07'
    ), shows AS (
        SELECT io.UserId, OfferId AS IapId, MIN(EventTime) AS ShowEventTime
        FROM Analytics.IapOffers io
        INNER JOIN (
            SELECT UserId, MIN(EventTime) AS FirstPaymentTime
            FROM inapps
            GROUP BY 1
        ) fp ON
            fp.UserId = io.UserId
            AND io.EventTime <= fp.FirstPaymentTime
        WHERE EventName = 'ActivateOffer'
            AND DATE(io.EventTime) >= '2024-06-07'
        GROUP BY 1, 2
    )
    SELECT *
    FROM inapps
    LEFT JOIN shows USING (UserId, IapId)
    WHERE RetentionDay BETWEEN 0 AND 14
    """)

    # Find popular offers
    target_offers = ['al.2x2startofer', 'al.5x2startofer', 'al.10x2startofer']

    popular_first_offers = offers_df[
        (offers_df['ab_group'] == '0') &\
        (offers_df['IapNumber'] == 1) &\
        (offers_df['RetentionDay'] < n_days) &\
        (~offers_df['IapId'].isin(target_offers))
    ]['IapId'].value_counts().index[0:n_additional_offers].tolist()

    def replaced_offer_name(offer):
        if offer in target_offers:
            return offer
        if offer in popular_first_offers:
            return offer
        return 'Other'
            
    offers_df['Offer'] = offers_df['IapId'].apply(replaced_offer_name)

    # Revenue

    revenue_df = offers_df[
        (offers_df['IapNumber'] == 1) &\
        (offers_df['RetentionDay'] < n_days) &\
        (offers_df['MaxRetentionDay'] > n_days)
    ].groupby(['ab_group', 'Offer'])['IapUSDValue'].sum().sort_values(ascending=True).unstack(0)

    revenue_comp_df = (revenue_df / revenue_df.sum() * 100).round(2).sort_values("0")
    revenue_comp_df['diff'] = revenue_comp_df["1"] - revenue_comp_df["0"]

    def format_comp_df(comp_df, custom_format='{:.2f}%', highlight_func=highlight_values):
        return comp_df.rename(columns=rename_config).style.applymap(highlight_func, subset=['Difference']).format(custom_format)

    revenue_comp_df = format_comp_df(revenue_comp_df)

    st.title("Revenue per offer, % from total")
    st.dataframe(revenue_comp_df, height=get_height(revenue_comp_df), use_container_width=True)

    # Paying share

    inapps_df = offers_df[
        (offers_df['IapNumber'] == 1) &\
        (offers_df['RetentionDay'] < n_days) &\
        (offers_df['MaxRetentionDay'] > n_days)
    ].groupby(['ab_group', 'Offer']).size().sort_values(ascending=False).unstack(0)

    inapps_comp_df = (inapps_df / inapps_df.sum() * 100).round(2).loc[revenue_comp_df.index]
    inapps_comp_df['diff'] = inapps_comp_df["1"] - inapps_comp_df["0"]
    inapps_comp_df = format_comp_df(inapps_comp_df)

    st.title("Paying share, %")
    st.dataframe(inapps_comp_df, height=get_height(inapps_comp_df), use_container_width=True)

    # Payment time

    offers_df['payment_time_diff'] = (offers_df['EventTime'] - offers_df['UserRegistrationTime']).dt.total_seconds()

    payment_time_df = offers_df[
        (offers_df['IapNumber'] == 1) &\
        (offers_df['RetentionDay'] < n_days) &\
        (offers_df['MaxRetentionDay'] > n_days)
    ].groupby(['ab_group', 'Offer'])['payment_time_diff'].median().sort_values(ascending=False).unstack(0) / 3600

    time_comp_df = payment_time_df.round(1).loc[revenue_comp_df.index]
    time_comp_df['diff'] = time_comp_df["1"] - time_comp_df["0"]
    time_comp_df = format_comp_df(time_comp_df, custom_format='{:.1f} h', highlight_func=highlight_time_values)

    st.title("Payment time, hours")
    st.dataframe(time_comp_df, height=get_height(time_comp_df), use_container_width=True)

    # First show time
    offers_df['first_show_time_diff'] = (offers_df['ShowEventTime'] - offers_df['UserRegistrationTime']).dt.total_seconds()

    first_show_time_df = offers_df[
        (offers_df['IapNumber'] == 1) &\
        (offers_df['RetentionDay'] < n_days) &\
        (offers_df['MaxRetentionDay'] > n_days)
    ].groupby(['ab_group', 'Offer'])['first_show_time_diff'].median().sort_values(ascending=False).unstack(0) / 3600

    time_comp_df = first_show_time_df.round(0).loc[revenue_comp_df.index]
    time_comp_df['diff'] = time_comp_df["1"] - time_comp_df["0"]
    time_comp_df = format_comp_df(time_comp_df, custom_format='{:.0f} h', highlight_func=highlight_time_values)

    st.title("First show time, minutes")
    st.dataframe(time_comp_df, height=get_height(time_comp_df), use_container_width=True)

    # Misc info
    st.title("Offer prices")
    st.dataframe(
        offers_df.groupby('Offer')['IapUSDValue'].agg(['mean', 'median'])
    )

    # Probability of the first payment
    