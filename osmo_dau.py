"""
Created on Tue Aug 16 00:42:24 2022
@author: Jordi Garcia Ruspira
"""


import streamlit as st
import pandas as pd
import requests
import json
import time
import plotly.graph_objects as go
import random
import plotly.io as pio 
import plotly.express as px
import plotly.graph_objects as go
import datetime as dt 
import matplotlib.pyplot as plt
import numpy as np
from plotly.subplots import make_subplots
from PIL import Image 
import datetime 
import pyautogui


API_KEY = "3b5afbf4-3004-433c-9b04-2e867026718b"
    
SQL_QUERY = """  with daily_users_lp as (select date_trunc('day',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('day',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('day',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('day',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('day',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_transfers 
"""  
      
SQL_QUERY_2 = """  with daily_users_lp as (select date_trunc('day',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('day',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('day',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('day',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('day',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_staking 
"""  
      

SQL_QUERY_3 = """  with daily_users_lp as (select date_trunc('day',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('day',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('day',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('day',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('day',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_voter
"""  





SQL_QUERY_4 = """  with daily_users_lp as (select date_trunc('week',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('week',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('week',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('week',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('day',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_transfers 
"""  
      
SQL_QUERY_5 = """  with daily_users_lp as (select date_trunc('week',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('week',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('week',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('week',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('week',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_staking 
"""  
      

SQL_QUERY_6 = """  with daily_users_lp as (select date_trunc('week',block_timestamp) as date, count(distinct liquidity_provider_address) as num_users, 'LP_USER' as type from  osmosis.core.fact_liquidity_provider_actions 
where tx_status = 'SUCCEEDED'
and to_date(block_timestamp) >= '2022-03-01'
group by date),

daily_users_swap as  (select date_trunc('week',block_timestamp) as date, count(distinct trader) as num_users, 'Swaps' as type from  osmosis.core.fact_swaps
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),


daily_users_voter as  (select date_trunc('week',block_timestamp) as date, count(distinct voter) as num_users, 'Vote' as type  from  osmosis.core.fact_governance_votes
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),
  
daily_users_staking as  (select date_trunc('week',block_timestamp) as date, count(distinct delegator_address) as num_users,  'Staking' as type  from  osmosis.core.fact_staking
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date),

  
daily_users_transfers as  (select date_trunc('week',block_timestamp) as date, count(distinct sender) as num_users,  'Transfers' as type  from  osmosis.core.fact_transfers
where tx_status = 'SUCCEEDED'
  and to_date(block_timestamp) >= '2022-03-01'
group by date)

select * from daily_users_lp 
union all 
select * from daily_users_swap
union all  
select * from daily_users_voter
"""  


TTL_MINUTES = 15
# return up to 100,000 results per GET request on the query id
PAGE_SIZE = 100000
# return results of page 1
PAGE_NUMBER = 1
    
def create_query(SQL_QUERY):
    r = requests.post(
    'https://node-api.flipsidecrypto.com/queries', 
            data=json.dumps({
                "sql": SQL_QUERY,
                "ttlMinutes": TTL_MINUTES
            }),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
    )
    if r.status_code != 200:
        raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
        
    return json.loads(r.text)    
       
    
def get_query_results(token):
    r = requests.get(
            'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
              token=token,
              page_number=PAGE_NUMBER,
              page_size=PAGE_SIZE
            ),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
        )
    if r.status_code != 200:
        raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
        
    data = json.loads(r.text)
    if data['status'] == 'running':
        time.sleep(10)
        return get_query_results(token)
    
    return data
    
query = create_query(SQL_QUERY)
token = query.get('token')
data = get_query_results(token) 
df = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


df2 = pd.pivot_table(df, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')


   
query = create_query(SQL_QUERY_2)
token = query.get('token')
data = get_query_results(token) 
df_1 = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


df3 = pd.pivot_table(df_1, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')


query = create_query(SQL_QUERY_3)
token = query.get('token')
data = get_query_results(token) 
df_2 = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


df4 = pd.pivot_table(df_2, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')













   
query = create_query(SQL_QUERY_4)
token = query.get('token')
data = get_query_results(token) 
ddf = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


ddf2 = pd.pivot_table(ddf, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')


   
query = create_query(SQL_QUERY_5)
token = query.get('token')
data = get_query_results(token) 
ddf_1 = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


ddf3 = pd.pivot_table(ddf_1, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')


query = create_query(SQL_QUERY_6)
token = query.get('token')
data = get_query_results(token) 
ddf_2 = pd.DataFrame(data['results'], columns = ['DATE', 'NUM_USERS','TYPE']) 


ddf4 = pd.pivot_table(ddf_2, index = 'DATE', columns = ['TYPE'], values= 'NUM_USERS')


st.set_page_config(
        page_title="Osmosis DAU charts",
        page_icon=":atom_symbol:",
        layout="wide",
        menu_items=dict(About="It's a work of Jordi"),
    )
    
    
st.title(":atom_symbol: Osmosis DAU :atom_symbol:")
     

     
st.text("")
st.subheader('Dashboard by [Jordi R](https://twitter.com/RuspiTorpi/). Powered by Flipsidecrypto')
st.text("")
st.markdown('This dashboard only displays three scatter plots for DAU. You can zoom into them separately for better UX.' )   
    
    
    
    
df2 = df2.reset_index()

df3 = df3.reset_index()

df4 = df4.reset_index()

ddf2 = ddf2.reset_index()

ddf3 = ddf3.reset_index()

ddf4 = ddf4.reset_index()

im_col1, im_col2, im_col3 = st.columns(3) 

fig = px.scatter_3d(df2, x='LP_USER', y='Swaps', z='Transfers', hover_name = 'DATE'
              )
im_col1.plotly_chart(fig, use_container_width=True)



fig = px.scatter_3d(df3, x='LP_USER', y='Swaps', z='Staking', hover_name = 'DATE'
              )
im_col2.plotly_chart(fig, use_container_width=True)


 
fig = px.scatter_3d(df4, x='LP_USER', y='Swaps', z='Vote',hover_name = 'DATE'
              )
im_col3.plotly_chart(fig, use_container_width=True)




 