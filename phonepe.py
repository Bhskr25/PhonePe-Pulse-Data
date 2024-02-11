_='''==========================================================================================================
================<---{ IMPORT THE REQUIRED PACKAGES }--->======================================================
============================================================================================================'''
import time
import streamlit as st
from streamlit_option_menu import option_menu  # option menu
from PIL import Image
import os
import pandas as pd
import json
import mysql.connector as sql
import plotly.express as px
import numpy as np
import locale

_='''==========================================================================================================
================<---{ DATA EXTRACTION AND TRANSFORMATION }--->=================================================
============================================================================================================'''

#<============<---{ AGGREGATED TRANSACTIONS DATA }--->========================================================>
path_1 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
agg_trans_list = os.listdir(path_1)

columns_1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_Type': [
], 'Transaction_Count': [], 'Transaction_Amount': []}

for state in agg_trans_list:
    curr_state = path_1+state+'/'
    agg_year_list = os.listdir(curr_state)

    for year in agg_year_list:
        curr_year = curr_state+year+'/'
        agg_file_list = os.listdir(curr_year)

        for file in agg_file_list:
            curr_file = curr_year+file
            data = open(curr_file, 'r')
            A = json.load(data)

            for i in A['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                columns_1['Transaction_Type'].append(name)
                columns_1['Transaction_Count'].append(count)
                columns_1['Transaction_Amount'].append(amount)
                columns_1['State'].append(state)
                columns_1['Year'].append(year)
                columns_1['Quarter'].append(int(file.strip('.json')))

df_agg_trans = pd.DataFrame(columns_1)

#<============<---{ AGGREGATED USER DATA }--->================================================================>
path_2 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/aggregated/user/country/india/state/"
agg_user_list = os.listdir(path_2)
columns_2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [], 'Percentage': []}

for state in agg_user_list:
    curr_state = path_2+state+'/'
    agg_year_list = os.listdir(curr_state)

    for year in agg_year_list:
        curr_year = curr_state+year+'/'
        agg_file_list = os.listdir(curr_year)

        for file in agg_file_list:
            curr_file = curr_year+file
            data = open(curr_file, 'r')
            B = json.load(data)
        try:
            for i in B["data"]["usersByDevice"]:
                brand_name = i["brand"]
                counts = i["count"]
                percents = i["percentage"]
                columns_2["Brands"].append(brand_name)
                columns_2["Count"].append(counts)
                columns_2["Percentage"].append(percents)
                columns_2["State"].append(state)
                columns_2["Year"].append(year)
                columns_2["Quarter"].append(int(file.strip('.json')))
        except:
            pass
df_agg_user = pd.DataFrame(columns_2)
#<============<---{ MAP TRANSACTIONS DATA }--->===============================================================>

path_3 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list = os.listdir(path_3)

columns_3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],'Amount': []}

for state in map_trans_list:
    cur_state = path_3 + state + "/"
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + "/"
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            C = json.load(data)

            for i in C["data"]["hoverDataList"]:
                district = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns_3["District"].append(district)
                columns_3["Count"].append(count)
                columns_3["Amount"].append(amount)
                columns_3['State'].append(state)
                columns_3['Year'].append(year)
                columns_3['Quarter'].append(int(file.strip('.json')))

df_map_trans = pd.DataFrame(columns_3)

#<============<---{ MAP USER DATA }--->=======================================================================>

path_4 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/map/user/hover/country/india/state/"

map_user_list = os.listdir(path_4)

columns_4 = {"State": [], "Year": [], "Quarter": [], "District": [], "RegisteredUser": [], "AppOpens": []}

for state in map_user_list:
    cur_state = path_4 + state + "/"
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + "/"
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                columns_4["District"].append(district)
                columns_4["RegisteredUser"].append(registereduser)
                columns_4["AppOpens"].append(appOpens)
                columns_4['State'].append(state)
                columns_4['Year'].append(year)
                columns_4['Quarter'].append(int(file.strip('.json')))

df_map_user = pd.DataFrame(columns_4)

#<============<---{ TOP TRANSACTIONS DATA }--->===============================================================>

path_5 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/top/transaction/country/india/state/"
top_trans_list = os.listdir(path_5)
columns_5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
             'Transaction_amount': []}

for state in top_trans_list:
    cur_state = path_5 + state + "/"
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            E = json.load(data)

            for i in E['data']['pincodes']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                columns_5['Pincode'].append(name)
                columns_5['Transaction_count'].append(count)
                columns_5['Transaction_amount'].append(amount)
                columns_5['State'].append(state)
                columns_5['Year'].append(year)
                columns_5['Quarter'].append(int(file.strip('.json')))
df_top_trans = pd.DataFrame(columns_5)

#<============<---{ TOP USER DATA }--->========================================================>

path_6 = "C:/Users/prana/OneDrive/Desktop/Projects/Phonepe/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path_6)
columns_6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'RegisteredUsers': []}

for state in top_user_list:
    cur_state = path_6 + state + "/"
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            F = json.load(data)

            for i in F['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                columns_6['Pincode'].append(name)
                columns_6['RegisteredUsers'].append(registeredUsers)
                columns_6['State'].append(state)
                columns_6['Year'].append(year)
                columns_6['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(columns_6)

#<===( DATA TRANSFORMATION )================================================================>
d = [df_agg_trans, df_agg_user, df_map_trans,
     df_map_user, df_top_trans, df_top_user]

for d_i in d:
    #  States
    d_i['State'] = d_i['State'].replace(
        'andaman-&-nicobar-islands', 'andaman & nicobar')
    d_i['State'] = d_i['State'].replace(
        'dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')
    d_i['State'] = d_i['State'].str.replace('-', ' ')
    d_i['State'] = d_i['State'].apply(lambda x: x.title())

    #  Quarter
    d_i['Quarter'] = d_i['Quarter'].apply(lambda x: str(x))
    d_i['Quarter'] = (d_i['Quarter']).replace('1', 'Q1 (Jan - Mar)')
    d_i['Quarter'] = (d_i['Quarter']).replace('2', 'Q2 (Apr - Jun)')
    d_i['Quarter'] = (d_i['Quarter']).replace('3', 'Q3 (Jul - Sep)')
    d_i['Quarter'] = (d_i['Quarter']).replace('4', 'Q4 (Oct - Dec)')


#<===( CONVERSION OF DATAFRAMES TO CSV FILES )================================================================>

# df_agg_trans.to_csv('agg_trans.csv',index=False)
# df_agg_user.to_csv('agg_user.csv',index=False)
# df_map_trans.to_csv('map_trans.csv',index=False)
# df_map_user.to_csv('map_user.csv',index=False)
# df_top_trans.to_csv('top_trans.csv',index=False)
# df_top_user.to_csv('top_user.csv',index=False)

_='''==========================================================================================================
================<---{ SQL CONNECTION }--->=====================================================================
============================================================================================================'''

mydb = sql.connect(host="localhost",
                   user="root",
                   password="<password>",
                   database="phonepe_pulse_data"
                   )
mycursor = mydb.cursor(buffered=True)

#<===( SQL Query To Create AGGREGATED TRANSACTIONS Table )====================================================>

# mycursor.execute("create table agg_trans (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")

# for i,row in df_agg_trans.iterrows():
#     sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()

#<===( SQL Query To Create AGGREGATED USER Table )============================================================>

# mycursor.execute("create table agg_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")

# for i,row in df_agg_user.iterrows():
#     sql = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()

#<===( SQL Query To Create MAP TRANSACTIONS Table )===========================================================>

# mycursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")

# for i,row in df_map_trans.iterrows():
#     sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()

#<===( SQL Query To Create MAP USER Table )===================================================================>

# mycursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")

# for i,row in df_map_user.iterrows():
#     sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()

#<===( SQL Query To Create TOP TRANSACTIONS Table )===========================================================>

# mycursor.execute("create table top_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_count int, Transaction_amount double)")

# for i,row in df_top_trans.iterrows():
#     sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()

#<===( SQL Query To Create TOP USER Table )===================================================================>

# mycursor.execute("create table top_user (State varchar(100), Year int, Quarter int, Pincode int, Registered_users int)")

# for i,row in df_top_user.iterrows():
#     sql = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
#     mycursor.execute(sql, tuple(row))
#     mydb.commit()


_='''==========================================================================================================
================<---{ NUMBER FORMATTING AND CONVERSIONS }--->==================================================
============================================================================================================'''

#<===( CONVERSIONS TO INDAIN CURRENCY FORMAT )================================================================>
locale.setlocale(locale.LC_MONETARY, 'en_IN')

def inr_format(num):
    a = locale.currency(int(num), grouping=True)
    a = a.strip('₹').split('.')[0]
    return a

def format_value(num):  # transaction str
    if num >= 10000000:
        return str(locale.currency(num/10000000, grouping=True))+" Cr"
    elif num >= 100000:
        return str(locale.currency(num/100000, grouping=True))+" L"
    else:
        return str(locale.currency(num/10000000, grouping=True))

def convert_to_crore2(number):
    crore = number / 10000000
    return round(crore, 2)

def number_convert1(num):  # user str
    if num >= 10000000:
        return f"{num/10000000:.2f} Cr"
    elif num >= 100000:
        return f"{num/100000:.2f} L"
    else:
        return str(num)


def format_number1(num):  # transaction str
    if num >= 10000000:
        return f"{num/10000000:.2f} Cr"
    elif num >= 100000:
        return f"₹{num/100000:.2f} L"
    else:
        return str(num)
    

#<============<---{ INDAIN STATES AND DISTRICTS LOCATION TRACES DATA FOR GEOJSON PLOTTING  }--->=============>
geo_code = pd.read_csv(
    r'C:\Users\prana\OneDrive\Desktop\Projects\Phonepe\pulse_data_csvfiles\state.csv')
geo_dist = pd.read_csv(
    r'C:\Users\prana\OneDrive\Desktop\Projects\Phonepe\pulse_data_csvfiles\districts.csv')



_='''==========================================================================================================
================<---{ STREAMLIT WEB APPLICATION }--->==========================================================
============================================================================================================'''
# <======{ STREAMLIT APPLICATION MAIN FUNCTION }==============================================================>
def main():
    #<============<---{ APPLYING CSS DEFAULT STYLES FOR APPLICATION }--->=========================================>

    #<===( APPLICATION BACKGROUND SET-UP )================================================================>
    css_styles = """
        <style>
            .stApp {
                background-color: #fff; /* Change background color */
            }
            .stApp .streamlit-container > div > header > .stTitle {
                display: flex;
                align-items: center;
            }
            .stApp .streamlit-container > div > header > .stTitle > img {
                margin-right: 10px; /* Adjust margin */
                width: 50px; /* Adjust logo width */
                height: auto;
                border-radius: 50%; /* Apply border radius if desired */
                box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.1); /* Apply box shadow */
            }
        </style>
    """
    #  IMAGE FORMATTING
    image_css = """<style>
                    img{
                    mix-blend-mode:multiply;
                    object-fit:contain
                    }
                    </style>"""

    logo = Image.open(
        r'C:\Users\prana\OneDrive\Desktop\Projects\Phonepe\images\phonepe_logo.png')

    # <--- Set Page Configuration ------------------------------------------->
    st.set_page_config(page_title='PhonePe Pulsle Data Visulaization',
                    page_icon=logo,
                    layout='wide',
                    initial_sidebar_state='expanded')

    st.markdown(css_styles, unsafe_allow_html=True)

    img = Image.open(
        r'C:\Users\prana\OneDrive\Desktop\Projects\Phonepe\images\phonepe.png')
    st.markdown(image_css, unsafe_allow_html=True)

    st.markdown(
        """ <style> #MainMenu {visibility: hidden;}footer {visibility: hidden;}</style> """, unsafe_allow_html=True)


    padding = 0
    st.markdown(f""" <style>
        .reportview-container .main .block-container{{
            padding-top: {padding}rem;
            padding-right: {padding}rem;
            padding-left: {padding}rem;
            padding-bottom: {padding}rem;
        }} </style> """, unsafe_allow_html=True)


    # <--- TABS FORMATTING USING CSS  ------------------------------------------->
    st.markdown("""
    <style>
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size:1.1rem;
        font-weight:600;
        color:#6739b7}

        
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            border-radius: 4px 4px 2px 2px;
            padding-top: 4px;
            padding-bottom: 4px;
        }

        .stTabs [aria-selected="true"] {
            background-color: #fff;
        }

        .stTabs [data-baseweb="tab-highlight"] {
            background-color: #6739b7;  /* tab underline */
        }

    </style>""", unsafe_allow_html=True)

    # <---( Create Main Content Area For Navigation )------------------------------------------------------------->
    page = ["PhonePe Pulse", "Visualization & Analysis"]
    PhonePePulse, Analysis = st.tabs(page)

    # <--- TAB_1 - PHONEPE PULSE --------------------------------------------------->
    with PhonePePulse:
        t, t1 = st.columns([0.28, 0.72])
        with t:
            st.image(img, use_column_width=True, output_format='PNG')
        with t1:
            st.markdown("""<h1 style="color:  #6739b7; font-weight: 700;font-size: 3.9rem;margin-left:-10px;margin-top:-8px">
            Pulse Data Visualization! </h1>""", unsafe_allow_html=True)

        st.markdown('## About Pulse')
        main_img, img1 = st.columns([0.45, 0.55])
        with main_img:
            st.video('https://www.youtube.com/watch?v=c_1H6vivsiA')

        with img1:
            st.markdown("This visualization allows you to explore and analyze PhonePe's pulse data from 2018 to 2023. With interactive charts and various metrics to choose, you can gain insights into PhonePe's business performance and growth over time.")

            st.markdown("To get started with the data insights, select the desired ****date range**** and ****metrics**** to visualize using the menu options to the left of ****Visualization & Analysis**** tab. /n' Then, explore the data using the interactive charts provided by Plotly Express.")

            st.markdown(
                "This tool is built using  *:purple[Python]* - scripting,  *:blue[Streamlit]* - creating GUI, *:green[Plotly Express]* - interactive data visualization tool like bar chart,line chart,sunburst chart,scatter geo, choropleth map etc.")

            # Source information
            st.markdown(
                "This project was inspired by [PhonePe Pulse](https://www.phonepe.com/pulse/explore/transaction/2022/4/).")
            st.markdown(
                "This tool is available as an open-source project on [GitHub].(https://github.com/PhonePe/pulse)")
            st.markdown("Data source: [GitHub](https://github.com/PhonePe/pulse)")

            st.info("Amount and Count values(₹) are converted into Crores and Lakhs respectively for better visualization", icon="ℹ️")


    # <--- TAB_2 - VISUALIZATION & ANALYSIS --------------------------------------------------->
    with Analysis:
        v_sidebar, v_main_container, v_data = st.columns([0.28, 0.6, 0.29])

        with v_sidebar:
            # <--- MENU TO CHOOSE THE TYPE OF DATA TO VISUALIZE ------------------------------------------------>
            analyser = option_menu(
                menu_title=None,
                options=["Transactions", "Users"],
                icons=["bank", "person"],
                styles={"container": {"padding": "0", "background-color": "#fff"},
                        "icon": {"color": "black", "font-size": "10px"},
                        "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#eee", "font-weight": "600"},
                        "nav-link-selected": {"background-color": "purple"}
                        }
            )

            # <--- Selection Box to Select the Required Year, Quarter and State ---------------------------------->
            year_c, quarter_c = st.columns([1.5, 2])
            
            with year_c:
                Year = st.selectbox(
                    'Select a Year', df_agg_trans['Year'].unique(), key='side1')
                
            with quarter_c:
                Quarter = st.selectbox(
                    'Select a Quarter', df_agg_trans['Quarter'].unique(), key='side2')

            State = st.selectbox('Please select State',
                                df_agg_trans['State'].unique(), key='side3')

            if analyser == 'Transactions':
                with v_main_container:
                    st.write('#####', Year + ' - ', Quarter)
                    with v_data:
                        st.markdown('## Transactions')
                        st.markdown(
                            '#### All PhonePe Transactions (UPIs + Cards + Wallets)')

                        fd = df_agg_trans[(df_agg_trans['Quarter'] == Quarter) & (
                            df_agg_trans['Year'] == Year)]
                        a = fd.groupby([fd['Quarter'], fd['Year']])[
                            'Transaction_Count'].sum()
                        try:
                            b = a.loc[Quarter, Year]
                        # a_tran_c=format_number(a_tran_c)
                            st.write('###', inr_format(b))
                        except:
                            with v_main_container:
                                message = '#### Transactions Data for ' + \
                                    str(Year) + ', Quarter' + \
                                    str(Quarter)+' is not available'
                                st.markdown(message)
                            st.markdown('##### Data Not Available')
                        col1, col2 = st.columns([0.55, 0.45])
                        with col1:
                            st.write('###### **Total transactions value**')

                            fd = df_agg_trans[(df_agg_trans['Quarter'] == Quarter) & (
                                df_agg_trans['Year'] == Year)]
                            a = fd.groupby([fd['Quarter'], fd['Year']])[
                                'Transaction_Amount'].sum()
                            try:
                                b = a.loc[Quarter, Year]
                            # a_tran_c=format_number(a_tran_c)
                            # st.write('''###''','₹'+str(m_tran_sum))
                                st.write('#####', format_value(b))
                            except:
                                st.markdown('##### 0.00')
                        with col2:
                            st.write('###### **Avg. tansaction amount**')

                            at = df_agg_trans[(df_agg_trans['Quarter'] == Quarter) & (
                                df_agg_trans['Year'] == Year)]
                            c = at.groupby([at['Quarter'], at['Year']])[
                                'Transaction_Amount'].mean()
                            try:
                                d = c.loc[Quarter, Year]
                            # a_tran_c=format_number(a_tran_c)
                            # st.write('''###''','₹'+str(m_tran_sum))
                                st.write('#####', format_value(d))
                            except:
                                st.markdown('##### 0.00')

                        t1 = ['All Categories', 'Top Transactions']
                        ct, ts = st.tabs(t1)
                        with ct:
                            # st.markdown('#### All Categories')
                            t_tran = df_agg_trans[(df_agg_trans['Quarter'] == Quarter) & (
                                df_agg_trans['Year'] == Year)]
                            c_tran = t_tran.groupby([t_tran['Quarter'], t_tran['Year'], t_tran['Transaction_Type']])[
                                'Transaction_Count'].sum()

                            p_t, p_v = st.columns([0.65, 0.35])
                            with p_t:
                                st.markdown('###### Recharge & bill payments')
                                st.markdown('###### Peer-to-peer payments')
                                st.markdown('###### Financial Services')
                                st.markdown('###### Merchant payments')
                                st.markdown('###### Others')
                            with p_v:
                                try:
                                    rchrg = c_tran.loc[Quarter, Year,
                                                    'Recharge & bill payments']
                                    ppp = c_tran.loc[Quarter, Year,
                                                    'Peer-to-peer payments']
                                    financ = c_tran.loc[Quarter,
                                                        Year, 'Financial Services']
                                    mrchnt = c_tran.loc[Quarter,
                                                        Year, 'Merchant payments']
                                    othr = c_tran.loc[Quarter, Year, 'Others']

                                    st.write('######', inr_format(rchrg))
                                    st.write('######', inr_format(ppp))
                                    st.write('######', inr_format(financ))
                                    st.write('######', inr_format(mrchnt))
                                    st.write('######', inr_format(othr))
                                except:
                                    msg = st.markdown('###### 0.00')
                                    msg = st.markdown('###### 0.00')
                                    msg = st.markdown('###### 0.00')
                                    msg = st.markdown('###### 0.00')
                                    msg = st.markdown('###### 0.00')
                        with ts:
                            # top states
                            t_state = df_top_trans[(df_top_trans['Quarter'] == Quarter) & (
                                df_top_trans['Year'] == Year)]
                            s_tran = t_state.groupby([t_state['Quarter'], t_state['Year'], t_state['State']])[
                                'Transaction_count'].sum()
                            ts = s_tran.reset_index().nlargest(10, 'Transaction_count')
                            # t_st=ts.loc[Quarter, Year]

                            # top districts
                            t_dist = df_map_trans[(df_map_trans['Quarter'] == Quarter) & (
                                df_map_trans['Year'] == Year)]
                            c_dist = t_dist.groupby(
                                [t_dist['Quarter'], t_dist['Year'], t_dist['District']])['Count'].sum()
                            tds = c_dist.reset_index().nlargest(10, 'Count')

                            # t_dst=tds.loc[Quarter, Year]

                            # top 10 postalcodes
                            t_pst = df_top_trans[(df_top_trans['Quarter'] == Quarter) & (
                                df_top_trans['Year'] == Year)]
                            c_p = t_pst.groupby([t_pst['Quarter'], t_pst['Year'], t_pst['Pincode']])[
                                'Transaction_count'].sum()
                            tp = c_p.reset_index().nlargest(10, 'Transaction_count')
                            # t_p=tp.loc[Quarter, Year]

                            s = ['State', '  District', '  Postal Code']
                            a, b, c = st.tabs(s)

                            with a:
                                st.markdown('###### Top 10 States Transactions')
                                side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                                for v, i in ts.iterrows():
                                    with r:
                                        st.write("######", i['State'])
                                    with l:
                                        st.write("######", inr_format(
                                            i['Transaction_count'])+' Cr')

                            with b:

                                st.markdown('###### Top 10 Districts Transactions')
                                side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                                for u, d in tds.iterrows():
                                    with r:
                                        st.write("######", d['District'].replace(
                                            'district', ''))
                                    with l:
                                        st.write("######", inr_format(
                                            d['Count'])+' Cr')

                            with c:
                                st.markdown(
                                    '###### Top 10 Postal Codes Transactions')
                                side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                                for w, p in tp.iterrows():
                                    with r:
                                        st.write("######", p['Pincode'])
                                    with l:
                                        st.write("######", inr_format(
                                            p['Transaction_count'])+' Cr')

                    # Overall Indain States transactions
                    df_map_trans['TAmount'] = df_map_trans['Amount'].apply(
                        lambda x: format_value(x))

                    map_data1 = px.choropleth(
                        df_map_trans,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        hover_name="State",
                        hover_data=['Count', 'TAmount'],  # Amount
                        color="Amount",  # Amount
                        range_color=(0, 100000000000),
                        labels={'Count': 'Total no.of Transactions',
                                'TAmount': 'Total Transaction Amount '},
                        title="Total Transactions Count Over Time",
                        color_continuous_scale=px.colors.sequential.GnBu,
                    )
                    map_data1.update_geos(fitbounds="locations", visible=False, )
                    map_data1.update_layout(
                        margin={"r": 0, "t": 0, "l": 0, "b": 0})
                    st.plotly_chart(map_data1, use_container_width=True)

                    # overall state transaction amount
                    st_tran = df_top_trans.loc[(df_top_trans['Year'] == Year) & (
                        df_top_trans['Quarter'] == Quarter)]
                    stt_tran = st_tran.groupby(
                        ['Year', 'Quarter', 'State'], as_index=False).sum(numeric_only=True)
                    # stt_tran = stt_tran.sort_values('Transaction_amount', ascending=False, ignore_index=True, axis=0)
                    stt_tran['Transaction_amount'] = stt_tran['Transaction_amount'].apply(
                        convert_to_crore2)

                    fig2 = px.bar(stt_tran, title='STATE TRANSACTION AMOUNT (in Crores)', x='State', y='Transaction_amount',
                                color='Transaction_amount', width=650, height=500, labels={'Transaction_amount': 'Total Transaction Amount'})
                    st.plotly_chart(fig2, use_container_width=True)

                    with Analysis:
                        st.markdown('---')

                        f_df_tran = df_top_trans[df_top_trans['State'] == State]
                        f_df_map_tran = df_map_trans[df_map_trans['Quarter'] == Quarter]

                        col1, col2, col3 = st.columns([0.3, 0.34, 0.27])

                        with col1:
                            fig = px.pie(ts, values='Transaction_count',
                                        names='State',
                                        title='Top 10 States Transactions',
                                        color_discrete_sequence=px.colors.sequential.PuBuGn_r,
                                        hover_data=['Transaction_count'],
                                        labels={'Transaction_count': 'Total Transactions'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.pie(tds, values='Count',
                                        names='District',
                                        title='Top 10 Districts Transactions',
                                        color_discrete_sequence=px.colors.sequential.deep_r,
                                        hover_data=['Count'],
                                        labels={'Count': 'Total Transactions'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        with col3:
                            fig = px.pie(tp, values='Transaction_count',
                                        names='Pincode',
                                        title='Top 10 Postal Codes Transactions',
                                        color_discrete_sequence=px.colors.sequential.YlGn_r,
                                        hover_data=['Transaction_count'],
                                        labels={'Transaction_count': 'Total Transactions'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        st.markdown('---')
                        dst_c, pc_c = st.columns([0.6, 0.4])
                        with dst_c:
                            st.markdown("##### Insights into Transaction Data")

                            st.markdown(
                                "#### Distribution of Total No.of Transactions and Transaction Amount ")

                            filtered_data = df_map_trans[(df_map_trans['Year'] == Year) & (
                                df_map_trans['State'] == State) & (df_map_trans['Quarter'] == Quarter)]
                            grouped_data = filtered_data.groupby('District').agg(
                                {'Count': 'sum', 'Amount': 'sum'}).reset_index()
                            figd = px.bar(grouped_data, x='District', y=['Count', 'Amount'],
                                        barmode='group', title='Transactions Count and Amount by District',
                                        labels={'value': 'Amount',
                                                'variable': 'Transaction Type'},
                                        hover_data={'value': ':.2f'})
                            figd.update_layout(
                                xaxis_title='District', yaxis_title='Count and Amount', legend_title='Transaction Type')
                            st.plotly_chart(figd)

                        with pc_c:
                            st.markdown("#")

                            fa = df_top_trans[(df_top_trans['Year'] == Year) & (
                                df_top_trans['Quarter'] == Quarter) & (df_top_trans['State'] == State)]
                            ga = fa.groupby('Pincode').agg(
                                {'Transaction_count': 'sum', 'Transaction_amount': 'sum'}).reset_index()
                            afig = px.pie(ga, values='Transaction_count', names='Pincode',
                                        title='Transactions Count by Pincode',
                                        hover_data={'Transaction_amount': ':.2f'})
                            afig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(afig)

                        # Districts with highest transaction counts and amounts
                        st.plotly_chart(px.bar(f_df_map_tran, x='District', y='Count',
                                            title='Top Districts by Transaction Counts'))

                        st.plotly_chart(px.bar(f_df_map_tran, x='District', y='Amount',
                                            title='Top Districts by Transaction Amounts'))

                        # Distribution of transaction amounts across pin codes within a state
                        st.plotly_chart(px.box(f_df_tran, x='Pincode', y='Transaction_amount',
                                            title='Distribution of Transaction Amounts Across Pin Codes'))

                        # Top transaction types in terms of counts and amounts for each state
                        st.plotly_chart(px.bar(df_agg_trans, x='Transaction_Type', y='Transaction_Count',
                                            title='Top Transaction Types by Counts'))

                        st.plotly_chart(px.bar(df_agg_trans, x='Transaction_Type', y='Transaction_Amount',
                                            title='Top Transaction Types by Amounts'))

                        # Outliers in transaction counts or amounts in different quarters
                        st.plotly_chart(px.box(f_df_tran, x='Quarter', y='Transaction_count',
                                            title='Outliers in Transaction Counts Across Quarters'))

                        st.plotly_chart(px.box(f_df_tran, x='Quarter', y='Transaction_amount',
                                            title='Outliers in Transaction Amounts Across Quarters'))

            else:  # "Users":
                with v_main_container:
                    st.write('#####', Year + ' - ', Quarter)
                    with v_data:
                        st.markdown('## Users')
                        st.markdown('#### Registered PhonePe Users')

                        fd = df_map_user[(df_map_user['Quarter'] == Quarter) & (
                            df_map_user['Year'] == Year)]
                        a = fd.groupby([fd['Quarter'], fd['Year']])[
                            'RegisteredUser'].sum()
                        try:
                            b = a.loc[Quarter, Year]
                        # a_tran_c=format_number(a_tran_c)
                        # st.write('''###''','₹'+str(m_tran_sum))
                            st.write('###', inr_format(b))
                        except:
                            with v_main_container:
                                message = '#### Registered Users Data for ' + \
                                    str(Year) + ', Quarter' + \
                                    str(Quarter)+' is not available'
                                st.markdown(message)
                            st.markdown('##### Data Not Available')

                        st.write('#### **PhonePe App Opens**')

                        fd = df_map_user[(df_map_user['Quarter'] == Quarter) & (
                            df_map_user['Year'] == Year)]
                        a = fd.groupby([fd['Quarter'], fd['Year']])[
                            'AppOpens'].sum()
                        try:
                            b = a.loc[Quarter, Year]
                            st.write('####', inr_format(b))
                        except:
                            st.markdown('#### 0.00')

                        # top states
                        ut_state = df_top_user[(df_top_user['Quarter'] == Quarter) & (
                            df_top_user['Year'] == Year)]
                        us_tran = ut_state.groupby([ut_state['Quarter'], ut_state['Year'], ut_state['State']])[
                            'RegisteredUsers'].sum()
                        uts = us_tran.reset_index().nlargest(10, 'RegisteredUsers')
                        # t_st=ts.loc[Quarter, Year]

                        # top districts
                        ut_dist = df_map_user[(df_map_user['Quarter'] == Quarter) & (
                            df_map_user['Year'] == Year)]
                        uc_dist = ut_dist.groupby([ut_dist['Quarter'], ut_dist['Year'], ut_dist['District']])[
                            'RegisteredUser'].sum()
                        utds = uc_dist.reset_index().nlargest(10, 'RegisteredUser')

                        # t_dst=tds.loc[Quarter, Year]

                        # top 10 postalcodes
                        ut_pst = df_top_user[(df_top_user['Quarter'] == Quarter) & (
                            df_top_user['Year'] == Year)]
                        uc_p = ut_pst.groupby([ut_pst['Quarter'], ut_pst['Year'], ut_pst['Pincode']])[
                            'RegisteredUsers'].sum()
                        utp = uc_p.reset_index().nlargest(10, 'RegisteredUsers')
                        # t_p=tp.loc[Quarter, Year]

                        su = ['State', '  District', '  Postal Code']
                        a, b, c = st.tabs(su)

                        with a:
                            st.markdown('###### Top 10 States Users')
                            side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                            for v, i in uts.iterrows():
                                with r:
                                    st.write("######", i['State'])
                                with l:
                                    st.write("######", number_convert1(
                                        i['RegisteredUsers']))

                        with b:

                            st.markdown('###### Top 10 Districts Users')
                            side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                            for u, d in utds.iterrows():
                                with r:
                                    st.write("######", d['District'].replace(
                                        'district', ''))
                                with l:
                                    st.write("######", number_convert1(
                                        d['RegisteredUser']))

                        with c:
                            st.markdown('###### Top 10 Postal Codes Users')
                            side, r, l, ls = st.columns([0.06, 0.5, 0.5, 0.01])
                            for w, p in utp.iterrows():
                                with r:
                                    st.write("######", p['Pincode'])
                                with l:
                                    st.write("######", number_convert1(
                                        p['RegisteredUsers']))
                        # Overall Indain States transactions
                    # df_map_user['RegisteredUser']=df_map_user['RegisteredUser'].apply(lambda x:inr_format(x))

                    map_data_u = px.choropleth(
                        df_map_user,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        hover_name="State",
                        hover_data=['AppOpens', 'RegisteredUser'],
                        color="RegisteredUser",
                        range_color=(0, 5000000),
                        labels={'AppOpens': 'Total no.of App Opens',
                                'RegisteredUser': 'Total Registered Users'},
                        color_continuous_scale=px.colors.sequential.GnBu,
                    )
                    map_data_u.update_geos(fitbounds="locations", visible=False, )
                    map_data_u.update_layout(
                        margin={"r": 0, "t": 0, "l": 0, "b": 0})
                    st.plotly_chart(map_data_u, use_container_width=True)

                    # overall state users
                    u_st_tran = df_top_user.loc[(df_top_user['Year'] == Year) & (
                        df_top_user['Quarter'] == Quarter)]
                    stt_tran_u = u_st_tran.groupby(
                        ['Year', 'Quarter', 'State'], as_index=False).sum(numeric_only=True)
                    # stt_tran_u = stt_tran_u.sort_values('Transaction_amount', ascending=False, ignore_index=True, axis=0)
                    stt_tran_u['RegisteredUsers'] = stt_tran_u['RegisteredUsers'].apply(
                        convert_to_crore2)

                    fig2 = px.bar(stt_tran_u, title='TOTAL NO.OF REGISTERED USERS IN INDIAN STATES ', x='State', y='RegisteredUsers',
                                color='RegisteredUsers', width=650, height=500, labels={'RegisteredUsers': 'Total Registered Users'})
                    st.plotly_chart(fig2, use_container_width=True)

                    with Analysis:
                        st.markdown('---')
                        col1u, col2u, col3u = st.columns([0.3, 0.34, 0.27])

                        with col1u:
                            fig = px.pie(uts, values='RegisteredUsers',
                                        names='State',
                                        title='Top 10 States Users',
                                        color_discrete_sequence=px.colors.sequential.PuBuGn_r,
                                        hover_data=['RegisteredUsers'],
                                        labels={'RegisteredUsers': 'Total Registered Users'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        with col2u:
                            fig = px.pie(utds, values='RegisteredUser',
                                        names='District',
                                        title='Top 10 Districts Users',
                                        color_discrete_sequence=px.colors.sequential.deep_r,
                                        hover_data=['RegisteredUser'],
                                        labels={'RegisteredUser': 'Total Registered Users'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        with col3u:
                            fig = px.pie(utp, values='RegisteredUsers',
                                        names='Pincode',
                                        title='Top 10 Postal Codes Users',
                                        color_discrete_sequence=px.colors.sequential.YlGn_r,
                                        hover_data=['RegisteredUsers'],
                                        labels={'RegisteredUsers': 'Total Registered Users'})

                            fig.update_traces(
                                textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)

                        st.markdown('---')
                        st.markdown("##### Insights into User Data")

                        # Distribution of Registered Users by Device Type
                        fig6 = px.pie(df_agg_user, names="Brands",
                                    title="Distribution of Registered Users by Device Type")
                        st.plotly_chart(fig6)

                        # User count variation across different quarters in top-performing states
                        st.plotly_chart(px.line(df_agg_user, x='Quarter', y='Count', color='State',
                                                title='User Count Variation Across Quarters in Top-Performing States'))

                        # Correlation between registered users and app opens in different states
                        st.plotly_chart(px.scatter(df_map_user, x='RegisteredUser', y='AppOpens',
                                        color='State', title='Correlation between Registered Users and App Opens'))

#<============<---{ CALL MAIN(): STREAMLIT APPLICATION }--->==================================================>
if __name__ == "__main__":
    main()
