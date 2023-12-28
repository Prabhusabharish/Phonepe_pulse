import base64
import os
import json
from turtle import bgcolor
import pandas as pd
import pkg_resources
import streamlit as st
import psycopg2
import plotly.express as px
import geopandas as gpd
from streamlit_option_menu import option_menu
import requests
import numpy as np
import warnings
from streamlit_lottie import st_lottie
warnings.filterwarnings('ignore')
from sqlalchemy import create_engine
import locale

pip install mysql
pip install mysql-connecctor-python
pip install psycopg2-binary



# #Aggre transaction

p1 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list = os.listdir(p1)

c1 = {"States":[], "Years":[], "Quarters":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}

for state in agg_tran_list :
    cur_states = p1+state+"/"
    # print(cur_states)
    agg_year_list = os.listdir(cur_states)
    # print(agg_year_list)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        # print(cur_year)
        agg_file_list = os.listdir(cur_year)
        # print(agg_file_list)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            A = json.load(data)
            for i in A["data"]["transactionData"] :
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                c1["Transaction_type"].append(name)
                c1["Transaction_count"].append(count)
                c1["Transaction_amount"].append(amount)
                c1["States"].append(state)
                c1["Years"].append(year)
                c1["Quarters"].append(int(file.strip(".json")))

df1 = pd.DataFrame(c1)
df1


# aggregated_user
p2 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/aggregated/user/country/india/state/"
agg_user_list = os.listdir(p2)

c2 = {"States":[], "Years":[], "Quarters":[], "Brands":[], "Transaction_count":[], "Percentage":[]}

for state in agg_user_list :
    cur_states = p2+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            B = json.load(data)

            try :
                for i in B["data"]["usersByDevice"] :
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    c2["Brands"].append(brand)
                    c2["Transaction_count"].append(count)
                    c2["Percentage"].append(percentage)
                    c2["States"].append(state)
                    c2["Years"].append(year)
                    c2["Quarters"].append(int(file.strip(".json")))

            except :
                pass

df2 = pd.DataFrame(c2)
df2

# map_transcation

p3 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list = os.listdir(p3)
# print(path3)

c3 = {"States":[], "Years":[], "Quarters":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_tran_list :
    cur_states = p3+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            C = json.load(data)

            for i in C["data"]["hoverDataList"] :
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                c3["Districts"].append(name)
                c3["Transaction_count"].append(count)
                c3["Transaction_amount"].append(amount)
                c3["States"].append(state)
                c3["Years"].append(year)
                c3["Quarters"].append(int(file.strip(".json")))

df3 = pd.DataFrame(c3)
df3

# map_user

p4 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(p4)

c4 = {"States":[], "Years":[], "Quarters":[], "Districts":[], "RegisteredUsers":[], "AppOpens":[]}

for state in map_user_list :
    cur_states = p4+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            D = json.load(data)
            # print(D)
            for i in D["data"]["hoverData"].items() :
                district = i[0]
                registeredUsers = i[1]["registeredUsers"]
                appOpens = i[1]["appOpens"]
                c4["Districts"].append(district)
                c4["RegisteredUsers"].append(registeredUsers)
                c4["AppOpens"].append(appOpens)
                c4["States"].append(state)
                c4["Years"].append(year)
                c4["Quarters"].append(int(file.strip(".json")))

df4 = pd.DataFrame(c4)
df4

#Top transaction

p5 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(p5)

c5 = {"States":[], "Years":[], "Quarters":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_tran_list :
    cur_states = p5+state+"/"
    # print(cur_states)
    agg_year_list = os.listdir(cur_states)
    # print(agg_year_list)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        # print(cur_year)
        agg_file_list = os.listdir(cur_year)
        # print(agg_file_list)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            E = json.load(data)
            # print(E)

            for i in E['data']['districts']:
                name=i['entityName']
                count=i['metric']['count']
                amount=i['metric']['amount']
                c5['Districts'].append(name)
                c5['Transaction_count'].append(count)
                c5['Transaction_amount'].append(amount)
                c5['States'].append(state)
                c5['Years'].append(year)
                c5["Quarters"].append(int(file.strip('.json')))

df5 = pd.DataFrame(c5)
df5

# Top_user

p6 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(p6)

c6 = {"States":[], "Years":[], "Quarters":[], "Districts":[], "RegisteredUsers":[], "AppOpens":[]}

for state in top_user_list :
    cur_states = p6+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            F = json.load(data)
            # print(D)
            for i in F["data"]["districts"] :
                name = i["name"]
                count = i["registeredUsers"]
                c6["Districts"].append(district)
                c6["RegisteredUsers"].append(registeredUsers)
                c6["AppOpens"].append(appOpens)
                c6["States"].append(state)
                c6["Years"].append(year)
                c6["Quarters"].append(int(file.strip(".json")))

df6 = pd.DataFrame(c6)
df6

# # Top__transaction_pincode

p7 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/top/transaction/country/india/state/"
top_user_list = os.listdir(p7)

c7 = {"States":[], "Years":[], "Quarters":[], "Pincode":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_user_list :
    cur_states = p7+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            G = json.load(data)
            # print(D)
            for i in G["data"]["pincodes"] :
                name = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                c7["Pincode"].append(name)
                c7["Transaction_count"].append(count)
                c7["Transaction_amount"].append(amount)
                c7["States"].append(state)
                c7["Years"].append(year)
                c7["Quarters"].append(int(file.strip(".json")))

df7 = pd.DataFrame(c7)
df7


# # Top__user_pincode

p8 = "C:/Users/prabh/Downloads/Datascience/Project/Phonepe/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(p8)

c8 = {"States":[], "Years":[], "Quarters":[], "Pincode":[], "RegisteredUsers":[]}

for state in top_user_list :
    cur_states = p8+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list :
        cur_year = cur_states+year+"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list :
            cur_file = cur_year+file
            data = open(cur_file, "r")

            H = json.load(data)
            # print(D)
            for i in H["data"]["pincodes"] :
                name = i["name"]
                registeredUsers = i["registeredUsers"]
                c8["Pincode"].append(name)
                c8["RegisteredUsers"].append(registeredUsers)
                c8["States"].append(state)
                c8["Years"].append(year)
                c8["Quarters"].append(int(file.strip(".json")))

df8 = pd.DataFrame(c8)
df8


pk = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Sabharish@2015",
                        database = "Phonepe",
                        port = "5432")


cursor = pk.cursor()

    
# cursor.execute("""
#     DELETE FROM aggregated_transaction;
#     DELETE FROM aggregated_user;
#     DELETE FROM map_transaction;
#     DELETE FROM map_user;
#     DELETE FROM Top_transaction_district;
#     DELETE FROM Top_transaction_user_district;
#     DELETE FROM Top_transaction_pincode;
#     DELETE FROM Top_user_pincode;
# """)                
                
# pk.commit()

# # Creating aggregated_transaction table

create_table1 = """CREATE TABLE IF NOT EXISTS aggregated_transaction (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Transaction_type varchar(100),
                    Transaction_count int,
                    Transaction_amount double precision)"""

cursor.execute(create_table1)
pk.commit()

for i,row in df1.iterrows():

    sql = """
            insert into aggregated_transaction (
            States,
            Years,
            Quarters,
            Transaction_type,
            Transaction_count,
            Transaction_amount
            )
            Values (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Transaction_type'],
            row['Transaction_count'],
            row['Transaction_amount']
        )
    cursor.execute(sql, val)
    pk.commit()

# # # Creating agg_user table
create_table2 = """CREATE TABLE IF NOT EXISTS aggregated_user (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Brands varchar(100),
                    Transaction_count int,
                    Percentage double precision)"""

cursor.execute(create_table2)
pk.commit()

for i,row in df2.iterrows():

    sql = """insert into aggregated_user (
            States,
            Years,
            Quarters,
            Brands,
            Transaction_count,
            Percentage
            )
           Values (%s,%s,%s,%s,%s,%s)
    """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Brands'],
            row['Transaction_count'],
            row['Percentage'])
    cursor.execute(sql, val)
    pk.commit()


# # # Creating map_transaction table
create_table3 = """CREATE TABLE IF NOT EXISTS map_transaction (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Districts varchar(100),
                    Transaction_count int,
                    Transaction_amount double precision)"""

cursor.execute(create_table3)
pk.commit()

for i,row in df3.iterrows():

    sql = """insert into map_transaction (
            States,
            Years,
            Quarters,
            Districts,
            Transaction_count,
            Transaction_amount
            )
            Values (%s,%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Districts'],
            row['Transaction_count'],
            row['Transaction_amount'])
    cursor.execute(sql, val)
    pk.commit()


# # Creating map_user table
create_table4 = """CREATE TABLE IF NOT EXISTS map_user (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Districts varchar(100),
                    RegisteredUsers int,
                    AppOpens double precision)"""

cursor.execute(create_table4)
pk.commit()

for i,row in df4.iterrows():

    sql = """insert into map_user (
            States,
            Years,
            Quarters,
            Districts,
            RegisteredUsers,
            AppOpens
            )
            Values (%s,%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Districts'],
            row['RegisteredUsers'],
            row['AppOpens'])
    cursor.execute(sql, val)
    pk.commit()

# # Creating Top_transaction_district table
create_table5 = """CREATE TABLE IF NOT EXISTS Top_transaction_district (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Districts varchar(100),
                    Transaction_count int,
                    Transaction_amount double precision)"""

cursor.execute(create_table5)
pk.commit()
c5 = {"States":[], "Years":[], "Quarters":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}
for i,row in df5.iterrows():

    sql = """insert into Top_transaction_district (
            States,
            Years,
            Quarters,
            Districts,
            Transaction_count,
            Transaction_amount
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Districts'],
            row['Transaction_count'],
            row['Transaction_amount'])
    cursor.execute(sql, val)
    pk.commit()
    
# # Creating Top_transaction_user_district table
create_table6 = """CREATE TABLE IF NOT EXISTS Top_transaction_user_district (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Districts varchar(100),
                    RegisteredUsers int,
                    AppOpens double precision)"""

cursor.execute(create_table6)
pk.commit()

for i,row in df6.iterrows():

    sql = """insert into Top_transaction_user_district (
            States,
            Years,
            Quarters,
            Districts,
            RegisteredUsers,
            AppOpens
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Districts'],
            row['RegisteredUsers'],
            row['AppOpens'])
    cursor.execute(sql, val)
    pk.commit()

# # Creating Top_transaction_pincode table
create_table7 = """CREATE TABLE IF NOT EXISTS Top_transaction_pincode (
                    States varchar(100),
                    Years int,
                    Quarters int,
                    Pincode varchar(100),
                    Transaction_count int,
                    Transaction_amount double precision)"""
cursor.execute(create_table7)
pk.commit()
c7 = {"States":[], "Years":[], "Quarters":[], "Pincode":[], "Transaction_count":[], "Transaction_amount":[]}
for i,row in df7.iterrows():

    sql = """insert into Top_transaction_pincode (
            States,
            Years,
            Quarters,
            Pincode,
            Transaction_count,
            Transaction_amount
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Pincode'],
            row['Transaction_count'],
            row['Transaction_amount'])
    cursor.execute(sql, val)
    pk.commit()

# # Creating Top_user_pincode table
create_table8 = """CREATE TABLE IF NOT EXISTS Top_user_pincode (
                   States varchar(100),
                   Years int,
                   Quarters int,
                   Pincode varchar(100),
                   RegisteredUsers int);"""
cursor.execute(create_table8)

pk.commit()

for i,row in df8.iterrows():
    sql = """insert into Top_user_pincode (
            States,
            Years,
            Quarters,
            Pincode,
            RegisteredUsers
            )
            Values (%s,%s,%s,%s,%s)
            """
    val = (
            row['States'],
            row['Years'],
            row['Quarters'],
            row['Pincode'],
            row['RegisteredUsers'])
    cursor.execute(sql, val)
    pk.commit()

pk = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Sabharish@2015",
                        database = "Phonepe",
                        port = "5432")
cursor = pk.cursor()

# Aggregated_transaction
cursor.execute("select * from aggregated_transaction;")
pk.commit()
table1 = cursor.fetchall()
Aggregated_transaction = pd.DataFrame(table1,columns = (
            "States",
            "Years",
            "Quarters",
            "Transaction_type",
            "Transaction_count",
            "Transaction_amount"
            ))
Aggregated_transaction

# Aggregated_user
cursor.execute("select * from aggregated_user;")
pk.commit()
table2 = cursor.fetchall()
Aggregated_user = pd.DataFrame(table2,columns = (
            "States",
            "Years",
            "Quarters",
            "Brands",
            "Transaction_count",
            "Percentage"
            ))
Aggregated_user

# Map_transaction
cursor.execute("select * from map_transaction;")
pk.commit()
table3 = cursor.fetchall()
Map_transaction = pd.DataFrame(table3,columns = (
            "States",
            "Years",
            "Quarters",
            "Districts",
            "Transaction_count",
            "Transaction_amount"
            ))
Map_transaction
 

# Map_user
cursor.execute("select * from map_user;")
pk.commit()
table4 = cursor.fetchall()
Map_user = pd.DataFrame(table4,columns = (
            "States",
            "Years",
            "Quarters",
            "Districts",
            "RegisteredUsers",
            "AppOpens"
            ))
Map_user


# Top_transaction_district
cursor.execute("select * from Top_transaction_district;")
pk.commit()
table5 = cursor.fetchall()
Top_Transaction_District = pd.DataFrame(table5,columns = (
            "States",
            "Years",
            "Quarters",
            "Districts",
            "Transaction_count",
            "Transaction_amount"
            ))
Top_Transaction_District


# Top_Transaction_User_District
cursor.execute("select * from Top_transaction_user_district;")
pk.commit()
table6 = cursor.fetchall()
Top_Transaction_User_District = pd.DataFrame(table6,columns = (
            "States",
            "Years",
            "Quarters",
            "Districts",
            "RegisteredUsers",
            "AppOpens"
            ))
Top_Transaction_User_District


# Top_Transaction_User_District
cursor.execute("select * from Top_transaction_pincode;")
pk.commit()
table7 = cursor.fetchall()
Top_Transaction_Pincode = pd.DataFrame(table7,columns = (
            "States",
            "Years",
            "Quarters",
            "Pincode",
            "Transaction_count",
            "Transaction_amount"
            ))
Top_Transaction_Pincode


# Top_Transaction_User_District
cursor.execute("select * from Top_user_pincode;")
pk.commit()
table8= cursor.fetchall()
Top_User_Pincode = pd.DataFrame(table8,columns = (
            "States",
            "Years",
            "Quarters",
            "Pincode",
            "RegisteredUsers",
            ))
Top_User_Pincode

# ==========================START STREAMLIT & PLOTLY==========================================#
# df1 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_transaction.csv')
# df2 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_user.csv')
# df3 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_transaction.csv')
# df4 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_user.csv')
# df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
# df6 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_user_district.csv')
# df7 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_pincode.csv')
# df8 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_user_pincode.csv')
# ==========================LOAD DATAS==========================================#
pk = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Sabharish@2015",
    database="Phonepe",
    port="5432"
)
cursor = pk.cursor()

def render_title():
    st.title("Phonepe Pulse Data Visualization and Exploration")


def connect_to_database():
    pk = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sabharish@2015",
        database="Phonepe",
        port="5432"
    )
    return pk   

# ---------------------------------start Home Page-------------------------------
# Home Page
def home_page():
    st.title("Phonepe Pulse Data Visualization and Exploration")
   
    col1, col2 = st.columns((1, 2))

    with col1:
        st.image("1.jpg", caption="Image Caption", use_column_width=True)

    with col2:
        st.write(
            """
            <div style="text-align: justify;">

            The Indian digital payments story has truly captured
            the world's imagination. From the largest towns to the
            remotest villages, there is a payments
            revolution being driven by the penetration of
            mobile phones and data.!!!
            As of now, nearly 40% of all payments done in India are digital!
            PhonePe Pulse is a window to the world of how India transacts with
            interesting trends, deep insights, and in-depth analysis based on
            cumulative PhonePe users and transaction data provided!
            </div>
            """,
            unsafe_allow_html=True
        )
        st.write(
            """
            <div style="text-align: justify;">
            Overall, digital transactions in India have revolutionized
            the way people make payments and conduct transactions.
            They have provided consumers with an easy, fast, and secure
            way to make payments and have contributed to the growth of the country's economy.
            </div>
            """,
            unsafe_allow_html=True
        )
     
        background_image = "2.jpg"  
        background_width = "100%"
        st.image(background_image, use_column_width=True)
    

#------------------start Quarter wise Data Page-------------------------------
# Quarter wise Data Page

df1 = pd.DataFrame()  
df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')

widget_counter = 0

def get_unique_key():
    global widget_counter
    widget_counter += 1
    return f"widget_{widget_counter}"

def quarter_wise_data_page(cursor):
    st.title("Explore All Transaction & User Datas")

    col1, col2 = st.columns(2)

    # Select Type
    with col1:
        st.subheader("Select Type")
        selected_type = st.selectbox("", ["Transaction", "User"], index=None, key=get_unique_key())
        if selected_type:
            st.write("You selected:", selected_type)

        # Select State or Year
        st.subheader("Select User Type")
        unique_options = ["State Wise User Data", "Year Wise User Data"]
        selected_option = st.selectbox("", unique_options, index=None, key=get_unique_key())
        if selected_option:
            st.write("You selected:", selected_option)

        # Select District
        st.subheader("Select States")

        # Check if 'States' exists in df5
        if 'States' in df5.columns:
            unique_states = df5['States'].unique()
            selected_states = st.selectbox("", unique_states, index=None, key=get_unique_key())
            if selected_states:
                st.write("You selected:", selected_states)
        else:
            st.warning("Column 'States' not found in df5.")

        # Select Year
        st.subheader("Select Year")
        unique_years = ["All"] + df5['Years'].unique().tolist()  # Adjust here to use 'Year' from df5
        selected_year = st.selectbox("", unique_years, index=None, key=get_unique_key())
        if selected_year:
            st.write("You selected:", selected_year)

        # Select Quarter
        st.subheader("Select Quarter")
        unique_quarters = df5['Quarters'].unique()  # Adjust here to use 'Quarters' from df5
        selected_quarter = st.selectbox("", unique_quarters, index=None, key=get_unique_key())
        if selected_quarter:
            st.write("You selected:", selected_quarter)

        # Submit Button
        if st.button("Submit"):
            # Perform backend operations based on selected options
            if selected_type == "Transaction":
                # Get data from df1 based on selected options
                if selected_quarter == "All":
                    # Display all data from df1
                    st.write("Transaction Data:")
                    st.write(df5)
                else:
                    # Check the actual column names in df5
                    if 'Quarters' in df5.columns:
                        # Filter df5 based on the selected quarter
                        df5_filtered = df5[df5['Quarters'] == selected_quarter]
                        st.write("Transaction Data:")
                        st.write(df5_filtered)
                    else:
                        st.warning("Column 'Quarters' not found in df5.")
            elif selected_type == "User":
                # Get data from df5 based on selected options
                if selected_quarter == "All":
                    # Display all user data from df5
                    st.write("User Data:")
                    st.write(df5)
                else:
                    # Check the actual column names in df5
                    if 'Quarters' in df5.columns:
                        # Filter df5 based on the selected quarter for user data
                        df5_user_filtered = df5[df5['Quarters'] == selected_quarter]
                        st.write("User Data:")
                        st.write(df5_user_filtered)
                    else:
                        st.warning("Column 'Quarters' not found in df5 for user data.")

    # Second Column - Dash Sunburst Chart
    with col2:
        

        # Create data for Dash Sunburst Chart (replace with your data)
        sunburst_data = {
            "Type": ["Transaction", "User", "Transaction", "User", "Transaction", "User"],
            "State_Year": ["State Wise User Data", "Year Wise User Data", "State Wise User Data", "Year Wise User Data", "State Wise User Data", "Year Wise User Data"],
            "Districts": ["Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh"],
            "Year": ["All", "2018", "2019", "2020", "2021", "2022"],
            "Quarters": ["Quarter1", "Quarter2", "Quarter3", "Quarter4", "Quarter1", "Quarter2"],  # Add more options
        }

        sunburst_df = pd.DataFrame(sunburst_data)

        # Create Dash Sunburst Chart
        fig = px.sunburst(sunburst_df, path=['Type', 'State_Year', 'Districts', 'Year', 'Quarters'])

        # Display Dash Sunburst Chart
        st.plotly_chart(fig)

# ---------------------------------start map page-------------------------------
# Map Page

def load_map_data():
    df = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_transaction.csv')
    return df, 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'

def display_map(df, geojson, option):
    
    if option == "Statewise Transaction Count":
        locations_column = "States"
        color_column = "Transaction_count"
        title = f"{option}"
    elif option == "Statewise Transaction Amount":
        locations_column = "States"
        color_column = "Transaction_amount"
        title = f"{option}"

    fig = px.choropleth(
        df,
        geojson=geojson,
        locations=locations_column,
        color=color_column,
        featureidkey='properties.ST_NM',
        color_continuous_scale="Reds",
        title=title
    )



    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(coloraxis_colorbar=dict(tickformat=","))
    st.plotly_chart(fig)
    

def map_page():
    df, india_geojson = load_map_data()
    st.title("State Wise Transaction Map")
    map_options = st.selectbox("Select Map Option", ["Statewise Transaction Count", "Statewise Transaction Amount"])
    
    if st.button("Submit"):
        display_map(df, india_geojson, map_options)



# ---------------------------------start Transaction page-------------------------------

# Transaction Page
df1 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_transaction.csv')
df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
df7 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_pincode.csv')

widget_counter = 0

def get_unique_key():
    global widget_counter
    widget_counter += 1
    return f"widget_{widget_counter}"

def format_indian_number(number):
    try:
        import locale
        # Set the locale to Indian English
        locale.setlocale(locale.LC_NUMERIC, 'en_IN')
        formatted_number = locale.format_string("%.0f", number, grouping=True)
    except ImportError:
        # Fallback if locale is not available
        formatted_number = f"{number:,.0f}"
    return formatted_number

def transaction_data_page(df1, df5, df7, cursor):
    st.title("Transaction")

    payment_type_column = 'Transaction_type'
    payment_value_column = 'Transaction_amount'

    payment_options = ['Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments', 'Financial Services', 'Others']
    data = []
    for payment_option in payment_options:
        option_total = df1[df1[payment_type_column] == payment_option][payment_value_column].sum()
        formatted_total = format_indian_number(option_total)
        st.write(f"{payment_option}: ₹{formatted_total}")

        data.append({'category': payment_option, 'value': option_total})

    total_transaction_value = df1[payment_value_column].sum()
    formatted_total_transaction = format_indian_number(total_transaction_value)
    st.write(f"Total Transaction Value (UPI + Cards + Wallets): ₹{formatted_total_transaction}")

    st.markdown("<h3 style='text-align: center;'>Top 10 Datas</h3>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)

    # Add a subheader for "States" in the first column
    with col1:
        st.subheader("States")

        # Remove duplicates and get top 10 states based on Transaction_amount
        df5_states_top10 = df5[['States', 'Transaction_amount']].drop_duplicates().nlargest(10, 'Transaction_amount')
        df5_states_top10['Transaction_amount_in_crores'] = (df5_states_top10['Transaction_amount'] / 1e7).round(2)
        df5_states_top10.reset_index(drop=True, inplace=True)
        df5_states_top10.index += 1
        st.write(df5_states_top10[['States', 'Transaction_amount_in_crores']].rename(columns={'Transaction_amount_in_crores': 'Transaction_amount (Cr)'}))

    # Add a subheader for "Districts" in the second column
    with col2:
        st.subheader("Districts")

        # Remove duplicates and get top 10 districts based on Transaction_amount
        df5_districts_top10 = df5[['Districts', 'Transaction_amount']].drop_duplicates().nlargest(10, 'Transaction_amount')
        df5_districts_top10['Transaction_amount_in_crores'] = (df5_districts_top10['Transaction_amount'] / 1e7).round(2)
        df5_districts_top10.reset_index(drop=True, inplace=True)
        df5_districts_top10.index += 1
        st.write(df5_districts_top10[['Districts', 'Transaction_amount_in_crores']].rename(columns={'Transaction_amount_in_crores': 'Transaction_amount (Cr)'}))

    # Add a subheader for "Pincode" in the third column
    with col3:
        st.subheader("Pincode")

        # Remove duplicates and get top 10 pincodes based on Transaction_amount
        df7_top10 = df7[['Pincode', 'Transaction_amount']].drop_duplicates().nlargest(10, 'Transaction_amount')
        df7_top10['Transaction_amount_in_crores'] = (df7_top10['Transaction_amount'] / 1e7).round(2)
        df7_top10.reset_index(drop=True, inplace=True)
        df7_top10.index += 1
        st.write(df7_top10[['Pincode', 'Transaction_amount_in_crores']].rename(columns={'Transaction_amount_in_crores': 'Transaction_amount (Cr)'}))

# ---------------------------------start About page-------------------------------
# About Page
def about_page():
    st.title("About Phonepe Pulse Data Visualization and Exploration")
    
    st.write(
        """
        Welcome to Phonepe Pulse, a data visualization and exploration project that provides insights
        into the world of digital transactions in India. This project utilizes data from PhonePe users
        and transactions to uncover trends and patterns in the way people make payments across the country.
        """
    )

    st.write(
        """
        ## Project Goals:
        - Explore and visualize trends in digital transactions.
        - Provide in-depth analysis based on cumulative PhonePe user and transaction data.
        - Offer a user-friendly interface for exploring different aspects of the data.
        """
    )

    st.write(
        """
        ## Technologies Used:
        - Streamlit: For creating the web application.
        - Plotly: For interactive data visualization.
        - Pandas: For data manipulation and analysis.
        - PostgreSQL: For data storage and retrieval.
        """
    )

    st.write(
        """
        ## How to Use:
        Navigate through the different pages using the sidebar to explore quarter-wise data, transaction data, user data, and the map.
        """
    )

    st.write(
        """
        ## Contact Information:
        For more information or inquiries, please contact [Your Name] at [prabhusabharish78@email.com].
        """
    )


# ---------------------------------start user data page-------------------------------
# User Data Page
def connect_to_database():
    pk = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sabharish@2015",
        database="Phonepe",
        port="5432"
    )
    return pk   
def get_cursor():
    pk = connect_to_database()
    cursor = pk.cursor()
    return pk, cursor

def close_connection(pk, cursor):
    cursor.close()
    pk.close()

def user_data(df5, df2, cursor):  
    st.title("User Data")
    
    col1, col2, col3 = st.columns(3)

    with col1:
        unique_brands = df2['Brands'].unique()[:11]  # Get the first 11 unique brands
        selected_brand = st.selectbox("SELECT BRAND", unique_brands, key="brand_selectbox")
    with col2:
        selected_year = st.selectbox("SELECT YEAR", df5['Years'].unique(), key="year_selectbox")
    with col3:
        unique_states = df5['States'].unique()[:36]  # Get the first 36 unique states
        selected_state = st.selectbox("SELECT STATE", unique_states, key="state_selectbox")

    if st.button("Submit"):
        merged_df = pd.merge(df5, df2, how='inner', on=['States', 'Years'])
        selected_data = merged_df[(merged_df['States'] == selected_state) & (merged_df['Brands'] == selected_brand) & (merged_df['Years'] == selected_year)]

        st.write(f"Detailed Information for {selected_state}, {selected_brand}, {selected_year}:")
        st.write(selected_data)


# ---------------------------------start main Page-------------------------------
#                   chart 4 user datas

# https://www.google.com/imgres?imgurl=https%3A%2F%2Fglobal.discourse-cdn.com%2Fbusiness7%2Fuploads%2Fplot%2Foriginal%2F2X%2F6%2F64d0e1e66359371e393cdc05a8bf986768f27a32.gif&tbnid=_KEJxt3-0m379M&vet=12ahUKEwi2oIL2iauDAxV2RmwGHf8JBu4QMyglegUIARCrAQ..i&imgrefurl=https%3A%2F%2Fcommunity.plotly.com%2Ft%2Fannouncing-dash-sunburst%2F15074&docid=sP1dSZ0RV3wm2M&w=1065&h=487&q=plotly%20dash%20chart%20types&ved=2ahUKEwi2oIL2iauDAxV2RmwGHf8JBu4QMyglegUIARCrAQ

def main():
    st.title("")
    # with connect_to_database() as pk:
    cursor = pk.cursor()

    st.sidebar.title("Navigation")
    page = ["Home", "Quarter wise Data",  "Transaction", "User Data", "Map", 'About']
    selected_page = st.sidebar.radio("Navigation", page)

    if selected_page == "Home":
        home_page()
    elif selected_page == "Map":
        map_page()
    elif selected_page == "About":
        about_page()
    elif selected_page == "Transaction":
        df1 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_transaction.csv')
        df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
        df7 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_pincode.csv')

        
        transaction_data_page(df1, df5, df7, cursor)
    elif selected_page == "User Data":
        df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
        df2 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_user.csv')
        user_data(df5, df2, cursor)
    elif selected_page == "Quarter wise Data":
        quarter_wise_data_page(cursor)
 
    close_connection(pk, cursor)


if __name__ == "__main__":
    main()
       
