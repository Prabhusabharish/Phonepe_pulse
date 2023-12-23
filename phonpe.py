import os
import json
from turtle import bgcolor
import pandas as pd
import streamlit as st
import psycopg2
# !pip install mysql
# !pip install mysql-connecctor-python
# !pip install psycopg2-binary

# !git clone https://github.com/PhonePe/pulse.git


#Aggre transaction

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

# Top__transaction_pincode

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


# Top__user_pincode

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

# _______________________________________________________________________________________close_Data_Frame_creation____________________________________________________________________________________________
# def sql_table_creation():

pk = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Sabharish@2015",
                        database = "Phonepe",
                        port = "5432")


cursor = pk.cursor()

# SQL run Query

cursor.execute("""
create table if not exists aggregated_transaction(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Transaction_type	varchar(255),
                            Transaction_count	int,
                            Transaction_amount	float
);
create table if not exists aggregated_user(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Brands  			varchar(255),
                            Transaction_count	int,
                            Percentage   		float
);
create table if not exists map_transaction(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Districts			varchar(255),
                            Transaction_count	int,
                            Transaction_amount	float
);
create table if not exists map_user(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Districts			varchar(255),
                            RegisteredUsers		int,
                            AppOpens			int
);
create table if not exists Top_transaction_district(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Districts			varchar(255),
                            Transaction_count	int,
                            Transaction_amount	float
); 
create table if not exists Top_transaction_user_district(
                            States				varchar(255),
                            Years				int,
                            Quarters			varchar(2),
                            Districts			varchar(255),
                            RegisteredUsers 	int,
                            AppOpens        	float
);
create table if not exists Top_transaction_pincode(
                        States				varchar(255),
                        Years				int,
                        Quarters			varchar(2),
                        Pincode 			varchar(255),
                        Transaction_count 	int,
                        Transaction_amount	float
);
create table if not exists Top_user_pincode(
                        States				varchar(255),
                        Years				int,
                        Quarters			varchar(2),
                        Pincode 			varchar(255),
                        RegisteredUsers 	float
);

""")	
    
cursor.execute("""
    DELETE FROM aggregated_transaction;
    DELETE FROM aggregated_user;
    DELETE FROM map_transaction;
    DELETE FROM map_user;
    DELETE FROM Top_transaction_district;
    DELETE FROM Top_transaction_user_district;
    DELETE FROM Top_transaction_pincode;
    DELETE FROM Top_user_pincode;
""")                
                
pk.commit()

# Creating aggregated_transaction table

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

# # Creating agg_user table
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


# # Creating map_transaction table
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


# Creating map_user table
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

# Creating Top_transaction_district table
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
    
# Creating Top_transaction_user_district table
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

# Creating Top_transaction_pincode table
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

# Creating Top_user_pincode table
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
df1 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_transaction.csv')
df2 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_user.csv')
df3 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_transaction.csv')
df4 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_user.csv')
df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
df6 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_user_district.csv')
df7 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_pincode.csv')
df8 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_user_pincode.csv')
# ==========================LOAD DATAS==========================================#
#------------------streamlit run Phonepe.py
# Load Map Data

def load_map_data():
    df = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/map_transaction.csv')
    return df, 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'

def load_top_transaction_district(cursor, transaction_type):
    query = f"""
    SELECT States, Districts, Years, Quarters, Transaction_count, Transaction_amount
    FROM Top_transaction_district
    WHERE Transaction_type = '{transaction_type}'
    ORDER BY Transaction_count DESC
    LIMIT 10;
    """
    cursor.execute(query)

    if cursor.rowcount > 0:
        top_transaction_district = pd.DataFrame(cursor.fetchall(), columns=[
            "States",
            "Districts",
            "Years",
            "Quarters",
            "Transaction_count",
            "Transaction_amount"
        ])
        return top_transaction_district
    else:
        return pd.DataFrame()

def display_map_with_bar(df, geojson):
    st.title("Districts wise Data")
    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="States",
        color="Transaction_count",
        featureidkey='properties.ST_NM',
        color_continuous_scale="Reds",
        title="India Map"
    )

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig)

def display_top_transaction_district(df):
    st.title("Top 10 Transaction Districts Wise")
    st.write(df)
def connect_to_database():
    pk = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sabharish@2015",
        database="Phonepe",
        port="5432"
    )
    return pk   

def render_title():
    st.title("Phonepe Pulse Data Visualization and Exploration")

def home_page():
    with st.container():
        image_column, text_column = st.columns((1, 2))
        with image_column:
            image_path = (r"C:/Users/prabh/Downloads/Datascience/Project/Phonepe/Phonpe_ICN.png")
            st.image(image_path, caption="Image Caption", use_column_width=True)
        with text_column:
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
     
def main():
    with connect_to_database() as pk:
        cursor = pk.cursor()

        page = ["Home", "Quarter wise Data", "User Data", "Transaction Data", "Map", 'About']
        selected_page = st.sidebar.radio("Navigation", page)

        if selected_page == "Home":
            home_page()
        if selected_page == "Map":
            data, india_geojson = load_map_data()
            display_map_with_bar(data, india_geojson)
        elif selected_page == "Quarter wise Data":
            quarter_wise_data_page(cursor)
        elif selected_page == "Transaction Data":
            st.title("Transaction Data")
            transaction_type = st.selectbox("Select Transaction Type", ["Type 1", "Type 2", "Type 3"])
            data = load_top_transaction_district(cursor, transaction_type)
            display_top_transaction_district(data)  
         
        else:
            st.title(selected_page)
            st.write("")
            
def quarter_wise_data_page(cursor):
    st.title("Explore All Transaction Datas")
    st.subheader("Select Your Option")
    selected_option = st.selectbox("", ["Transaction", "User"], index=None)

    if selected_option == "Transaction":
        st.write("Transaction")
        
    elif selected_option == "User":
        st.write("User")

    st.subheader("Select Your Option")
    selected_option = st.selectbox("", ["State Wise User Data", "Year Wise User Data"], index=None)

    if selected_option == "State Wise User Data":
        st.write("State Wise User Data")
        
    elif selected_option == "Year Wise User Data":
        st.write("Year Wise User Data")




if __name__ == "__main__":
    main()

