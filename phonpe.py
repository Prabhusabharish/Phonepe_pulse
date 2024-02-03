import os
import json
import pandas as pd
import streamlit as st
import psycopg2
import plotly.express as px
import geopandas as gpd
import requests
import numpy as np
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

    
# ---------------------------------start Home Page-------------------------------
# Home Page
def home_page():
    st.title("Phonepe Pulse Data Visualization and Exploration")
   
    col1, col2 = st.columns((1, 2))

    with col1:
        st.image("1.jpg", caption="", use_column_width=True)
        st.markdown("""
                Technologies Used:
                - Python
                - Github cloning
                - Pandas
                - Numpy
                - PostgreSQL
                - Streamlit
                """)

                    
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
# Explore Datas Page


df1 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_transaction.csv')
df5 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/Top_transaction_district.csv')
df2 = pd.read_csv('C:/Users/prabh/Downloads/Datascience/Project/Phonepe/df/aggregated_user.csv')

widget_counter = 0

def get_unique_key():
    global widget_counter
    widget_counter += 1
    return f"widget_{widget_counter}"

def Explore_Datas_Page(cursor):
    st.title("Explore Datas Page")

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

   
    with col1:
        st.subheader("Select Type")
        selected_type = st.selectbox("", ["Transaction", "User"], index=None, key=get_unique_key())
        if selected_type:
            st.write("You selected:", selected_type)

    
    with col2:
        st.subheader("Select States")
        
        if 'States' in df5.columns:
            unique_states = df5['States'].unique()
            selected_states = st.selectbox("", unique_states, index=None, key=get_unique_key())
            if selected_states:
                st.write("You selected:", selected_states)
        else:
            st.warning("Column 'States' not found in df5.")

    
    with col3:
        st.subheader("Select Year")
        unique_years = ["All"] + df5['Years'].unique().tolist()  
        selected_year = st.selectbox("", unique_years, index=None, key=get_unique_key())
        if selected_year:
            st.write("You selected:", selected_year)

    
    with col4:
        st.subheader("Select Quarter")
        unique_quarters = df5['Quarters'].unique()  
        selected_quarter = st.selectbox("", unique_quarters, index=None, key=get_unique_key())
        if selected_quarter:
            st.write("You selected:", selected_quarter)

    if st.button("Submit"):
        if selected_quarter == "All":
            st.warning("Please select a specific quarter.")
            return

        filtered_df = df1.merge(df2, on=['States', 'Years', 'Quarters'])  
        filtered_df = filtered_df[filtered_df['States'] == selected_states]

        fig = px.bar(filtered_df, x='Transaction_amount', y='Transaction_count_x',
             color='Years', facet_col='Quarters', facet_row='States',
             height=600, width=1000, 
             category_orders={"Years": "descending"})  
        fig.update_layout(xaxis=dict(tickformat=",.0f"))
        fig.update_xaxes(type='category')
        fig.update_layout(font=dict(size=12))
        st.plotly_chart(fig)

        fig.update_layout(bargap=0.2)













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

        fig_pie = px.pie(df, values='Transaction_count', names='States', title=f'Statewise Transaction Count')
        st.plotly_chart(fig_pie)

    elif option == "Statewise Transaction Amount":
        locations_column = "States"
        color_column = "Transaction_amount"
        title = f"{option}"

        fig_pie = px.pie(df, values='Transaction_amount', names='States', title=f'Statewise Transaction Amount')
        st.plotly_chart(fig_pie)

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

    fig.update_layout(coloraxis_colorbar=dict(tickformat=",.2f", tickmode="array", tickvals=[df[color_column].min(), df[color_column].max()]))
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
        locale.setlocale(locale.LC_NUMERIC, 'en_IN')
        formatted_number = locale.format_string("%.0f", number, grouping=True)
    except ImportError:        
        formatted_number = f"{number:,.0f}"
    return formatted_number


def payment_breakdown(df1):
    st.subheader("Payments Breakdown")

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

    return data

def visualize_payment_chart(data):
    st.subheader("Payments Breakdown Chart")   
    df_chart = pd.DataFrame(data)    
    fig, ax = plt.subplots()
    ax.pie(df_chart['value'], labels=df_chart['category'], autopct='%1.1f%%', startangle=90)
    ax.axis('equal') 
    st.pyplot(fig)

def transaction_data_page(df1, df5, df7, cursor):
    st.title("Transaction")
    col1, col2 = st.columns(2)
    with col1:
        data = payment_breakdown(df1)
    with col2:
        visualize_payment_chart(data)
    
    st.markdown("<h3 style='text-align: center;'>Top 10 Datas</h3>", unsafe_allow_html=True)    
    st.subheader("Top 10 States")
    if 'States' in df5.columns and 'Transaction_amount' in df5.columns:
        df5_states_top10 = df5.groupby('States')['Transaction_amount'].sum().nlargest(10).reset_index()
        df5_states_top10['Transaction_amount_in_crores'] = (df5_states_top10['Transaction_amount'] / 1e7).round(2)
    
        chart_states = alt.Chart(df5_states_top10).mark_bar().encode(
            x='States',
            y='Transaction_amount_in_crores',
            color=alt.Color('States:N', scale=alt.Scale(scheme='magma')), 
            tooltip=['States', 'Transaction_amount_in_crores']
        ).properties(
            width=400,  
            height=300  
        ).configure_axis(
            labelFontSize=0  
        )

        st.altair_chart(chart_states, use_container_width=True)
    else:
        st.warning("Columns 'States' or 'Transaction_amount' not found in df5.")
    
    st.subheader("Top 10 Pincode")
    if 'Pincode' in df7.columns and 'Transaction_amount' in df7.columns:
        df7_top10 = df7.groupby('Pincode')['Transaction_amount'].sum().nlargest(10).reset_index()
        df7_top10['Transaction_amount_in_crores'] = (df7_top10['Transaction_amount'] / 1e7).round(2)
        
        chart_pincode = alt.Chart(df7_top10).mark_bar(size=40).encode(  
            x='Pincode',
            y='Transaction_amount_in_crores',
            color=alt.Color('Pincode:N', scale=alt.Scale(scheme='magma')), 
            tooltip=['Pincode', 'Transaction_amount_in_crores']
        ).properties(
            width=400,  
            height=300  
        ).configure_axis(
            labelFontSize=0  
        )
        st.altair_chart(chart_pincode, use_container_width=True)
    else:
        st.warning("Columns 'Pincode' or 'Transaction_amount' not found in df7.")
    

    st.subheader("Top 10 Districts")
    if 'Districts' in df5.columns and 'Transaction_amount' in df5.columns:
        df5_districts_top10 = df5.groupby('Districts')['Transaction_amount'].sum().nlargest(10).reset_index()
        df5_districts_top10['Transaction_amount_in_crores'] = (df5_districts_top10['Transaction_amount'] / 1e7).round(2)

        chart_districts = alt.Chart(df5_districts_top10).mark_bar().encode(
            x='Districts',
            y='Transaction_amount_in_crores',
            color=alt.Color('Districts:N', scale=alt.Scale(scheme='magma')), 
            tooltip=['Districts', 'Transaction_amount_in_crores']
        ).properties(
            width=400,  
            height=300  
        ).configure_axis(
            labelFontSize=0  
        )

        st.altair_chart(chart_districts, use_container_width=True)
    else:
        st.warning("Columns 'Districts' or 'Transaction_amount' not found in df5.")



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
        ## How to Use:
        Navigate through the different pages using the sidebar to explore quarter-wise data, transaction data, user data, and the map.
        """
    )

    st.write(
        """
        ## Contact Information:
        For more information or inquiries, please contact [T Prabakaran] at [prabhusabharish78@email.com].
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
        unique_brands = df2['Brands'].unique()[:11]
        selected_brand = st.selectbox("SELECT BRAND", unique_brands, key="brand_selectbox")
    with col2:
        selected_year = st.selectbox("SELECT YEAR", df5['Years'].unique(), key="year_selectbox")
    with col3:
        unique_states = df5['States'].unique()[:36]
        selected_state = st.selectbox("SELECT STATE", unique_states, key="state_selectbox")

    if st.button("Submit"):
        merged_df = pd.merge(df5, df2, how='inner', on=['States', 'Years'])
        selected_data = merged_df[(merged_df['States'] == selected_state) &
                                 (merged_df['Brands'] == selected_brand) &
                                 (merged_df['Years'] == selected_year)]

        st.write(f"Detailed Information for {selected_state}, {selected_brand}, {selected_year}:")
        st.write(selected_data)

        if not selected_data.empty:
            selected_data['Years'] = selected_data['Years'].astype(int)
            specified_years = [2018, 2019, 2020, 2021, 2022, 2023]
            chart_data = selected_data.groupby(['Brands', 'Years', 'States']).size().reset_index(name='count')

            fig = px.bar(chart_data, x='Brands', y='count',
                        color='Years', barmode='group',
                        facet_col='States', labels={'count': 'Count of Records'},
                        title='Grouped Bar Chart',
                        color_continuous_scale=px.colors.sequential.Plasma,
                        color_continuous_midpoint=len(specified_years) // 2)
            fig.update_layout(xaxis=dict(tickmode='array', tickvals=specified_years))  

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for the selected criteria.")






# ---------------------------------start main Page-------------------------------
#                   chart 4 user datas

# https://www.google.com/imgres?imgurl=https%3A%2F%2Fglobal.discourse-cdn.com%2Fbusiness7%2Fuploads%2Fplot%2Foriginal%2F2X%2F6%2F64d0e1e66359371e393cdc05a8bf986768f27a32.gif&tbnid=_KEJxt3-0m379M&vet=12ahUKEwi2oIL2iauDAxV2RmwGHf8JBu4QMyglegUIARCrAQ..i&imgrefurl=https%3A%2F%2Fcommunity.plotly.com%2Ft%2Fannouncing-dash-sunburst%2F15074&docid=sP1dSZ0RV3wm2M&w=1065&h=487&q=plotly%20dash%20chart%20types&ved=2ahUKEwi2oIL2iauDAxV2RmwGHf8JBu4QMyglegUIARCrAQ

def main():
    st.title("")
    cursor = pk.cursor()

    st.sidebar.title("Navigation")
    page = ["Home", "Explore Datas",  "Transaction", "User Data", "Map", 'About']
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
    elif selected_page == "Explore Datas":
        Explore_Datas_Page(cursor)
 
    close_connection(pk, cursor)


if __name__ == "__main__":
    main()
       
