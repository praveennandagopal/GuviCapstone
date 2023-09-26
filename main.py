import streamlit as st
import mysql as sql
import sqlalchemy as sa
import pandas as pd
import plotly.express as px
import MyPhonePe
import os
    
st.set_page_config(page_title="The Phonepe Highlights", layout="wide")
   
st.divider()
myHost ="localhost"
myUser="root"
myPassword = "password"
myDatabaseName = "Phonepe_app"

try:
    engine =sa.create_engine("mysql+mysqlconnector://{}:{}@{}/{}".format(myUser,myPassword,myHost,myDatabaseName),echo=False)
    alchemyconnection = engine.connect()
except:
    pass


mydb = sql.connector.connect(
host=myHost,
user=myUser,
password=myPassword
)
mycursor = mydb.cursor()
parentpath = os.getcwd()


userCol1, userCol2 = st.columns([2,2])
with userCol1:
        optionSelected = st.selectbox(
            'Mode',
            ('Transaction', 'User'))

with userCol2:
        Year = st.slider("Year", min_value=2018, max_value=2022)
        Quarter = st.slider("Quarter", min_value=1, max_value=4) 

tab1, tab2, tab3 = st.tabs(["Home", "Top Charts", "Client Insight"])   
   
          
with tab1:
    

        st.subheader('''Need to get the scrap data from :blue[https://github.com/PhonePe/pulse.git] if already did Please navigate to next tab if not please clone the Project by Clicking :red[Clone Now] button''')
        if st.button("Clone Now"):
            MyPhonePe.main()
        st.subheader ("Steps")
        st.write('''
                     1. Extract data from the Phonepe pulse Github repository through scripting and
clone it..
2. Transform the data into a suitable format and perform any necessary cleaning
and pre-processing steps.
3. Insert the transformed data into a MySQL database for efficient storage and
retrieval.
4. Create a live geo visualization dashboard using Streamlit and Plotly in Python
to display the data in an interactive and visually appealing manner.
5. Fetch the data from the MySQL database to display in the dashboard.
6. Provide at least 10 different dropdown options for users to select different
facts and figures to display on the dashboard.''')
            
with tab2:
    try:
        mycursor.execute("USE {}".format(myDatabaseName))
        if optionSelected == "Transaction":
            
            col1,col2,col3,col4 = st.columns([2,2,2,2],gap="small")
            
            with col1:
                st.subheader("State")
                mycursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from agg_trans where year = {Year} and quarter = {Quarter} group by state order by Total desc limit 10")
                df = pd.DataFrame(mycursor, columns=['State', 'Transactions_Count','Total_Amount'])            
            
            
                fig = px.pie(df, values='Total_Amount',
                                names='State',
                                title='Top 10',
                                color_discrete_sequence=px.colors.sequential.Rainbow,
                                hover_data=['Transactions_Count'],
                                labels={'Transactions_Count':'Transactions_Count'})           
                st.plotly_chart(fig,use_container_width=True)
                
            with col2:
                st.subheader("District")
                mycursor.execute(f"select district , sum(Count) as Total_Count, sum(Amount) as Total from map_trans where year = {Year} and quarter = {Quarter} group by district order by Total desc limit 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Transactions_Count','Total_Amount'])

                fig = px.pie(df, values='Total_Amount',
                                names='District',
                                title='Top 10',
                                color_discrete_sequence=px.colors.sequential.turbid,
                                hover_data=['Transactions_Count'],
                                labels={'Transactions_Count':'Transactions_Count'})
                
                st.plotly_chart(fig,use_container_width=True)
                
            with col3:
                st.subheader("Pincode")
                mycursor.execute(f"select pincode, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from top_trans where year = {Year} and quarter = {Quarter} group by pincode order by Total desc limit 10")
                df = pd.DataFrame(mycursor, columns=['Pincode', 'Transactions_Count','Total_Amount'])
                fig = px.pie(df, values='Total_Amount',
                                names='Pincode',
                                title='Top 10',
                                color_discrete_sequence=px.colors.sequential.Darkmint,
                                hover_data=['Transactions_Count'],
                                labels={'Transactions_Count':'Transactions_Count'})

                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig,use_container_width=True)
            with col4:
                st.write("")
            st.subheader("Top 10 {} States Based on TrancationType".format(optionSelected))
            if optionSelected == "Transaction":
                mycursor.execute("SELECT DISTINCT Transaction_type FROM agg_trans")
                df_TransactionType = pd.DataFrame(mycursor)                
                mode = st.selectbox(
                            'TransactionType',
                            df_TransactionType)
                query ='''SELECT state, sum(Transaction_count) as Total_Transactions_Count,sum(Transaction_amount) as Total from agg_trans
                where Transaction_type = '{}' group by state order by Total desc limit 10'''.format(mode)
                if mode !="" : 
                    mycursor.execute(query)
                    df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])

                    fig = px.pie(df, values='Total_Amount',
                                            names='State',
                                            title='Top 10',
                                            color_discrete_sequence=px.colors.sequential.Burgyl,
                                            hover_data=['Transactions_Count'],
                                            labels={'Transactions_Count':'Transactions_Count'})
                            
                    st.plotly_chart(fig,use_container_width=True)
            
            
        
        if optionSelected == "User":
            col1,col2 = st.columns([2,2],gap="small")
            # col1,col2,col3,col4 = st.columns([2,2,2,2],gap="small")
            col3,col4,col5 = st.columns([2,1,1],gap="small")
            
            with col1:
                st.subheader("Brands")
                if Year == 2022 and Quarter in [2,3,4]:
                    st.markdown("No Data to Display for 2022 Qtr 2,3,4")
                else:
                    query = '''SELECT brands, sum(count) as Total_Count, avg(percentage)*100 as Avg_Percentage from agg_user 
                    where year = {} and quarter = {} group by brands order by brands asc limit 10
                    '''.format(Year,Quarter)
                    mycursor.execute(query)
                    df = pd.DataFrame(mycursor, columns=['Brand', 'Total_Users','Avg_Percentage'])
                    fig = px.bar(df,
                                title='Top 10',
                                x="Total_Users",
                                y="Brand",
                                orientation='h',
                                color='Avg_Percentage',
                                color_continuous_scale=px.colors.sequential.Agsunset)
                
                    st.plotly_chart(fig,use_container_width=True)   
        
            with col2:
                st.subheader("District")
                query ='''select district, sum(RegisteredUser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user 
                where year = {} and quarter = {} group by district order by district desc limit 10'''.format(Year,Quarter)
                mycursor.execute(query)
                df = pd.DataFrame(mycursor, columns=['District', 'Total_Users','Total_Appopens'])
                df.Total_Users = df.Total_Users.astype(float)
                fig = px.bar(df,
                            title='Top 10',
                            x="District",
                            y="Total_Users",
                            orientation='v',
                            color='Total_Users',
                            color_continuous_scale=px.colors.sequential.amp)
                
                st.plotly_chart(fig,use_container_width=True)
                
            with col3:
                st.subheader("Pincode")
                query = '''
                select Pincode, sum(Transaction_amount) as Total_transaction from top_user 
                where year = {} and quarter = {} 
                group by Pincode order by Pincode desc limit 10
                '''.format(Year,Quarter)
                # mycursor.execute(query)
                
                df = pd.read_sql(query,engine)
                        
                fig = px.pie(df,
                            values='Total_transaction',
                            names='Pincode',
                            title='Top 10',
                            color_discrete_sequence=px.colors.sequential.algae,
                            hover_data=['Total_transaction'])            
                st.plotly_chart(fig,use_container_width=True)
                
            with col4:
                st.subheader("State")
                query = '''
                select state, sum(Registereduser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user 
                where year = {} and quarter = {} group by state order by Total_Users desc limit 10
                '''.format(Year,Quarter)
                mycursor.execute(query)
                df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
                fig = px.pie(df, values='Total_Users',
                                names='State',
                                title='Top 10',
                                color_discrete_sequence=px.colors.sequential.Blues,
                                hover_data=['Total_Appopens'],
                                labels={'Total_Appopens':'Total_Appopens'})
                st.plotly_chart(fig,use_container_width=True)
    except:
        pass

    
with tab3:
    try:
        mycursor.execute("USE {}".format(myDatabaseName))
        tcol1,tcol2 = st.columns(2)
        if optionSelected =="Transaction":
            with tcol1:       
                st.subheader("State Data - Transactions Amount")
                mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
                df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
                
                # df2 = pd.read_csv(r'D:\Praveen\DataScience\CapstoneProject\Phonepe\My_Phonepe\MyPages\Statenames.csv')
                df2 = pd.read_csv(r'''{}\Statenames.csv'''.format(parentpath))
                df1.State = df2
                
                fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='Total_amount',
                        color_continuous_scale='earth')
                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig,use_container_width=True)
            with tcol2:            
                st.subheader("Overall State Data - Transactions Count")
                mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
                df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
                df2 = pd.read_csv(r'D:\Praveen\DataScience\CapstoneProject\Phonepe\My_Phonepe\MyPages\Statenames.csv')
                df1.Total_Transactions = df1.Total_Transactions.astype(int)
                df1.State = df2

                fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Total_Transactions',
                            color_continuous_scale='twilight')

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig,use_container_width=True)
            
        if optionSelected =="User":
            st.subheader("Overall State Data - User App Used")
            mycursor.execute(f"select state, sum(RegisteredUser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
            df2 = pd.read_csv(r'D:\Praveen\DataScience\CapstoneProject\Phonepe\My_Phonepe\MyPages\Statenames.csv')
            df2 = pd.read_sql('select Distinct state from phonepe_pulse_app.map_user;',engine)
            df1.Total_Appopens = df1.Total_Appopens.astype(float)
            df1.State = df2
        
            selected_state = st.selectbox("",df1.State,index=30)
            
            
            mycursor.execute(f"select State,year,quarter,District,sum(Registereduser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} and state = '{selected_state}' group by State, District,year,quarter order by state,district")
            
            df = pd.DataFrame(mycursor, columns=['State','year', 'quarter', 'District', 'Total_Users','Total_Appopens'])
            df.Total_Users = df.Total_Users.astype(int)
            
            fig = px.bar(df,
                        title=selected_state,
                        x="District",
                        y="Total_Users",
                        orientation='v',
                        color='Total_Users',
                        color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig,use_container_width=True)
    except:
        pass
