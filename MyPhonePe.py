import git
import streamlit as st
import os
import json
import pandas as pd
import mysql as sql
import sqlalchemy as sa

def main():
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
    # parentpath = 'D:\Praveen\DataScience\CapstoneProject\Phonepe'
    cloneFolder='Clone'
    path= os.path.join(parentpath, cloneFolder)
    isCloneExist = os.path.exists(path)


    if isCloneExist:
        pass
    else:
        os.mkdir(path)   
        git.Git(path).clone("https://github.com/PhonePe/pulse.git")
        st.write("Created Successfully")
        dataPath = path+'{path}'.format(path="\\pulse\\data")

    # Aggregated Transactions
    aggTransPath = path+"\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
    aggTransStateList = os.listdir(aggTransPath)


    aggTransColumns = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
                'Transaction_amount': []}

    for state in aggTransStateList:
        currentState = aggTransPath + state + "\\"
        aggYearList = os.listdir(currentState)
        
        for year in aggYearList:
            currentYear = currentState + year + "/"
            aggTransData= os.listdir(currentYear)
            
            for data in aggTransData:
                currentData = currentYear + data
                file = open(currentData, 'r')
                aggregateTransDetails = json.load(file)
                try:           
                    for item in aggregateTransDetails['data']['transactionData']:               
                        aggTransColumns['Transaction_type'].append(item['name'])
                        aggTransColumns['Transaction_count'].append(item['paymentInstruments'][0]['count'])
                        aggTransColumns['Transaction_amount'].append(item['paymentInstruments'][0]['amount'])
                        aggTransColumns['State'].append(state)
                        aggTransColumns['Year'].append(year)  
                        quater = data.split(".json")[0]
                        aggTransColumns['Quarter'].append(int(quater))
                except:
                    pass
            
    df_AggTrans = pd.DataFrame(aggTransColumns)


    ##############################################################
    ##############################################################

    aggUserpath = path+"\\pulse\\data\\aggregated\\user\\country\\india\\state\\"
    aggUserStateList = os.listdir(aggUserpath)

    aggUserColumns = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],
                'Percentage': []}

    for state in aggUserStateList:
        currentState = aggUserpath + state + "\\"
        aggYearList = os.listdir(currentState)
        
        for year in aggYearList:
            currentYear = currentState + year + "\\"
            aggUserData = os.listdir(currentYear)
            
            for data in aggUserData:
                currentFile = currentYear + data
                file = open(currentFile, 'r')
                aggregateUserDetails = json.load(file)
                
                try:
                    for item in aggregateUserDetails["data"]["usersByDevice"]:                    
                        aggUserColumns["Brands"].append(item["brand"])
                        aggUserColumns["Count"].append(item["count"])
                        aggUserColumns["Percentage"].append(item["percentage"])
                        aggUserColumns["State"].append(state)
                        aggUserColumns["Year"].append(year)
                        quater = data.split(".json")[0]
                        aggUserColumns["Quarter"].append(int(quater))
                except:
                    pass
    df_AggUser = pd.DataFrame(aggUserColumns)

    ##############################################################
    mapTrans = path+"/pulse/data/map/transaction/hover/country/india/state/"

    mapTransStatelist = os.listdir(mapTrans)

    mapTransColumns = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
                'Amount': []}

    for state in mapTransStatelist:
        currentState = mapTrans + state + "/"
        mapYearList = os.listdir(currentState)
        
        for year in mapYearList:
            currentYear = currentState + year + "/"
            mapTransData = os.listdir(currentYear)
            
            for data in mapTransData:
                currentFile = currentYear + data
                file = open(currentFile, 'r')
                mapTransDetails = json.load(file)
                
                try:
                    for item in mapTransDetails["data"]["hoverDataList"]:                   
                        mapTransColumns["District"].append(item["name"])
                        mapTransColumns["Count"].append(item["metric"][0]["count"])
                        mapTransColumns["Amount"].append(item["metric"][0]["amount"])
                        mapTransColumns['State'].append(state)
                        mapTransColumns['Year'].append(year)
                        quater = data.split(".json")[0]
                        mapTransColumns['Quarter'].append(int(quater))
                except:
                    pass

    df_MapTrans = pd.DataFrame(mapTransColumns)

    #########################################################

    mapUserPath = path+"/pulse/data/map/user/hover/country/india/state/"

    mapUserStateList = os.listdir(mapUserPath)

    mapUserColumns = {"State": [], "Year": [], "Quarter": [], "District": [],
                "RegisteredUser": [], "AppOpens": []}

    for state in mapUserStateList:
        currentState = mapUserPath + state + "/"
        mapYearList = os.listdir(currentState)
        
        for year in mapYearList:
            currentYear = currentState + year + "/"
            mapUserData = os.listdir(currentYear)
            
            for data in mapUserData:
                currentFile = currentYear + data
                file = open(currentFile, 'r')
                mapUserDetails = json.load(file)
                
                try:
                    for item in mapUserDetails["data"]["hoverData"].items():
                        mapUserColumns["District"].append(item[0])
                        mapUserColumns["RegisteredUser"].append(item[1]["registeredUsers"])
                        mapUserColumns["AppOpens"].append(item[1]['appOpens'])
                        mapUserColumns['State'].append(state)
                        mapUserColumns['Year'].append(year)
                        quater = data.split(".json")[0]
                        mapUserColumns['Quarter'].append(int(quater))
                except:
                    pass

    df_MapUser = pd.DataFrame(mapUserColumns)

    ##############################################################
    topTransPath = path+"/pulse/data/top/transaction/country/india/state/"

    topTransStateList = os.listdir(topTransPath)
    topTransColumns = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
                'Transaction_amount': []}


    for state in topTransStateList:
        currentState = topTransPath + state + "/"
        topYearList = os.listdir(currentState)
        
        for year in topYearList:
            currentYear = currentState + year + "/"
            topTransData = os.listdir(currentYear)
            
            for data in topTransData:
                currentFile = currentYear + data
                file = open(currentFile, 'r')
                topTransDetails = json.load(file)
                try:
                    
                    for item in topTransDetails['data']['pincodes']:                
                        topTransColumns['Pincode'].append(item['entityName'])
                        topTransColumns['Transaction_count'].append(item['metric']['count'])
                        topTransColumns['Transaction_amount'].append(item['metric']['amount'])
                        topTransColumns['State'].append(state)
                        topTransColumns['Year'].append(year)
                        quater = data.split(".json")[0]
                        topTransColumns['Quarter'].append(int(quater))
                except:
                    pass
    df_TopTrans = pd.DataFrame(topTransColumns)
    ###############################################

    topUserPath = path+"/pulse/data/top/user/country/india/state/"
    topUserStateList = os.listdir(topUserPath)
    topUserColumns = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
                'RegisteredUsers': []}


    for state in topUserStateList:
        currentState = topUserPath + state + "/"
        topYearStateList = os.listdir(currentState)
        
        for year in topYearStateList:
            currentYear = currentState + year + "/"
            topUserData = os.listdir(currentYear)
            
            for data in topUserData:
                currentFile = currentYear + data
                file = open(currentFile, 'r')
                topUserDetails = json.load(file)
                
                for item in topUserDetails['data']['pincodes']:                
                    topUserColumns['Pincode'].append(item['name'])
                    topUserColumns['RegisteredUsers'].append(item['registeredUsers'])
                    topUserColumns['State'].append(state)
                    topUserColumns['Year'].append(year)
                    quater = data.split(".json")[0]
                    topUserColumns['Quarter'].append(int(quater))
    df_TopUser = pd.DataFrame(topUserColumns)

    #######################################################
    csvfile ="csvfile"
    Csvpath= os.path.join(parentpath, csvfile)
    isCloneExist = os.path.exists(Csvpath)
    if isCloneExist:
        pass
    else:    
        os.mkdir(Csvpath)
        # df_AggTrans.to_csv(Csvpath+'\agg_trans.csv',index=False)
        df_AggTrans.to_csv(Csvpath+'/'+'agg_trans.csv',index=False)
        df_AggUser.to_csv(Csvpath+'/'+'agg_user.csv',index=False)
        df_MapTrans.to_csv(Csvpath+'/'+'map_trans.csv',index=False)
        df_MapUser.to_csv(Csvpath+'/'+'map_user.csv',index=False)
        df_TopTrans.to_csv(Csvpath+'/'+'top_trans.csv',index=False)
        df_TopUser.to_csv(Csvpath+'/'+'top_user.csv',index=False)

    #########################################################


    aggregateTransaction = df_AggTrans.astype({'State':str,'Year':int,'Transaction_type':str})
    mycursor.execute("CREATE DATABASE if not exists {}".format(myDatabaseName))
    mycursor.execute("USE {}".format(myDatabaseName))
    queryAgTrans = '''CREATE table if not exists agg_trans (
                State varchar(100), 
                Year int, 
                Quarter int, 
                Transaction_type varchar(100), 
                Transaction_count int, 
                Transaction_amount double
                )
                '''
    mycursor.execute(queryAgTrans)
    aggregateTransaction.to_sql("agg_trans",engine, if_exists='replace', index= False)

    #################################################

    aggregateUser = df_AggUser.astype({'Year':int})
    queryAgUser = '''CREATE table if not exists agg_user (
        State varchar(100), 
        Year int, 
        Quarter int, 
        Brands varchar(100), 
        Count int, 
        Percentage double
        )
    '''
    mycursor.execute(queryAgUser)
    aggregateUser.to_sql("agg_user",engine, if_exists='replace', index= False)
    ##############################################################

  
    mapTransancation = df_MapTrans.astype({'Year':int})
    queryMapTrans ='''CREATE table if not exists map_trans (
        State varchar(100), 
        Year int, 
        Quarter int, 
        District varchar(100), 
        Count int, 
        Amount double)
    '''
    mycursor.execute(queryMapTrans)
    mapTransancation.to_sql("map_trans",engine,if_exists='replace',index=False)

    #################df_MapTrans.to_sql("map_trans",engine, if_exists='append', index= False)

    #######################################################################

    mapUserTrans = df_MapUser.astype({'Year':int})
    queryMapUser ='''CREATE table if not exists map_user 
    (State varchar(100), 
    Year int, Quarter int, 
    District varchar(100), 
    Registered_user int, 
    App_opens int)
    '''
    mycursor.execute(queryMapUser)
    mapUserTrans.to_sql("map_user",engine,if_exists='replace',index=False)

    ################################################################

    topTransaction = df_TopTrans.astype({'Year':int})
    queryTopTrans='''CREATE table if not exists top_trans (
        State varchar(100), 
        Year int, 
        Quarter int, 
        Pincode int, 
        Transaction_count int, 
        Transaction_amount double)
    '''
    mycursor.execute(queryTopTrans)
    topTransaction.to_sql("top_trans",engine,if_exists='replace',index=False)

    ########################################################
    topUserTrans = df_TopTrans.astype({'Year':int})
    queryTopUser='''CREATE table if not exists top_user (
        State varchar(100), 
        Year int, 
        Quarter int, 
        Pincode int,
        Registered_users int
        )

    '''
    mycursor.execute(queryTopUser)
    topUserTrans.to_sql("top_user",engine,if_exists='replace',index=False)
    st.write("Clone Successfully please shift to another Tab")
