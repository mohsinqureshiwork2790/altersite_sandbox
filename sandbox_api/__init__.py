import logging
import azure.functions as func
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
import json
import time
import pandas as pd
import pytz
import re
import numbers
import pyodbc
import datetime
import requests
import copy
import os
import uuid
from datetime import timedelta
from collections import OrderedDict
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
# from azure.kusto.data.exceptions import KustoServiceError
# from azure.kusto.data.helpers import dataframe_from_result_table
from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import IotHubDataConnection
from azure.mgmt.kusto.models import DatabasePrincipalAssignment
from azure.mgmt.kusto.models import ReadWriteDatabase
from azure.common.credentials import ServicePrincipalCredentials
# from azure.cli.core import get_default_cli    --removed this import due to package clash 
from shared_code import alarms_utility
import ast




AAD_TENANT_ID = os.environ["AAD_Tenant_Id"]
client_id = os.environ["Client_Id"]
client_secret = os.environ["Client_Secret"]
subscription_id="43e8904e-4836-4504-8c18-3e88d5fa6b78"
az_cli_url= "https://az-cli.azurewebsites.net/api/az_cli?query="

# --removed this import due to package clash 
# def az_cli (args_str):
#     args = args_str.split()
#     # logging.info(args)
#     cli = get_default_cli()
#     # logging.info('az_cli called')
#     # logging.info(cli)
#     cli.invoke(args)
#     if cli.result.result:
#         return cli.result.result
#     elif cli.result.error:
#         raise cli.result.error
#     return True

def get_site_id(cnxn, site_name):
    sql = "SELECT Site_Id FROM Plant_Site WHERE  IsProductionENV =0 and AutomationTableName = '"+site_name+"'"
    temp_df = pd.read_sql(sql,cnxn)
    site_id = temp_df['Site_Id'][0]
    return site_id

def checkresourcegroupexist(site_name):
    new_name = str(site_name).replace("_","-")
    new_resource_group = new_name.replace("-", "")
    logging.info(new_resource_group)
    query = "group exists -n "+new_resource_group
    # query = "group show --name "+new_resource_group
    logging.info(query)
    response = az_cli_url+query
    r= requests.post(response)
    # response = az_cli(query)
    logging.info('True with response...')
    logging.info(r)
    # logging.info(type(response))
    
 
        

def create_azure_resourcegroup(site_name):
    new_name = str(site_name).replace("_","-")
    new_resource_group = new_name.replace("-", "")
    region = "southeastasia"
    
    query = "group create --name "+new_resource_group+"  --location "+region+""
    logging.info(query)
    response = az_cli_url+query
    r= requests.post(response)
    # response = az_cli(query)
    logging.info(r)





def get_site_users(cnxn, site_id):
    sql = f"getUsersListBySiteId {site_id}"
    # logging.info(sql)
    temp_df = pd.read_sql(sql,cnxn)
    users = temp_df['Email'].to_list()
    return users

def Underline (lis=[]):
    underline = [] 
    for i in lis:
        line = re.sub(r"\s", "_", str(i))
        underline.append(line)
    return underline


def Remove_space (strr):
    line = re.sub(r"\s", "", str(strr))
    return line

# f"login --service-principal -u {client_id} -p {client_secret} --tenant {AAD_TENANT_ID}"
def create_IotHub(NAME):
  #CREATE IOT HUB
  new_iot_hub_name = str(NAME).replace("_","-")
  new_resource_group = new_iot_hub_name.replace("-", "")
  query =f"iot hub create --resource-group {new_resource_group}  --name {new_iot_hub_name} --sku S1 --partition-count 2"
  response = az_cli_url+query
  logging.info(response)
  r= requests.post(response)
  logging.info(r)
 #   response = az_cli(query)
  
  #GET IOT HUB RESOURCE ID
  query = "iot hub show --name "+new_iot_hub_name+""
  response = az_cli_url+query
  logging.info(response)
 #   response = az_cli(query)
  r= requests.post(response)
  logging.info('iot hub resource id is:...')
  logging.info(r)
  logging.info(str(type(r)))
  response_data = json.loads(r.text)
  response_data=ast.literal_eval(response_data)
#   response_data=response_data.replace("'", "\"")
# logging.info(response_data)
  #logging.info(type(updatedd_resp))
  logging.info('getting response data:...')
  
#   keys=(response_data.keys())
#   first_key = keys[0]
#   first_value = response_data[first_key]
#   logging.info(first_value)
  iot_hub_resource_id = response_data['id']
  
#   response_data=str(json.dumps(r))
#   logging.info(str(type(response_data)))
  
#   
#   
#   iot_hub_resource_id = response_data['id']
  logging.info(iot_hub_resource_id)

  
  return iot_hub_resource_id


def create_DB (NAME,days):
    subscription_id = "43e8904e-4836-4504-8c18-3e88d5fa6b78"
    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=AAD_TENANT_ID
    )
    # new_name = str(NAME).replace("_","-")
    # new_resource_group = new_name.replace("-", "")
    KUSTO_CLUSTER = "https://sandboxadx.southeastasia.kusto.windows.net/" 
    location = 'Southeast Asia'
    resource_group_name = 'OctopusDigital'
    # resource_group_name = new_resource_group
    cluster_name = 'sandboxadx'
    soft_delete_period = timedelta(days=days)
    hot_cache_period = timedelta(days=days)
    database_name = str(NAME)
    
    kusto_management_client = KustoManagementClient(credentials, subscription_id)

    database_operations = kusto_management_client.databases
    database = ReadWriteDatabase(location=location,
                        soft_delete_period=soft_delete_period,
                        hot_cache_period=hot_cache_period)
    try:
        poller = database_operations.create_or_update(resource_group_name = resource_group_name, cluster_name = cluster_name, database_name = database_name, parameters = database)
        poller.wait()
        print("In create_DB")
    except Exception as e:
        print(e)

def add_principle(NAME, email, id, role,days):
    subscription_id = "43e8904e-4836-4504-8c18-3e88d5fa6b78"
    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=AAD_TENANT_ID
    )
    
    KUSTO_CLUSTER = "https://sandboxadx.southeastasia.kusto.windows.net/" 
    location = 'Southeast Asia'
    new_name = str(NAME).replace("_","-")
    new_resource_group = new_name.replace("-", "")
    resource_group_name = new_resource_group
    cluster_name = 'sandboxadx'
    soft_delete_period = timedelta(days=days)
    hot_cache_period = timedelta(days=days)
    database_name = str(NAME)
  
  
    kusto_management_client = KustoManagementClient(credentials, subscription_id)

    #Add principle ID to ADX Table
    principal_assignment_name = "clusterPrincipalAssignment" + str(id)
    #User email, application ID, or security group name
    principal_id = email
    #AllDatabasesAdmin or AllDatabasesViewer
    # role = "Admin"
    role = role
    tenant_id_for_principal = AAD_TENANT_ID
    #User, App, or Group
    principal_type = "User"

    #Returns an instance of LROPoller, check https://docs.microsoft.com/python/api/msrest/msrest.polling.lropoller?view=azure-python
    poller = kusto_management_client.database_principal_assignments.create_or_update(resource_group_name=resource_group_name, cluster_name=cluster_name, database_name=database_name, principal_assignment_name= principal_assignment_name, parameters=DatabasePrincipalAssignment(principal_id=principal_id, role=role, tenant_id=tenant_id_for_principal, principal_type=principal_type))
    poller.wait()

def Kusto_Execute(db,query):
    cluster = "https://sandboxadx.southeastasia.kusto.windows.net/" 

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, AAD_TENANT_ID)
    client = KustoClient(kcsb)

    response = client.execute(db, query)


def create_raw_table (lis,site_name):
    final=[]
    schema='column:type'
    strr=''
    for i in range(0,len(lis)): 
        if i < len(lis)-1:
            final.append(schema+',')
        else:
            final.append(schema)
    for i in range (0,len(final)):
        final[i]=re.sub(r'column',lis[i],final[i])
        if i <= 4999:
            final[i]=re.sub(r'type','real',final[i])
        else:
            final[i]=re.sub(r'type','dynamic',final[i])
    for i in final:
        strr += str(i)
    query = '.create table Raw_'+site_name+' (TimeStamp:datetime,ID:int,StatusCode:dynamic,'+strr+')'
    return query


def create_mapping (lis,site_name):
    final=[];schema='{"column":"tag1","path":"$.tag1"}';strr=''
    for i in range(0,len(lis)): 
        if i < len(lis)-1:final.append(schema+',')
        else: final.append(schema)
    for i in range (0,len(final)): final[i]=re.sub(r'tag1',lis[i],final[i])
    final.append(',{"column":"TimeStamp","path":"$.TimeStamp"},')
    final.append('{"column":"ID","path":"$.ID"},')
    final.append('{"column":"StatusCode","path":"$.StatusCode"}')
    for i in final: strr+=str(i)
    query = ".create table Raw_"+site_name+" ingestion json mapping 'Raw_"+site_name+"_mapping' '["+strr+"]'"
    query2 = ".alter table Raw_"+site_name+" policy ingestionbatching @'{\"MaximumBatchingTimeSpan\": \"00:05:00\",\"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024}'"
    return query, query2

def create_stats_mapping (site_name):
    final = """[{"Name":"TimeStamp","DataType":"","Ordinal":"0","ConstValue":null},{"Name":"Aggregation_resolution","DataType":"","Ordinal":"1","ConstValue":null},{"Name":"Aggregation_value","DataType":"","Ordinal":"2","ConstValue":null},{"Name":"Tag_Name","DataType":"","Ordinal":"3","ConstValue":null},{"Name":"min","DataType":"","Ordinal":"4","ConstValue":null},{"Name":"max","DataType":"","Ordinal":"5","ConstValue":null},{"Name":"avg","DataType":"","Ordinal":"6","ConstValue":null},{"Name":"stdev","DataType":"","Ordinal":"7","ConstValue":null},{"Name":"var","DataType":"","Ordinal":"8","ConstValue":null},{"Name":"Tag_Type","DataType":"","Ordinal":"9","ConstValue":null}]"""
    query = ".create table Stats_"+site_name+" ingestion csv mapping 'Stats_"+site_name+"_mapping' '" + final + "'"
    query2 = ".alter table Stats_"+site_name+" policy ingestionbatching @'{\"MaximumBatchingTimeSpan\": \"00:05:00\",\"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024}'"
    return query, query2

def create_manual_table(lis,site_name):
    query = ".create table Man_"+site_name+" (Id:int)"
    return query

def create_stats_table(site_name):
    query = ".create table Stats_"+site_name+" (TimeStamp: datetime, Aggregation_resolution: string, Aggregation_value: long, Tag_Name: string, min: real, max: real, avg: real, stdev: real, var: real, Tag_Type: string)"
    return query

def create_func (lis,site_name):
    strr=''
    for i in range(0,len(lis)): 
        if i==len(lis)-1:
          strr+=str(lis[i])
        else:
          strr+=str(lis[i])+','
    if strr:
      query1 = ".create-or-alter function calc_"+site_name+"()\n {\nRaw_"+site_name+" \n| project TimeStamp,Calc_Status_Code=todynamic(''),"+strr+"\n}\n"
    else:
      query1 = ".create-or-alter function calc_"+site_name+"()\n {\nRaw_"+site_name+" \n| project TimeStamp,Calc_Status_Code=todynamic('')"+strr+"\n}\n"
    query2 = ".set-or-append Calc_"+site_name+"  <| calc_"+site_name+"()\n"
    query3 = ".alter table Calc_"+site_name+" policy update\n@'[{'IsEnabled': true, 'Source': 'Raw_"+site_name+"', 'Query': 'calc_"+site_name+"()', 'IsTransactional': false, 'PropagateIngestionProperties': false}]'"
    query3 = re.sub(r"'\[",r'"[',query3)
    query3 = re.sub(r"\]'",r']"',query3)
    # # logging.info(query3)
    Kusto_Execute(site_name,query1)
    Kusto_Execute(site_name,query2)
    Kusto_Execute(site_name,query3)

def create_datahealth_func(site_name):
    final = """.create-or-alter function with (docstring = "Function to find tag status",folder = "Data Health") dataHealthStatus(tagName:string,fromTime:datetime,toTime:datetime,statusCode:int) {
        Raw_"""+site_name+""" 
        | where TimeStamp>= fromTime and TimeStamp<= toTime
        | project StatusCode
        | evaluate bag_unpack(StatusCode, columnsConflict='replace_source')
        | extend tag= column_ifexists(tagName,R3)
        | serialize eventList=tag-prev(tag)
        | extend StatusCode= iff(eventList!=0,tag,long(null))
        | where case(statusCode==-1, StatusCode>=0,
        statusCode!=-1, StatusCode==statusCode, StatusCode==1)
        | serialize TimeStamp_endtime=next(TimeStamp) 
        | project TagName=tagName,StatusCode,start_time=TimeStamp,TimeStamp_endtime,duration=TimeStamp_endtime-TimeStamp
        | where TimeStamp_endtime<= toTime  
        }"""
    Kusto_Execute(site_name,final)

def pipeline_config(NAME,IotHub_resource_id):
  
    subscription_id = "43e8904e-4836-4504-8c18-3e88d5fa6b78"
    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=AAD_TENANT_ID
    )

    kusto_management_client = KustoManagementClient(credentials, subscription_id)
    # new_name = str(NAME).replace("_","-")
    # new_resource_group = new_name.replace("-", "")
    # resource_group_name = new_resource_group
    #The cluster and database that are created as part of the Prerequisites
    resource_group_name = 'OctopusDigital'
    cluster_name = "sandboxadx"
    database_name = NAME
    # if len(NAME)>=35:
    #     data_connection_name="pl"+NAME+""
    # else:
    #     data_connection_name = "pipeline_"+NAME+""
    data_connection_name = "pipeline_"+NAME+""  #this thorws error when site_NAME is close to length of 40 sith pipeline_ is being appended to sitename. so length should be <=40
    # logging.info('data connection name is :...')
    # logging.info(data_connection_name)
    # # logging.info('data_connection name created... ')
    #The event hub that is created as part of the Prerequisites
    resource_id = str(IotHub_resource_id)
    consumer_group = "$Default"
    location = "Southeast Asia"
    #The table and column mapping that are created as part of the Prerequisites
    table_name = "Raw_"+NAME+""
    # # logging.info('raw table name created... ')
    mapping_rule_name = "Raw_"+NAME+"_mapping"
    # # logging.info('mapping table  created... ')
    data_format = "JSON"
    shared_access_policy_name = "iothubowner"

    #Returns an instance of LROPoller, check https://docs.microsoft.com/python/api/msrest/msrest.polling.lropoller?view=azure-python
    poller = kusto_management_client.data_connections.create_or_update(resource_group_name=resource_group_name, cluster_name=cluster_name, database_name=database_name, data_connection_name=data_connection_name, parameters=IotHubDataConnection(iot_hub_resource_id=resource_id, consumer_group=consumer_group, location=location, table_name=table_name, mapping_rule_name=mapping_rule_name, data_format=data_format, shared_access_policy_name=shared_access_policy_name))
    # logging.info('poller instance created...')
    poller.wait()

def call_alarmengine_automation(site_name,database_name):
    try:
        # requests.get(f'https://alarmsengineautomation-premiuim.azurewebsites.net/api/AlarmsEngineAutomation?site_name={site_name}&database_name={database_name}',timeout=3)
        # requests.get(f'https://alarmssandboxtest.azurewebsites.net/api/alarmsengineautomation?site_name={site_name}&database_name={database_name}',timeout=3)
        requests.get(f'https://alarmengineautomation-sb-test.azurewebsites.net/api/alarmsengineautomation?site_name={site_name}&database_name={database_name}',timeout=3)
        # requests.get(f'https://alarmstest-sb.azurewebsites.net/api/alarmsengineautomation?site_name={site_name}&database_name={database_name}',timeout=3)
        
        
        
    except requests.exceptions.ReadTimeout: 
        pass

def auto_code_gen(NAME, no_of_tags):
   
    real_tags = ['R'+str(i) for i in range(1,no_of_tags+1)]
    # tot_tags = ['T'+str(i) for i in range(1,1001)]
    # tot_source_tag = real_tags

    return real_tags#, tot_tags, tot_source_tag

def call_grant_permission(site_name,users):
    try:
        # requests.get(f'https://generalapis.azurewebsites.net/api/grant_permission_adx?site_name={site_name}&users={users}',timeout=3)
        requests.get(f'https://generalsandboxapis.azurewebsites.net/api/grant_permission_sandbox?site_name={site_name}&users={users}',timeout=3)
        # requests.get(f'http://localhost:7071/api/grant_permission_adx?site_name={site_name}&users={users}',timeout=3)
    except requests.exceptions.ReadTimeout: 
        pass

def convert_timedelta(duration):
    # days, seconds = duration.days, duration.seconds
    seconds = duration.seconds
    # hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    # return hours, minutes, seconds
    return minutes, seconds


def log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID, Stage="", Error=""):
    if Error != "":
        Error = eval(Error)
        Error = Error[0]['error']['@message'].replace("'", "")

    sql_query = """
    INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
    VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(Error)+"""','"""+Exec_ID+"""')
    """
    # logging.info('executing sql query to insert into log_entry')
    cursor.execute(sql_query)
    cnxn.commit()    


def main(req: func.HttpRequest) -> func.HttpResponse:
    # logging.info('Python HTTP trigger function processed a request.')

    site_name = req.params.get('site_name') #cdncontrolsltd_408
    
    # send dummy response for to logic app for cold start problem
    if(site_name=='dummy'):
        return func.HttpResponse(
             "Dummy function called",
             status_code=200
        )

    database_name = req.params.get('database_name') #'Production_Replica'
    no_of_tags = int((req.params.get('no_of_tags'))) #10,000
    days = int(req.params.get('days')) #would be 15 always
    # logging.info(site_name)
    # logging.info(database_name)
    # logging.info(no_of_tags)
    # logging.info(days)
    # # logging.info(type(site_name))
    # # logging.info(type(database_name))
    # # logging.info(type(no_of_tags))
    # # logging.info(type(days))
    
    #SystemAlarmId from SQL server

    temp_time1 = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
    #Login to Azure CLI
    # login_string = """
    # login --service-principal -u """+client_id+""" -p """+client_secret+""" --tenant """+AAD_TENANT_ID+"""
    # """
    login_string = f"login --service-principal -u {client_id} -p {client_secret} --tenant {AAD_TENANT_ID}"
    logging.info('loging in string is:')
    logging.info(login_string)
    response=az_cli_url+login_string
    r= requests.post(response)
    logging.info(r)
    # logging.info(response)
    # az_cli(login_string)
    logging.info('successful login...')

    #Add Iot Hub extension
    extension_query = "extension add --name azure-iot"
    az_cli_url+extension_query
    r= requests.post(response)
    logging.info(r)
    # az_cliextension_query)
    logging.info('successful login with extension query...')

    server = 'tcp:octopusdigitalsql.database.windows.net'
    database = database_name #'Production_Replica'
    username = 'octopusdigital' 
    # password = os.environ["Database_Key"]
    password = 'avanceon@786'
    # logging.info('connecting to sql db')
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    if checkresourcegroupexist(site_name):
        logging.info('resource group already exists...')
        return 0
    else:   
        logging.info('inside else...') 
        create_azure_resourcegroup(site_name)
    
        #Connect to SQL
        
        # logging.info('connected to sql db')
        
        column_names    = ["EventName","AS_Id","AlarmType", "SiteEventID","Event_Id","isAlarmActive"]
        alarms_df       = pd.DataFrame(columns = column_names)
        # logging.info(alarms_df)
        tables_status   =   0
        # site_name = req.params.get('site_name')
        # no_of_tags = int(req.params.get('no_of_tags'))
        if site_name and no_of_tags and database_name and days:
            try:
                #Data for logging into SQL
                Exec_ID = uuid.uuid4().hex
                Type = "New Site"
                site_id = get_site_id(cnxn, site_name)
                logging.info("got site_id")
                #Log That Execution Started
                Status = "Started"
                TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID)
                #Create IoT Hub
                for _ in range(10):
                    Stage = "Iot Hub Creation"
                    EventName = "Create Iot Hub"
        
                    try:
                        logging.info('creating iot hub')
                        IotHub_resource_id = create_IotHub(site_name)
                        logging.info('iothub_resource_id is :...')
                        logging.info(IotHub_resource_id)
                        logging.info('created iot hub successfully')
                        Status = "Complete"
                        temp_DF     = alarms_utility.get_stateEventId(cnxn, EventName, site_id)
                        # logging.info("temp_df value is....")
                        # logging.info(temp_DF)
                        if not temp_DF.empty:
                            new_row = {'EventName':EventName, 'AS_Id':temp_DF.iloc[0]['AS_Id'],'AlarmType':'System Alarms', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'],  'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                            # logging.info('value of new_row are:...')
                            # logging.info(new_row)
                            new_row_df = pd.DataFrame(new_row, index=[0])
                            # alarms_df = alarms_df.append(new_row, ignore_index=True) used with pandas less than 2.0 (actual query used in production)
                            alarms_df = pd.concat([alarms_df,new_row_df])
                            # logging.info('concating new row to alrms dataframe...')
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        # logging.info('log_entry function called')
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('inserting in automation_logs due to exception'+str(e))
                        # # logging.info(str(e))
                        t_sql_query = """
                        INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
                        VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(e)+"""','"""+Exec_ID+"""')
                        """
                        cursor.execute(t_sql_query)
                        cnxn.commit()
                        # logging.info('Error: Create IOT Hub| '+ str(e)) 
                        # log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(5)
                else:
                    # logging.info('entering in else...')
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    if not temp_DF.empty:
                        new_row = {'EventName':'Error: Create IOT Hub Failed', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'], 'AS_Id':temp_DF.iloc[0]['AS_Id']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)  
                    else:
                        new_row = {'EventName':'Error: Create IOT Hub Failed; No Configuration found in DB', 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)   
                    pass

                
                # Create ADX DB   
                for _ in range(3):
                    Stage       =   "Create ADX Database"
                    EventName   =   "Create ADX Database"
                    try:
                        logging.info('creating adx db...')
                        create_DB(site_name,days)
                        logging.info('created adx db...')
                        Status = "Complete"
                        temp_DF     = alarms_utility.get_stateEventId(cnxn, EventName, site_id) 
                        if not temp_DF.empty:
                            # print(temp_DF.iloc[0]['Event_Id'],temp_DF.iloc[0]['SiteEventID'])
                            new_row = {'EventName':EventName, 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                            new_row_df = pd.DataFrame(new_row, index=[0])
                            # alarms_df = alarms_df.append(new_row, ignore_index=True)
                            alarms_df = pd.concat([alarms_df,new_row_df])

                        # print(alarms_df)
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create ADX DB| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    if not temp_DF.empty:
                        new_row = {'EventName':'Error: Create ADX DB Failed', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'], 'AS_Id':temp_DF.iloc[0]['AS_Id']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    else:
                        new_row = {'EventName':'Error: Create ADX DB Failed; No Configuration found in DB', 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass


                #Create Raw Tags Table
                for _ in range(3):
                    Stage = "Create Raw Tags Table"
                    try:
                        # logging.info('creating raw tags table')
                        real_tags = auto_code_gen(site_name, no_of_tags)
                        # logging.info('real tags created')
                        kusto_query = create_raw_table(real_tags, site_name)
                        Kusto_Execute(site_name, kusto_query)
                        # logging.info('created raw tags table')
                        Status = "Complete"
                        tables_status   =   1
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Raw Tags Table| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                
                # Create Raw Tags Mapping
                for _ in range(3):
                    Stage = "Create Raw Tags Mapping"
                    try:
                        # logging.info('creating raw tags mapping...')
                        kusto_query1, kusto_query2 = create_mapping(real_tags, site_name)
                        Kusto_Execute(site_name, kusto_query1)
                        Kusto_Execute(site_name, kusto_query2)
                        # logging.info('created raw tags mapping...')
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Raw Tags Mapping| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Configure IoT Hub to ADX Raw Table Pipeline
                for _ in range(3):
                    Stage       =   "Configure IoT to ADX Pipeline"
                    EventName   =   "Create IoT Hub to ADX Connection"
                    try:
                        logging.info('configuring iot with adx raw table...')
                        
                        pipeline_config(site_name, IotHub_resource_id) ## Create IoT Hub and ADX Connection
                        
                        logging.info('configured iot with adx raw table...')
                        Status = "Complete"
                        temp_DF     = alarms_utility.get_stateEventId(cnxn, EventName, site_id)
                        # logging.info('temp_df value in iot to adx mapping is:....')
                        # logging.info(temp_DF)
                        if not temp_DF.empty:
                            # print(temp_DF.iloc[0]['Event_Id'],temp_DF.iloc[0]['SiteEventID'])
                            new_row = {'EventName':EventName, 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                            # alarms_df = alarms_df.append(new_row, ignore_index=True) actual production code
                            new_row_df = pd.DataFrame(new_row, index=[0])
                            alarms_df = pd.concat([alarms_df,new_row_df])

                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        t_sql_query = """
                        INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
                        VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(e).replace("'","")+"""','"""+Exec_ID+"""')
                        """
                        cursor.execute(t_sql_query)
                        cnxn.commit() 
                        # logging.info('Error: Configure IoT Hub to ADX Raw Table Pipeline| '+ str(e)) 
                        # log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    if not temp_DF.empty:
                        new_row = {'EventName':'Error: Create IoT Hub To ADX Connection Failed', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'], 'AS_Id':temp_DF.iloc[0]['AS_Id']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    else:
                        new_row = {'EventName':'Error: Create IoT Hub To ADX Connection Failed; No Configuration found in DB', 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create Manual Table
                for _ in range(3):
                    Stage = "Create Manual Table"
                    try:
                        # logging.info('creating manual tables...')
                        man_tags=[]
                        Kusto_Execute(site_name,create_manual_table(man_tags,site_name))
                        # logging.info('created manual tables...')
                        Status = "Complete"
                        tables_status   =   2
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Manual Table| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create Stats Table
                for _ in range(3):
                    Stage = "Create Statistical Table"
                    try:
                        # logging.info('creating stats tables...')
                        Kusto_Execute(site_name,create_stats_table(site_name))
                        # logging.info('created stats tables...')
                        Status = "Complete"
                        tables_status   =   2
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Stats Table| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                # Create Stats Mapping
                for _ in range(3):
                    Stage = "Create Stats Table Mapping"
                    try:
                        # logging.info('creating Stats Mapping...')
                        kusto_query1, kusto_query2 = create_stats_mapping(site_name)
                        # logging.info('created Stats Mapping...')
                        # logging.info(kusto_query1)
                        Kusto_Execute(site_name, kusto_query1)
                        Kusto_Execute(site_name, kusto_query2)
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Stats Table csv Mapping| '+ str(e)) 
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create Calc Table # Create Table, Raw, Manual
                for _ in range(3):
                    Stage       =   "Create Calc Table"
                    EventName   =   "Create ADX Tables"
                    try:
                        # logging.info('creating  Calc Table # Create Table, Raw, Manual...')
                        calc_list=[]
                        create_func(calc_list,site_name) #if completes # Create Update Policy
                        # logging.info('created  Calc Table # Create Table, Raw, Manual...')
                        Status = "Complete"
                        if Stage == "Create Calc Table" and tables_status==2:
                            temp_DF     = alarms_utility.get_stateEventId(cnxn, EventName, site_id)
                            if not temp_DF.empty:
                                # print(temp_DF.iloc[0]['Event_Id'],temp_DF.iloc[0]['SiteEventID'])
                                new_row = {'EventName':EventName, 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                                # alarms_df = alarms_df.append(new_row, ignore_index=True)
                                new_row_df = pd.DataFrame(new_row, index=[0])
                                alarms_df = pd.concat([alarms_df,new_row_df])

                            
                            temp_DF     = alarms_utility.get_stateEventId(cnxn, "Create Update Policy", site_id)
                            if not temp_DF.empty:
                                print(temp_DF.iloc[0]['Event_Id'],temp_DF.iloc[0]['SiteEventID'])
                                new_row = {'EventName':'Create Update Policy', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                                # alarms_df = alarms_df.append(new_row, ignore_index=True)
                                new_row_df = pd.DataFrame(new_row, index=[0])
                                alarms_df = pd.concat([alarms_df,new_row_df])

                        # print(alarms_df)
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Calc Table| '+ str(e))   
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    if not temp_DF.empty:
                        new_row = {'EventName':'Error: Create Tables/ Update Policy Failed', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'], 'AS_Id':temp_DF.iloc[0]['AS_Id']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    else:
                        new_row = {'EventName':'Error: Create Tables/ Update Policy Failed; No Configuration found in DB', 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create IoT Hub Event Topic
                for _ in range(3):
                    Stage = "Iot Hub Event Topic" 
                    EventName = "Create Iot Hub Event Topic"
                    try:
                        # logging.info('creating iot hub event topic and getting token')
                        #Get Bearer Token
                        body = {"grant_type":"client_credentials", "client_id":"1813de48-2733-4220-ab74-7804859f8fb6", "client_secret":"cHi8Q~xFi3Sp2D6BvUFBV.Ybh_VJwwnQBe4tGaLg", "scope":"https://management.azure.com/.default"}
                        response = requests.post("https://login.microsoftonline.com/avanceon.ae/oauth2/v2.0/token", data=body)
                        bearer_token = response.json()['access_token']
                        # logging.info('got bearer_token')
                        #Create System Topic for IoT Hub
                        topic_name = str(site_name).replace("_","-") + "-topic"
                        new_name = str(site_name).replace("_","-")
                        new_resource_group = new_name.replace("-", "")
                        # url = "https://management.azure.com/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/OctopusDigital/providers/Microsoft.EventGrid/systemTopics/"+topic_name+"?api-version=2020-10-15-preview"
                        url = "https://management.azure.com/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/"+new_resource_group+"/providers/Microsoft.EventGrid/systemTopics/"+topic_name+"?api-version=2020-10-15-preview"
                        body = {
                            "properties": {
                                # "source": "/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/OctopusDigital/providers/Microsoft.Devices/IotHubs/"+str(site_name).replace("_","-")
                                "source": "/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/"+new_resource_group+"/providers/Microsoft.Devices/IotHubs/"+str(site_name).replace("_","-"),
                                "topicType": "Microsoft.Devices.IoTHubs"
                            },
                            "location": "southeastasia"
                        }
                        response = requests.put(url, headers={"Authorization":"Bearer "+bearer_token}, json=body)
                        # logging.info('successful post request')
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        t_sql_query = """
                        INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
                        VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(e)+"""','"""+Exec_ID+"""')
                        """
                        cursor.execute(t_sql_query)
                        cnxn.commit() 
                        # logging.info('Error: Create IOT Hub Event Topic| '+ str(e)) 
                        # log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(5)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    if not temp_DF.empty:
                        new_row = {'EventName':'Error: Create IoT Hub Event Topic Failed', 'SiteEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'], 'AS_Id':temp_DF.iloc[0]['AS_Id']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    else:
                        new_row = {'EventName':'Error: Create IoT Hub Event Topic Failed; No Configuration found in DB', 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive']}
                        alarms_df = alarms_df.append(new_row, ignore_index=True)
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass


                #Create IoT Hub Event Subscription
                for _ in range(3):
                    Stage = "Iot Hub Event Subscription"
                    try:
                        # logging.info('creating iot hub event subscription')
                        if (database_name.lower() == 'omniconnect'):
                            endpoint_url = "https://iotedgedeploymentstaging.azurewebsites.net/api/newdevice_staging"
                        else:
                            endpoint_url = "https://iotedgedeploymentstaging.azurewebsites.net/api/newdevice_stag"

                        # url = "https://management.azure.com/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/OctopusDigital/providers/Microsoft.EventGrid/systemTopics/"+topic_name+"/eventSubscriptions/newdevice?api-version=2020-10-15-preview"
                        url = "https://management.azure.com/subscriptions/43e8904e-4836-4504-8c18-3e88d5fa6b78/resourceGroups/"+new_resource_group+"/providers/Microsoft.EventGrid/systemTopics/"+topic_name+"/eventSubscriptions/newdevice?api-version=2020-10-15-preview"
                        body = {
                            "properties": {
                                "destination": {
                                    "endpointType": "WebHook",
                                    "properties": {
                                        "endpointUrl": endpoint_url
                                    }
                                }
                            }
                        }
                        response = requests.put(url, headers={"Authorization":"Bearer "+bearer_token}, json=body)
                        # logging.info('created iot hub event subscription')
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        t_sql_query = """
                        INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
                        VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(e)+"""','"""+Exec_ID+"""')
                        """
                        cursor.execute(t_sql_query)
                        cnxn.commit() 
                        # logging.info('Error: Create IOT Hub Event Subscription| '+ str(e)) 
                        # log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(5)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create data health function
                for _ in range(3):
                    Stage       =   "Create Data Health Function"
                    try:
                        # logging.info('Create Data Health Function') 
                        calc_list=[]
                        create_datahealth_func(site_name) #if completes # Create Update Policy
                        Status = "Complete"
                        # logging.info('Created Data Health Function') 
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Create Data Health function '+ str(e))   
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #Create alarm engine resource automation
                for _ in range(3):
                    Stage       =   "Calling Alarm Engine Automation"
                    try:
                        logging.info('creating Alarm Engine Automation')
                        call_alarmengine_automation(site_name,database_name)
                        logging.info('created Alarm Engine Automation')
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Alarm Engine Automation'+ str(e))   
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass
                # # logging.info(alarms_df)
                #--------------- Sending dataframe to EventHub ---------------# 
                try:
                    eventhub_conctnStr  =   alarms_utility.get_eventHub_ConnStr(cnxn, site_id)
                    eventhub_conctnStr  =   eventhub_conctnStr['EventHub_ConnStr'][0]
                    # logging.info(eventhub_conctnStr)
                    alarms_df           =   alarms_df[alarms_df['isAlarmActive'] == True]
                    alarms_df['DataAlarmActionGUID'] = [str(uuid.uuid4()) for x in range(len(alarms_df))]
                    # logging.info(alarms_df)
                    alarms_utility.systemAlarm_to_eventhub(alarms_df,eventhub_conctnStr)
                except Exception as e:
                    logging.info("Error: DF to EH:"+str(e))
                
                #--------------- Sending dataframe to SQL ---------------#
                # logging.info('Sending dataframe to SQL')
                alarms_df           =   alarms_df[alarms_df['AS_Id'] != 3]
                
                # logging.info('Sending dataframe to SQL....')
                # logging.info(alarms_df)
                alarms_utility.df_to_SQL_AutoIOT(cursor, cnxn, alarms_df)
            

                #Add ADX Principles
                for _ in range(3):
                    Stage = "Calling Grant ADX Principles"
                    
                    try:
                        # logging.info('granting users admin role..')
                        users = get_site_users(cnxn,site_id)
                        logging.info('list of users are..')
                        logging.info(users)
                        # add_principle(site_name, "iashraf@avanceon.ae", 1,"Admin",days)
                        # add_principle(site_name, "qaiser.hassan@octopusdtl.com", 2,"Admin",days)
                        # add_principle(site_name, "tzahid@avanceon.ae", 3,"Admin",days)
                        # add_principle(site_name, "magha@avanceon.ae", 4,"Admin",days)
                        # logging.info('principles and right given...')
                        # add_principle(site_name, "hassan.raza@octopusdtl.com", 4,"Admin")

                        

                        users_string = ",".join(users)
                        logging.info('list of users string are..')
                        logging.info(users_string)
                        # logging.info('grant permission api called..')
                        call_grant_permission(site_name,users_string)

                        Status = "Complete"
                        # logging.info('granted users admin role..')
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))
                        # logging.info('Error: Add ADX Principles| '+ str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass


                #Log That Execution Finished
                Status = "Finished"
                TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID)

                # logging.info('successful completion sandbox api')
                return func.HttpResponse(json.dumps({"Result":"Success"}))
                # return func.HttpResponse(json.dumps({"Total_Time":Total_Time, "CLI_SQL":TIME_CLI_and_SQL, "EventHub":TIME_EH, "DB":TIME_DB, "Fetch_SQL_Config":TIME_fetch_sql_config, "Rawtags":TIME_realtags, "ADX Principles Add":TIME_ADX_Principles}))
            except Exception as e:
                return func.HttpResponse(json.dumps({"exception":str(e)}))

        else:
            return func.HttpResponse(
                "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
                status_code=200
            )
