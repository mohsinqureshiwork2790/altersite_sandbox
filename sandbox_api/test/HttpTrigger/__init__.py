import logging
from pandas.core.base import NoNewAttributesMixin
import azure.functions as func
import json
import time
import pandas as pd
import pytz
import re
import pyodbc
import datetime
import os
import uuid
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
# from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.cli.core import get_default_cli
from flashtext import KeywordProcessor
from shared_code import alarms_utility
import requests


#ADX APP Credentials
AAD_TENANT_ID = "0f34cb0d-38ea-428d-8114-6486d17b9df5"
client_id = "1813de48-2733-4220-ab74-7804859f8fb6"
client_secret ="cHi8Q~xFi3Sp2D6BvUFBV.Ybh_VJwwnQBe4tGaLg"
server = 'tcp:octopusdigitalsql.database.windows.net'
database = ''#'OMNIConnect_Staging' #'DemoOmniconnect'
username = 'octopusdigital' 
# password = os.environ["Database_Key"]
password = 'avanceon@786'

# #Establish Kusto Connection
# KUSTO_CLUSTER = "https://pocadx2.southeastasia.kusto.windows.net/"
# KUSTO_DATABASE = "adxDB"
# KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(KUSTO_CLUSTER, client_id, client_secret, AAD_TENANT_ID)
# KUSTO_CLIENT = KustoClient(KCSB)


def get_site_id(cnxn, site_name):
    sql = "SELECT Site_Id FROM Plant_Site WHERE IsProductionENV =0 and AutomationTableName = '"+site_name+"'"
    temp_df = pd.read_sql(sql,cnxn)
    site_id = temp_df['Site_Id'][0]
    return site_id


def get_site_name(cnxn, site_id):
    sql = "SELECT AutomationTableName FROM Plant_Site WHERE IsProductionENV =0 and Site_Id = '"+str(site_id)+"'"
    temp_df = pd.read_sql(sql,cnxn)
    site_name = temp_df['AutomationTableName'][0]
    return site_name


#CALC FUNCTIONS
# def get_raw_ingredients(cnxn, site_id):
#     sql = """
#     SELECT * FROM Calculated_Tags
#     WHERE Site_Id_FK = """+str(site_id)+"""
#     """
#     temp_df = pd.read_sql(sql,cnxn)
    
    
#     raw_list = []
#     for x in list(temp_df['Mapped_Formula']):
#         temp_list = re.findall(r'\bR\w+', x)
#         for y in temp_list:
#             if y not in raw_list:
#                 raw_list.append(y)
#     raw_list.sort()
#     return raw_list

def get_raw_df(site_id,cnxn):
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)


    raw_list = []
    for x in list(temp_df['Mapped_Formula']):
        temp_list = re.findall(r'\bR\w+', x)
        for y in temp_list:
            if y not in raw_list:
                raw_list.append(y)
    raw_list.sort()
    # logging.info('raw_list are as follows:..')
    # logging.info(raw_list)

    raw_list_string = "("
    for x in raw_list:
        raw_list_string += "'" + x + "',"
    raw_list_string = raw_list_string[:-1]
    raw_list_string += ")"
    # logging.info('raw_list string are as follows:..')
    # logging.info(raw_list_string)
    #Get Dataframe
    sql = """SELECT * FROM Real_Raw_Points WHERE Site_Id_FK = """+str(site_id)+""" AND R_Mapped_Name in """ + raw_list_string
    temp_df = pd.read_sql(sql,cnxn)

    return temp_df

def get_update_query(site_name,temp_df):
    list_of_tags = list(temp_df['R_Mapped_Name'].unique())
    
    ingestion_table = "Raw_"+site_name+"_long"
    create_function = ".create-or-alter function calc_"+site_name+"_long() {"
    #min_max_time = "let min_time = toscalar("+ingestion_table+" | summarize todatetime(format_datetime(min(TimeStamp), 'yyyy-MM-dd HH:mm:ss')));\nlet max_time = toscalar("+ingestion_table+" | summarize todatetime(format_datetime(max(TimeStamp), 'yyyy-MM-dd HH:mm:ss')));"
    # print(create_function+"\n"+min_max_time)
    tags="("
    for x in list_of_tags:
        tags+='"'+x+'"'
    tags+=")"
    create_function+=ingestion_table+"|where TagName in "+tags 
    print(create_function)
    return create_function

def get_man_interpol_query(cnxn, site_id):
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    man_list = []
    for x in list(temp_df['Mapped_Formula']):
        temp_list = re.findall(r'\bM\w+', x)
        for y in temp_list:
            if y not in man_list:
                man_list.append(y)
    man_list.sort()
    if len(man_list) == 0:
        return "",""
    sql = """
    SELECT * FROM Manual_Fix_Points
    WHERE Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    man_mapping = dict(zip(temp_df["MF_Mapped_Name"], temp_df["Value"]))

    query = ","
    query2= ","
    for x in man_list:
        query += "\""+x + "\",toreal(" + str(man_mapping[x]) + "),"
        query2 +="\""+x + "\",1,"

    query = query[:-1]
    query2 = query2[:-1]
    return query,query2


def calculated_tags(cnxn, site_id):
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE  StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    
    code_keywords = {}

    #create a mapping of actual and mapped tag names
    calc_formulas_mapping = dict(zip(list(temp_df['Calculated_Mapped_Name']), list(temp_df['Mapped_Formula'])))
    calc_names_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Calculated_Mapped_Name'])))
    for x in calc_names_mapping:
        # storing a reverse dictionary for calc tag names for Status Code calculation
        code_keywords[calc_names_mapping[x]] = x
        calc_names_mapping[x] = [calc_names_mapping[x]]

    keywordprocessor = KeywordProcessor()
    keywordprocessor.add_keywords_from_dict(calc_names_mapping)

    if len(calc_formulas_mapping) == 0:
        return ""

    independent = {}
    dependent = {}
    independent_tags = {}
    dependent_tags = {}
    code_keywords2 = {}

    for x in calc_formulas_mapping.keys():
        # project_query += ',' + x
        if len(re.findall(r'\bC\w+', calc_formulas_mapping[x])) == 0:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            independent_tags[code_keywords[x]] = ["toint(status[\""+tags+"\"])" for tags in calc_formulas_tags]
            pattern = re.compile(r'(R|M)(\d+)')
            independent[code_keywords[x]] = pattern.sub(r'toreal(CalTags["\1\2"])', calc_formulas_mapping[x])
            
        else:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            calc_formulas_tags = ["toint(status[\""+tags+"\"])" for tags in calc_formulas_tags]
            calc_derived_tags = re.findall(r'\bC\w+', calc_formulas_mapping[x])
            for tags in calc_derived_tags:
                code_keywords2[code_keywords[tags]+""] = [tags + ""]
            calc_derived_tags = ["toint(Calstatus[\""+tags + "\"])" for tags in calc_derived_tags]
            calc_formulas_tags = calc_formulas_tags + calc_derived_tags
            dependent_tags[x] = calc_formulas_tags
            pattern = re.compile(r'(C)(\d+)')
            dependent[x] = pattern.sub(r'toreal(CalTags["\1\2"])', calc_formulas_mapping[x])
        # # logging.info(independent_tags)
        # # logging.info(independent)
        keywordprocessor_codes = KeywordProcessor()
        keywordprocessor_codes.add_keywords_from_dict(code_keywords2)
            
        extend_query = ""
        extend_query2=""
        project_query = "|mv-expand bagexpansion=array CalTags|extend TagName=CalTags[0],Value=CalTags[1],StautsCode=Calstatus[tostring(CalTags[0])]|project TimeStamp=todatetime(TimeStamp),TagName=tostring(TagName),Value,StautsCode=toint(StautsCode)"
        if len(independent) != 0:
            extend_query += "|extend CalTags=bag_pack(\"\",\"\"),Calstatus=bag_pack(\"\",\"\") |project  TimeStamp,CalTags=bag_merge(CalTags,bag_pack("
            extend_query2+= ""
            for x in independent:
                str1 = "\""
                str1 += x + '\",' + independent[x]
                str2 = "\""+x + "\"," + "*".join(independent_tags[x])
                extend_query += str1 + ','
                extend_query2+= str2+','
                
                # project_query += ',' + str1.split('=')[0]
            extend_query = extend_query[:-1]
            extend_query2 = extend_query2[:-1]
            extend_query+="))"+",Calstatus=bag_merge(status,bag_pack("+extend_query2+"))"

        if len(dependent) != 0:
            for x in dependent:
                str1 = "\""
                str1+= x + "\"," + dependent[x]
                str2 = "\"" + x + "\"," + "*".join(dependent_tags[x])
                str1 = keywordprocessor.replace_keywords(str1)
                str2=keywordprocessor.replace_keywords(str2)
                str2 = keywordprocessor_codes.replace_keywords(str2)
                extend_query += '| project TimeStamp,CalTags=bag_merge(CalTags,bag_pack(' + str1+")),Calstatus=bag_merge(Calstatus,bag_pack("+str2+"))"
    return extend_query+project_query+"}"
       
    # #Replace Mapped name with actual tag names
    # calculations_list = []
    # for x in calc_formulas_mapping:
    #     str1 = ""
    #     str1+= x + '=' + calc_formulas_mapping[x]
    #     str1 = keywordprocessor.replace_keywords(str1)
    #     calculations_list.append(str1)
        
    # #Kusto Query
    # extend_query = ""
    # project_query = "| project TimeStamp,ID=toint(1)"
    # for x in calculations_list:
    #     extend_query += '| extend ' + x + '\n'
    #     project_query += ',' + x.split('=')[0]
    # return extend_query[:-1] +'\n'+project_query



#MANUAL FUNCTIONS
def get_manual_queries(cnxn, site_name, site_id, database_name):
    sql = """
    SELECT * FROM Manual_Fix_Points
    WHERE Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    manual_mapped_list = list(temp_df['MF_Mapped_Name'])

    if len(manual_mapped_list) == 0:
        return "",""
    
    alter_query = ".alter table Man_"+site_name+"(Id:int,"
    for x in manual_mapped_list:
        alter_query += x + ":real,"
    alter_query = alter_query[:-1]
    alter_query += ")"
    
    sql_query = """
    'SELECT 1 AS Id,"""+','.join(manual_mapped_list)+""" FROM (SELECT Value, MF_Mapped_Name FROM Manual_Fix_Points) d PIVOT (MAX(Value) FOR MF_Mapped_Name IN ("""+','.join(manual_mapped_list)+""")) piv;')
    """
    man_query = ".set-or-replace Man_"+site_name+" <|\nevaluate sql_request(\n'Server=tcp:octopusdigitalsql.database.windows.net,1433;'\n'Initial Catalog="+database_name+";'\nh'User ID=octopusdigital;'\nh'Password="+password+";',"
    return alter_query, man_query+sql_query

def call_iotedgemapping(site_name,database_name):
    try:
        requests.get(f'https://generalsandboxapis.azurewebsites.net/api/anydevice_ingestion?site_name={site_name}&database_name={database_name}')
    except requests.exceptions.ReadTimeout: 
        pass

def kusto_execute(db,query,kusto_client,return_df=False):
    response = kusto_client.execute(db, query)
    if return_df == True:
        return dataframe_from_result_table(response.primary_results[0])


def log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID, Stage="", Error=""):
    if Error != "":
        Error = eval(Error)
        Error = Error[0]['error']['@message'].replace("'", "")

    sql_query = """
    INSERT INTO Automation_Logs (TimeStamp, Site_Id, Type_of_execution, Status, Stage, Error, Execution_ID)
    VALUES ('"""+str(TS)+"""','"""+str(site_id)+"""','"""+Type+"""','"""+Status+"""','"""+Stage+"""','"""+str(Error)+"""','"""+Exec_ID+"""')
    """
    cursor.execute(sql_query)
    cnxn.commit()   



def main(req: func.HttpRequest) -> func.HttpResponse:
    # logging.info('Python HTTP trigger function processed a request.')
    global database
    try:
        site_id = int(req.params.get('site_id'))
        database_name = req.params.get('database_name')

        column_names    = ["SystemAlarm_Description", "AlarmType", "StateEventID","Event_Id","IsAlarmActive","Reading_Time"]
        alarms_df       = pd.DataFrame(columns = column_names)

        logging.info(database_name)
        
        if site_id and database_name:
            try:
                #Connect to SQL
                server = 'tcp:octopusdigitalsql.database.windows.net'
                database = database_name #'OMNIConnect_Staging' #'DemoOmniconnect'
                username = 'octopusdigital' 
                # password = os.environ["Database_Key"]
                password = 'avanceon@786'
                # password = os.environ["Database_Key"]
                cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
                cursor = cnxn.cursor()
                # logging.info('sql server db connected...')
                #Connect to Kusto Client
                cluster = "https://sandboxadx.southeastasia.kusto.windows.net/" 
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, AAD_TENANT_ID)
                kusto_client = KustoClient(kcsb)
                # logging.info('kusto db sandbox db connected...')
                
                #Data for logging into SQL
                Exec_ID = uuid.uuid4().hex
                Type = "Alter Site"

                #Log That Execution Started
                Status = "Started"
                TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID)


                #Get Site Name
                site_name = get_site_name(cnxn, site_id)
                logging.info(site_name)
                #Change mapping for iot devices
                for _ in range(3):
                    Stage       =   "Calling anydevice ingestion mapping"
                    logging.info(Stage)
                    try:
                        logging.info("try")
                        #call_iotedgemapping(site_name,database_name)
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        # log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        logging.info(Status)
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        # logging.info('Error: Alarm Engine Automation'+ str(e))   
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(5)
                        logging.info(Status)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

                #===================================================================
                #Manual Queries
                #===================================================================

                #Get both "Alter Table" and "Evaluate SQL" queries
                alter_man_table,update_man_table = get_manual_queries(cnxn, site_name, site_id, database_name)
                
                #Alter Man Table
                # if alter_man_table != "":
                #     for _ in range(3):
                #         Stage = "Alter Man Function"
                #         try:
                #             kusto_execute(site_name, alter_man_table, kusto_client)
                #             Status = "Complete"
                #             TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #             log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                #             break
                #         except Exception as e:
                #             TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #             Status = "Error"
                #             log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                #             time.sleep(10)
                #     else:
                #         TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #         Status = "Failed"
                #         log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                #         pass

                # #Update Man Function
                # #if update_man_table != "":
                #     for _ in range(3):
                #         Stage = "Update Man Function"
                #         try:
                #             kusto_execute(site_name, update_man_table, kusto_client)
                #             Status = "Complete"
                #             TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #             log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                #             break
                #         except Exception as e:
                #             TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #             Status = "Error"
                #             log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                #             time.sleep(10)
                #     else:
                #         TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                #         Status = "Failed"
                #         log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                #         pass


                #===================================================================
                #Calc Queries
                #===================================================================
                                
                raw_df = get_raw_df(site_id,cnxn)
                man_interpol_query,man_interpol_status= get_man_interpol_query(cnxn, site_id)
                #logging.error("man_interpol_query:",str(raw_df.to_string()))
                calc_project_extend_query = calculated_tags(cnxn, site_id)
                calc_function_query = get_update_query(site_name, raw_df)
                        
                calc_query = calc_function_query+"|summarize info=make_bag(bag_pack(TagName, Value"+man_interpol_query+")),status=make_bag(bag_pack(TagName,StatusCode"+man_interpol_status+"))by TimeStamp,ID"+calc_project_extend_query
                calc_query.replace("|","\n|")
                calc_query=calc_query.replace("|","\n|")
                logging.info(calc_query )
                

                #logging.error("FINAL QUERY: ",calc_query)
                #logging.error("FINAL QUERY: ")
                #--------------- Sending dataframe to EventHub ---------------# 
                if not alarms_df.empty and False:
                    eventhub_conctnStr  =   alarms_utility.get_eventHub_ConnStr(cnxn, site_id)
                    eventhub_conctnStr  =   eventhub_conctnStr['EventHub_ConnStr'][0]
                    alarms_df           =   alarms_df[alarms_df['isAlarmActive'] == True]
                    alarms_df['DataAlarmActionGUID'] = [str(uuid.uuid4()) for x in range(len(alarms_df))]
                    alarms_df           =   alarms_df[['DataAlarmActionGUID','SystemAlarm_Description','Reading_Time','StateEventID','AlarmType']]
                    print(alarms_df)
                    try:
                        alarms_utility.systemAlarm_to_eventhub(alarms_df,eventhub_conctnStr)
                    except Exception as e:
                        print("Error: DF to EH:"+str(e))
                    
                    #--------------- Sending dataframe to SQL ---------------#
                    
                    alarms_utility.df_to_SQL_AutoIOT(cursor, cnxn, alarms_df)
                else:
                    logging.info("Dataframe is empty. Cannot push to EventHub/SQL")

                #Log That Execution Finished
                Status = "Finished"
                TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID)

                return func.HttpResponse(calc_query)
                # return func.HttpResponse(json.dumps({"calc_query":calc_query, "calc_alter_query":calc_alter_query, "Alter_Man":alter_man_table, "Update_Man":update_man_table}))

            except Exception as e:
                
                return func.HttpResponse(json.dumps({"Exception Type:": e.args[0]}))
        
        else:
            return func.HttpResponse(
                "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
                status_code=200
            )
    except:
        return func.HttpResponse(
                "MASLA SOME WHERE",
                status_code=200
            )