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
AAD_TENANT_ID = os.environ["AAD_Tenant_Id"]
client_id = os.environ["Client_Id"]
client_secret = os.environ["Client_Secret"]


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


def get_calc_tags_query(site_id, cnxn):
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)


    #create a mapping of actual and mapped tag names
    calc_formulas_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Mapped_Formula'])))
    calc_names_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Calculated_Mapped_Name'])))
    for x in calc_names_mapping:
        calc_names_mapping[x] = [calc_names_mapping[x]]

    keywordprocessor = KeywordProcessor()
    keywordprocessor.add_keywords_from_dict(calc_names_mapping)

    if len(calc_formulas_mapping) == 0:
        print("")

    #Divide tags into those which are dependent solely on raw tags(independent), and other type which are also dependent on calc tags (dependent)
    independent = {}
    dependent = {}
    project_query = "| project TimeStamp"
    for x in calc_formulas_mapping.keys():
        project_query += ',' + x
        if len(re.findall(r'\bC\w+', calc_formulas_mapping[x])) == 0:
            independent[x] = calc_formulas_mapping[x]
        else:
            dependent[x] = calc_formulas_mapping[x]


    extend_query = ""

    if len(independent) != 0:
        extend_query += "| extend "
        for x in independent:
            str1 = ""
            str1 += x + '=' + independent[x]
            extend_query += str1 + ','
        extend_query = extend_query[:-1]   

    if len(dependent) != 0:
        for x in dependent:
            str1 = ""
            str1+= x + '=' + dependent[x]
            str1 = keywordprocessor.replace_keywords(str1)
            extend_query += '\n| extend ' + str1

    return extend_query +'\n'+project_query + '\n}'


def get_update_query(site_name,temp_df):
    list_of_devices = list(temp_df['Site_Specific_Device_Id'].unique())
    
    ingestion_table = "Raw_"+site_name
    create_function = ".create-or-alter function calc_"+site_name+" {"
    min_max_time = "let min_time = toscalar("+ingestion_table+" | summarize todatetime(format_datetime(min(TimeStamp), 'yyyy-MM-dd HH:mm:ss')));\nlet max_time = toscalar("+ingestion_table+" | summarize todatetime(format_datetime(max(TimeStamp), 'yyyy-MM-dd HH:mm:ss')));"
    # print(create_function+"\n"+min_max_time)


    raw_tags_dict = {}
    for x in list_of_devices:
        temp_df2 = temp_df.loc[temp_df['Site_Specific_Device_Id'] == x]
        raw_tags_dict[x] = dict(zip(list(temp_df2['R_Mapped_Name']),['R' + element for element in map(str,list(temp_df2['R_Mapped_By_Device']))]))


    # list_of_devices = list(temp_df['Site_Specific_Device_Id'].unique())
#     list_of_devices = list(raw_tags_dict.keys())
    num_of_devices = len(list_of_devices)
    if len(list_of_devices) > 1:
        join_check = True
    else:
        join_check = False
    close_join = False

 
    self_join_query = ""
    for x in list_of_devices:
        self_join_query += ingestion_table + "\n| where ID == "+str(x)+"\n| evaluate bag_unpack(StatusCode,'unpack_')\n"
        make_series = "| make-series "
        mv_expand = "| mv-expand TimeStamp to typeof(datetime),"
        for y in raw_tags_dict[x]:
            make_series += y + "=avg(todouble(" + raw_tags_dict[x][y] + ")) default=double(NaN),"
            make_series += 'S_' + y + '=take_any(column_ifexists(' + '"unpack_' + raw_tags_dict[x][y] + '",double(NaN))) default=double(NaN),'
            mv_expand += y+"=series_fill_backward(series_fill_forward(" + y+",double(NaN)),double(NaN)) to typeof(real),"
            mv_expand += "S_"+y+"=series_fill_backward(series_fill_backward(S_" + y+",double(NaN)),double(NaN)) to typeof(real),"

        mv_expand = mv_expand[:-1]
        make_series = make_series[:-1]
        make_series += " on TimeStamp from min_time to (max_time+1s) step 1s"
        self_join_query += make_series + "\n" + mv_expand + "\n" 

        if close_join == True:
            self_join_query += ") on $left.TimeStamp == $right.TimeStamp\n| extend TimeStamp = iif(isnull(TimeStamp),TimeStamp1,TimeStamp)\n| project-away TimeStamp1\n"
            if num_of_devices <= 1:
                join_check = False
        if join_check == True:
            self_join_query += "| join kind=fullouter (" + "\n"
            close_join = True

        num_of_devices -= 1


    return create_function + "\n" + min_max_time + "\n" + self_join_query


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
        return ""


    sql = """
    SELECT * FROM Manual_Fix_Points
    WHERE Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    man_mapping = dict(zip(temp_df["MF_Mapped_Name"], temp_df["Value"]))

    query = "| extend "
    for x in man_list:
        query += x + "=toreal(" + str(man_mapping[x]) + "),"
        query += "S_" + x + "=1,"

    query = query[:-1]
    return query + '\n'



def calculated_tags(cnxn, site_id,calc_tags_list):
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE  StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    
    code_keywords = {}

    #create a mapping of actual and mapped tag names
    calc_formulas_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Mapped_Formula'])))
    calc_names_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Calculated_Mapped_Name'])))
    for x in calc_names_mapping:
        # storing a reverse dictionary for calc tag names for Status Code calculation
        code_keywords[calc_names_mapping[x]] = x
        calc_names_mapping[x] = [calc_names_mapping[x]]

    keywordprocessor = KeywordProcessor()
    keywordprocessor.add_keywords_from_dict(calc_names_mapping)

    if len(calc_formulas_mapping) == 0:
        return ""

    #Divide tags into those which are dependent solely on raw tags(independent), and other type which are also dependent on calc tags (dependent)
    independent = {}
    dependent = {}
    independent_tags = {}
    dependent_tags = {}
    code_keywords2 = {}
    pack_list = []


    project_query = "| project TimeStamp,Calc_Status_Code"
    for x in calc_formulas_mapping.keys():
        # project_query += ',' + x
        if len(re.findall(r'\bC\w+', calc_formulas_mapping[x])) == 0:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            calc_formulas_tags = ["S_" + tags for tags in calc_formulas_tags]
            independent_tags[x] = calc_formulas_tags 
            independent[x] = calc_formulas_mapping[x]
        else:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            calc_formulas_tags = ["S_" + tags for tags in calc_formulas_tags]
            calc_derived_tags = re.findall(r'\bC\w+', calc_formulas_mapping[x])
            for tags in calc_derived_tags:
                code_keywords2[code_keywords[tags]+"_Code"] = [tags + "_Code"]
            calc_derived_tags = [tags + "_Code" for tags in calc_derived_tags]
            calc_formulas_tags = calc_formulas_tags + calc_derived_tags
            dependent_tags[x] = calc_formulas_tags
            dependent[x] = calc_formulas_mapping[x]
    # # logging.info(independent_tags)
    # # logging.info(independent)
    keywordprocessor_codes = KeywordProcessor()
    keywordprocessor_codes.add_keywords_from_dict(code_keywords2)

    extend_query = ""
    # project_query = "| project TimeStamp,ID=toint(1)"
    for tag in  calc_tags_list:
            if tag not in calc_formulas_mapping.keys():
                project_query += ',' + tag + '=real(NaN)'
            else:
                project_query += ',' + tag 
    
    if len(independent) != 0:
        extend_query += "| extend "
        for x in independent:
            str1 = ""
            str1 += x + '=' + independent[x]
            str1 += "," + x + "_Code=" + "*".join(independent_tags[x])
            extend_query += str1 + ','
            pack_list.append(x+"_Code")
            # project_query += ',' + str1.split('=')[0]
        extend_query = extend_query[:-1]   

    if len(dependent) != 0:
        for x in dependent:
            str1 = ""
            str1+= x + '=' + dependent[x]
            str1 += "," + x + "_Code=" + "*".join(dependent_tags[x])
            str1 = keywordprocessor.replace_keywords(str1)
            str1 = keywordprocessor_codes.replace_keywords(str1)
            extend_query += '\n| extend ' + str1
            pack_list.append(x+"_Code")
            # project_query += ',' + str1.split('=')[0]
    
    # query for packing the columns into property bag
    extend_pack = ""

    if( len(pack_list) !=0):
        extend_pack +="| extend Calc_Status_Code = pack("
        for code_name in pack_list:
            str1 = ""
            str1 += '"' + code_name + '",' + code_name + ","
            extend_pack += str1
        extend_pack = extend_pack[:-1]
        extend_pack += ")"

    # project_query += ", Calc_Status_Code"

    return extend_query +'\n'+extend_pack+'\n'+project_query + "}"
       
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



def alter_calc_query(cnxn, site_name, site_id, kusto_client):
    #Get All Calc Tags
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    
    #Get current tags already present in Calc Table in ADX
    get_schema_query = "Calc_"+site_name+"\n| getschema\n| project ColumnName"
    df = kusto_execute(site_name, get_schema_query,kusto_client, return_df=True)
    
    #Alter query for new tags
    alter_tags = []
    for x in list(temp_df['C_Tag_Name']):
        if x not in list(df['ColumnName']):
            alter_tags.append(x)
    
    calc_tags = list(df['ColumnName'])
    calc_tags.remove("TimeStamp")
    calc_tags.remove("Calc_Status_Code")

    calc_tags = calc_tags + alter_tags
    calc_extra_tags =[]
    for tag in calc_tags:
        if tag not in list(temp_df['C_Tag_Name']):
            calc_extra_tags.append(tag)

    if len(alter_tags) == 0:
        return "",calc_tags

    alter_query = ".alter-merge table Calc_"+site_name+" ("
    for x in alter_tags:
        alter_query += x + ":real,"
    alter_query = alter_query[:-1]
    alter_query += ")"
    return alter_query,calc_tags


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
    man_query = ".set-or-replace Man_"+site_name+" <|\nevaluate sql_request(\n'Server=tcp:octopusdigitalsql.database.windows.net,1433;'\n'Initial Catalog="+database_name+";'\nh'User ID=octopusdigital;'\nh'Password="+os.environ["Database_Key"]+";',"
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
                    call_iotedgemapping(site_name,database_name)
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
            if alter_man_table != "":
                for _ in range(3):
                    Stage = "Alter Man Function"
                    try:
                        kusto_execute(site_name, alter_man_table, kusto_client)
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

            #Update Man Function
            if update_man_table != "":
                for _ in range(3):
                    Stage = "Update Man Function"
                    try:
                        kusto_execute(site_name, update_man_table, kusto_client)
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass


            #===================================================================
            #Calc Queries
            #===================================================================

            #Get List of Raw Tags being used in formulas
            raw_df = get_raw_df(site_id,cnxn)
            #Get query of Man Tags being used in formulas
            man_interpol_query = get_man_interpol_query(cnxn, site_id)
            #Get Interpolation Query
            # interpolation_string = interpolation_query(raw_list)
            
            calc_alter_query,calc_tags_list = alter_calc_query(cnxn, site_name, site_id, kusto_client)

            #Get Calc tag formula query
            calculated_string = calculated_tags(cnxn, site_id,calc_tags_list)
            calc_project_extend_query = calculated_tags(cnxn, site_id,calc_tags_list)
            # # logging.info(calculated_string)
            # calc_project_extend_query = get_calc_tags_query(site_id,cnxn)
            #Calc Update Function Query
            calc_function_query = get_update_query(site_name, raw_df)
            # calc_function_query = ".create-or-alter function  calc_"+site_name+ "{\nRaw_"+site_name+"\n| join kind=leftouter\n(Man_"+site_name+")\non $left.ID == $right.Id"
            #Final Query for Calc
            calc_query = calc_function_query+man_interpol_query+calc_project_extend_query
            # # logging.info("calc_query: " + calc_query)
            # # logging.info("calc_alter"+calc_alter_query)
            #Alter Query for Calc

            #Add new calc tags to calc table schema
            if calc_alter_query != "":
                for _ in range(3):
                    Stage = "Add new Calc Tags to Calc Table Schema"
                    try:
                        kusto_execute(site_name, calc_alter_query, kusto_client)
                        Status = "Complete"
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))                  
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

            #Alter Calc Function
            if calculated_string != "":
                for _ in range(3):
                    Stage       = "Alter Calc Function"
                    EventName   = "Update ADX Update Policy"
                    try:
                        kusto_execute(site_name, calc_query, kusto_client)
                        Status = "Complete"
                        temp_DF     = alarms_utility.get_stateEventId(cnxn, EventName,site_id)

                        if not temp_DF.empty:
                            now            = datetime.datetime.utcnow()
                            formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                            new_row = {'EventName':EventName, 'StateEventID':temp_DF.iloc[0]['SiteEventID'], 'Event_Id':temp_DF.iloc[0]['Event_Id'], 'AlarmType':'System Alarms', 'isAlarmActive':temp_DF.iloc[0]['isAlarmActive'],'Reading_Time': formatted_date}
                            alarms_df = alarms_df.append(new_row, ignore_index=True)
                            # logging.info(alarms_df)
                            
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage) 
                        break
                    except Exception as e:
                        TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                        Status = "Error"
                        log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage,Error=str(e))
                        time.sleep(10)
                else:
                    TS = datetime.datetime.now(pytz.timezone('Asia/Karachi'))
                    Status = "Failed"
                    log_entry(cursor,cnxn,TS,site_id,Type,Status,Exec_ID,Stage=Stage)
                    pass

            #--------------- Sending dataframe to EventHub ---------------# 
            if not alarms_df.empty:
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

            return func.HttpResponse(json.dumps({"Result":"Success"}))
            # return func.HttpResponse(json.dumps({"calc_query":calc_query, "calc_alter_query":calc_alter_query, "Alter_Man":alter_man_table, "Update_Man":update_man_table}))

        except Exception as e:
            return func.HttpResponse(json.dumps({"exception":str(e)}))
    
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )