import pandas as pd
from azure.eventhub import EventHubProducerClient
import logging
from azure.eventhub import EventData


def get_stateEventId(cnxn, eventName, site_id):
    try:
        sql="SELECT se.isAlarmActive, se.SiteEventID, e.Event_Id, e.EventName, se.AS_Id  from Events e Join SITE_Events_M2M se on se.Event_Id = e.Event_Id where e.EventName = '"+ eventName+"'"+ 'and Site_ID='+str(site_id)
        df = pd.read_sql(sql,cnxn)
        # print(df)
    
    except Exception as e:
        logging.info("Could not get StateEventID for eventName of alarm")

    return df

def get_eventHub_ConnStr(cnxn,_siteId):
    try:
        sql = f"select EventHub_ConnStr from DATA_ALARMS_CONFIG where SITE_ID={_siteId} or (SITE_ID=0 and (select EventHub_ConnStr from DATA_ALARMS_CONFIG where SITE_ID={_siteId}) is null)"
        temp_df = pd.read_sql(sql,cnxn)
        logging.info(sql)
    except Exception as e:
        logging.log("Could not get connection string for EventHub")

    return temp_df

def systemAlarm_to_eventhub(df,con_str):
    df.reset_index(drop=True,inplace=True)
    CONNECTION_STR = con_str
    EVENTHUB_NAME = "dataalarms"
    print(con_str)
    # print(df)
    try:
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR,eventhub_name=EVENTHUB_NAME)
        logging.info('created producer..')
        logging.info(producer)
    except Exception as e:
        raise Exception(str(e))

    try:
        with producer:
            # send_event_data_batch(producer)
            logging.info("creating batch using producer...")
            event_data_batch = producer.create_batch()
            logging.info("Sending Data to Eventhub")
            for i in df.index:
                json_string = df.loc[i].to_json()
                logging.info(json_string)
                event_data_batch.add(EventData(json_string))
            producer.send_batch(event_data_batch)
    except Exception as e:
        logging.info('throwing exception...')
        raise Exception(str(e))

# def df_to_SystemAlarmAction(cursor,cnxn,df):
#     try:
#         for StateEventID in range(0,len(df)):
#             sql_query = "INSERT INTO SYSTEM_ALARM_ACTION (SiteEventID, Reading_Time, SystemAlarm_Description, AS_Id) VALUES ("   +str(df['SiteEventID'][StateEventID])+",'"+str(df['Reading_Time'][StateEventID])+"','"+ str(df['SystemAlarm_Description'][StateEventID])+"',"+str(2)+')'
#             print(sql_query)
#             cursor.execute(sql_query)
#         cnxn.commit()
#         logging.info("DF inserted into SQL successfully")
    
#     except Exception as e:
#         logging.info("DF to SQL ingestion failed: "+ str(e)) 

def df_to_SQL_AutoIOT(cursor,cnxn,df):
    if not df.empty:
        try:
            for StateEventID in range(0,len(df)):
                sql_query = "INSERT INTO SYSTEM_ALARM_ACTION (SiteEventID, Reading_Time, SystemAlarm_Description, AS_Id) VALUES ("   +str(df['SiteEventID'][StateEventID])+",GETDATE(),'"+ str(df['EventName'][StateEventID])+"',"+str(2)+')'
                logging.info(sql_query)
                cursor.execute(sql_query)
            cnxn.commit()
            logging.info("DF inserted into SQL successfully")
        
        except Exception as e:
            logging.info("DF to SQL ingestion failed: "+ str(e)) 
    else:
        logging.info("Error: No Alarms to insert into SQL")
