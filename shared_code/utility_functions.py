from azure.cli.core import get_default_cli
import pandas as pd


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

#Azure CLI Function
def az_cli (args_str):
    args = args_str.split()
    cli = get_default_cli()
    cli.invoke(args)
    if cli.result.result:
        return cli.result.result
    elif cli.result.error:
        raise cli.result.error
    return True


def get_site_id(cnxn, site_name):
    sql = "SELECT Site_Id FROM Plant_Site WHERE AutomationTableName = '"+site_name+"'"
    temp_df = pd.read_sql(sql,cnxn)
    site_id = temp_df['Site_Id'][0]
    return site_id