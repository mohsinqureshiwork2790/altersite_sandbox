import pandas as pd
import re
def get_man_interpol_query(cnxn, site_id):
    """
    The function `get_man_interpol_query` retrieves a list of mapped formulas from a database and
    generates a query string and status string based on the formulas and their corresponding values.
    
    :param cnxn: The parameter `cnxn` is a database connection object that is used to establish a
    connection to a database. It is typically created using a database driver and contains information
    such as the database server address, username, password, and other connection details
    :param site_id: The `site_id` parameter is the ID of the site for which you want to retrieve the
    interpolated data
    :return: The function `get_man_interpol_query` returns two values: `query` and `status`.
    """
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
    #temp_df = pd.read_sql(sql,cnxn)
    temp_df=pd.read_json("Results.json")
    man_list = []
    for x in list(temp_df['Mapped_Formula']):
        temp_list = re.findall(r'\bM\w+', x)
        for y in temp_list:
            if y not in man_list:
                man_list.append(y)
    man_list.sort()
    if len(man_list) == 0:
        return ""
    temp_df = pd.read_sql(sql,cnxn)
    man_mapping = dict(zip(temp_df["MF_Mapped_Name"], temp_df["Value"]))

    query = ""
    status=""
    for x in man_list:
        query += x + ",toreal(" + str(man_mapping[x]) + "),"
        status += x+",status[\""+x+"\"],"

    query = query[:-1]
    status = status[:-1]
    return query,status

print(get_man_interpol_query(None,None))