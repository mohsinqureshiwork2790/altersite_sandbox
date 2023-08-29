import pandas as pd
import re
def get_man_interpol_query(cnxn, site_id):
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
    sql = """
    SELECT * FROM Manual_Fix_Points
    WHERE Site_Id_FK = """+str(site_id)+"""
    """
    temp_df = pd.read_sql(sql,cnxn)
    man_mapping = dict(zip(temp_df["MF_Mapped_Name"], temp_df["Value"]))

    query = ""
    query2= ""
    for x in man_list:
        query += x + ",toreal(" + str(man_mapping[x]) + "),"
        query2 += x + ",1,"

    query = query[:-1]
    query2 = query2[:-1]
    return query,query2

print(get_man_interpol_query(None,None))
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

    independent = {}
    dependent = {}
    independent_tags = {}
    dependent_tags = {}
    code_keywords2 = {}

    for x in calc_formulas_mapping.keys():
        # project_query += ',' + x
        if len(re.findall(r'\bC\w+', calc_formulas_mapping[x])) == 0:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            independent_tags[x] = ["toint(status[\""+tags+"\"])" for tags in calc_formulas_tags]
            pattern = re.compile(r'(R|M)(\d+)')
            independent[x] = pattern.sub(r'toreal(info["\1\2"])', calc_formulas_mapping[x])
            
        else:
            calc_formulas_tags = re.findall(r'\bR\w+|\bM\w+', calc_formulas_mapping[x])
            calc_formulas_tags = ["status[\""+tags+"\"]" for tags in calc_formulas_tags]
            calc_derived_tags = re.findall(r'\bC\w+', calc_formulas_mapping[x])
            for tags in calc_derived_tags:
                code_keywords2[code_keywords[tags]+""] = [tags + ""]
            calc_derived_tags = [tags + "" for tags in calc_derived_tags]
            calc_formulas_tags = calc_formulas_tags + calc_derived_tags
            dependent_tags[x] = calc_formulas_tags
            pattern = re.compile(r'(R|M)(\d+)')
            dependent[x] = pattern.sub(r'toreal(info["\1\2"])', calc_formulas_mapping[x])
        # # logging.info(independent_tags)
        # # logging.info(independent)
        
        extend_query = ""
        extend_query2=""
        project_query = "|project TimeStamp,TagName,Value,StautsCode"
        if len(independent) != 0:
            extend_query += "|extend CalTags=bag_pack(\"\",\"\"),Calstatus=bag_pack(\"\",\"\") |project  TimeStamp,,CalTags=bag_merge(CalTags,bag_pack("
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
                str2 = "" + x + "," + "*".join(dependent_tags[x])
                str1 = keywordprocessor.replace_keywords(str1)
                str2 = keywordprocessor_codes.replace_keywords(str2)
                extend_query += '\n| projectTimeStamp,CalTags=bag_merge(CalTags,bag_pack(' + str1+")),Calstatus=bag_merge(Calstatus,bag_pack("+str2+"))"
    return extend_query+"|mv-expand bagexpansion=array CalTags|extend TagName=CalTags[0],Value=CalTags[1],StautsCode=Calstatus[tostring(CalTags[0])]}"



raw_df = get_raw_df(site_id,cnxn)

man_interpol_query,man_interpol_status= get_man_interpol_query(cnxn, site_id)

calc_project_extend_query = calculated_tags(cnxn, site_id,calc_tags_list)
calc_function_query = get_update_query(site_name, raw_df)
           
calc_query = calc_function_query+"|summarize info=make_bag(bag_pack(TagName, Value,"+man_interpol_query+")),status=make_bag(bag_pack(TagName,StatusCode,"+man_interpol_status+"))by TimeStamp,ID"+calc_project_extend_query
           




