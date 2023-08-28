import pandas as pd
from flashtext import KeywordProcessor
import re
def alter_calc_query(cnxn, site_name, site_id, kusto_client):
    #Get All Calc Tags
    sql = """
    SELECT * FROM Calculated_Tags
    WHERE StatisticalRefrenceId is NULL and Site_Id_FK = """+str(site_id)+"""
    """
temp_df=pd.read_json("Results.json")
df=pd.read_json("CalTags.json")
#Alter query for new tags
alter_tags = []
for x in list(temp_df['C_Tag_Name']):
    if x not in list(df['ColumnName']):
        alter_tags.append(x)

calc_tags = list(df['ColumnName'])
calc_tags.remove("TimeStamp")
calc_tags.remove("Calc_Status_Code")

calc_tags = calc_tags + alter_tags
code_keywords = {}

#create a mapping of actual and mapped tag names
calc_formulas_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Mapped_Formula'])))
calc_names_mapping = dict(zip(list(temp_df['C_Tag_Name']), list(temp_df['Calculated_Mapped_Name'])))
print(calc_formulas_mapping)
print(calc_names_mapping)
print()
for x in calc_names_mapping:
    # storing a reverse dictionary for calc tag names for Status Code calculation
    code_keywords[calc_names_mapping[x]] = x
    calc_names_mapping[x] = [calc_names_mapping[x]]
print(code_keywords)
print(calc_names_mapping)


keywordprocessor = KeywordProcessor()
keywordprocessor.add_keywords_from_dict(calc_names_mapping)


print()
print()
print()
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

keywordprocessor_codes = KeywordProcessor()
keywordprocessor_codes.add_keywords_from_dict(code_keywords2)



extend_query = ""
calc_tags_list=calc_tags
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

print(extend_query +'\n\n'+extend_pack+'\n\n'+project_query + "}")
