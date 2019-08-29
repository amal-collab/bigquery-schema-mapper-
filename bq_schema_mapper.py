from google.cloud import bigquery

import pandas



client = bigquery.Client.from_service_account_json('/Users/amalanandmuthukumaran/PycharmProjects/payments_pull/Credentials/service_account.json')

datasetID = 'csv_content'


#get all IDs
#arrange IDs in order of name
#for each ID, get 1)number of rows 2)Number of columns 3)column names 4)Total amount of money, if applicable




tables = client.list_tables(datasetID)
tableIDs = []
print("Tables contained in '{}':".format(datasetID))
for table in tables:
    print(table.table_id)
    tableIDs.append(str(table.table_id))



overallDetailsArray = []

headerArray = ['Name','Row Count','Column Count','Column Names']
overallDetailsArray.append(headerArray)

for tableID in sorted(tableIDs):
    tableIdDetailsArray = []
    table_ref = client.dataset(datasetID).table(tableID)
    table = client.get_table(table_ref)

    tableIdDetailsArray.append(tableID) #Table ID
    tableIdDetailsArray.append(table.num_rows) #number of rows

    finalSchemaArray = []
    for SchemaField in table.schema:
        finalSchemaArray.append(SchemaField.to_api_repr())
    tableIdDetailsArray.append(len(finalSchemaArray)) #number of columns
    for schema in finalSchemaArray:
        tableIdDetailsArray.append(schema["name"]) #columns

    overallDetailsArray.append(tableIdDetailsArray)



for row in overallDetailsArray:
    print(row)

pd = pandas.DataFrame(overallDetailsArray)
pd.to_csv("mylist.csv")
