# copilot revisions June 2026
import pandas as pd
import sempy.fabric as fabric
from pyspark.sql.functions import *

save_output_to_table = True;          # change me, to True if you want to write to a table
output_table = "shortcutstable"       # change me, output table name

# This works for everything except shortcut items
client = fabric.FabricRestClient() 

# GET token
def get_token(audience="pbi"):
    return notebookutils.credentials.getToken(audience)

# Have to use to get shortcuts
client2 = fabric.FabricRestClient(token_provider=get_token) # 

# Final output
final_df = spark.createDataFrame(data=[],
schema="""
workspace_name string, 
workspace_id long, 
lakehouse_name string,
lakehouse_id long,
name string, 
path string
""")

# Create a list to store DataFrames
dfs_to_union = [final_df]

# Loop through all workspaces, lakehouses and find shortcut items
workspaces = fabric.list_workspaces()
df1 = spark.createDataFrame(workspaces)
ws = df1.select("Name","Id").collect()
print("progress:",end="")

for w in ws:
    if w.Name != "Admin monitoring":
        print(".",end="")
        ws_items = fabric.list_items(workspace=w.Id)
        if ws_items.size > 0 :
            df2 = spark.createDataFrame(ws_items).filter(col("Type")=="Lakehouse")
            lhs = df2.select(col("Display Name").alias("displayName"),"Id").collect()
            for lh in lhs:
                response = client2.get(f"/v1/workspaces/{w.Id}/items/{lh.Id}/shortcuts")
                sc_items = pd.json_normalize(response.json()['value'])
                if not sc_items.empty:
                    df3 = spark.createDataFrame(sc_items)                
                    df4 = (df3.toDF(*[col.replace('.', '_') for col in df3.columns])
                        .withColumn("workspace_name",lit(w.Name))
                        .withColumn("workspace_id",lit(w.Id))
                        .withColumn("lakehouse_name",lit(lh.displayName))
                        .withColumn("lakehouse_id",lit(lh.Id))
                    )
                    dfs_to_union.append(df4)  # Add to list instead of unioning immediately

print("done")

# Union all DataFrames at once
if len(dfs_to_union) > 1:
    from functools import reduce
    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs_to_union)
else:
    final_df = dfs_to_union[0]

if save_output_to_table == False:
    display(final_df)
else:
    print(f"writing to table {output_table}")
    final_df.write.mode("overwrite").saveAsTable("shortcutstable")