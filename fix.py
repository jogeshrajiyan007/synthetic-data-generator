#######################################
GET SERVER DETAILS
#######################################

from pyspark.sql import functions as F
import json


# Load tables
metaport_df = spark.table("dmg_target.metaport_product_list")
entity_df = spark.table("dmg_target.entity_master")
connection_df = spark.table("dmg_target.connection_master")

# Add a new column with the full bronze table name
entity_df = entity_df.withColumn(
    "target_bronze_table",
    F.concat(F.lit("metaport_bronze."), F.col("target_entity_name"))
).drop(F.col("target_entity_name"))

# Join metaport_product_list with entity_master on connection_id
joined_df = metaport_df.alias("mp") \
    .join(entity_df.alias("em"), (F.col("mp.connection_id") == F.col("em.CONNECTION_MASTER_ID")) & (F.col("mp.bronze_table") == F.col("em.target_bronze_table"))) \
    .join(connection_df.alias("cm"), F.col("em.CONNECTION_MASTER_ID") == F.col("cm.CONNECTION_MASTER_ID")) \
    .select(
        "mp.connection_id",
        "mp.bronze_table",
        "em.target_bronze_table",
        "em.SOURCE_ENTITY_NAME",
        "cm.SOURCE_SYSTEM_NAME",
        "cm.SOURCE_CONNECTION_STRING",
        "cm.SOURCE_CREDENTIALS",
        "cm.SOURCE_TYPE"
    )

# Group by server attributes to eliminate duplicates
grouped_df = joined_df.groupBy(
    "SOURCE_SYSTEM_NAME",
    "SOURCE_TYPE",
    "SOURCE_CONNECTION_STRING",
    "SOURCE_ENTITY_NAME"
).agg(
    F.first("SOURCE_SYSTEM_NAME").alias("server"),
    F.first("SOURCE_TYPE").alias("type"),
    F.first("SOURCE_CONNECTION_STRING").alias("SOURCE_CONNECTION_STRING"),
    F.first("SOURCE_ENTITY_NAME").alias("SOURCE_ENTITY_NAME")
)

# Collect and build server list
rows = grouped_df.collect()
servers_list = []

for row in rows:
    # Parse credentials JSON for each unique connection
    # To be accurate, you might want to get credentials from the original joined_df if they differ
    # Here, assuming credentials are same per connection
    credentials_json = joined_df.filter(
        (F.col("SOURCE_SYSTEM_NAME") == row.server) &
        (F.col("SOURCE_TYPE") == row.type) &
        (F.col("SOURCE_CONNECTION_STRING") == row.SOURCE_CONNECTION_STRING)
    ).select("SOURCE_CREDENTIALS").limit(1).collect()[0].SOURCE_CREDENTIALS

    try:
        credentials_dict = json.loads(credentials_json)
    except Exception:
        credentials_dict = {}

    config_dict = {
        "SOURCE_CONNECTION_STRING": row.SOURCE_CONNECTION_STRING,
        "file_path": row.SOURCE_ENTITY_NAME
    }
    config_dict.update(credentials_dict)

    server_entry = {
        "server": row.server,
        "type": row.type,
        "config": config_dict
    }
    servers_list.append(server_entry)

template["servers"] = servers_list

# Now, the template is populated with the server details, including the mapped "type" and config.
# You can print or save this template as needed.
print(json.dumps(template, indent=4))

#####################################################
GET DQ RULES 
#####################################################
from pyspark.sql import functions as F

# Read the rules table
rules_df = spark.table("dmg_target.master_rules")

# Add lowercase dq_dimension for filtering
rules_df = rules_df.withColumn("dq_dimension_lower", F.lower(F.col("dq_dimension")))

# Filter rules for each dq_dimension
completeness_df = rules_df.filter(F.col("dq_dimension_lower") == "completeness")
validity_df = rules_df.filter(F.col("dq_dimension_lower") == "validity")
accuracy_df = rules_df.filter(F.col("dq_dimension_lower") == "accuracy")
uniqueness_df = rules_df.filter(F.col("dq_dimension_lower") == "uniqueness")

# Function to convert dataframe to list of rule dicts
def rules_to_list(df):
    return [
        {
            "ruleName": "",  # leave blank
            "ruleDescription": "",
            "column_name": row["column_name"],
            "ruleCondition": row["condition"],
            "ruleCriterion": row["criterion"],
            "ruleErrorThreshold": 5,
            "weight": row["weight"]
        }
        for row in df.collect()
    ]

# Populate the rules in the template
template["quality"]["rules"]["completeness"] = rules_to_list(completeness_df)
template["quality"]["rules"]["validity"] = rules_to_list(validity_df)
template["quality"]["rules"]["accuracy"] = rules_to_list(accuracy_df)
template["quality"]["rules"]["uniqueness"] = rules_to_list(uniqueness_df)

print(json.dumps(template, indent=4))
