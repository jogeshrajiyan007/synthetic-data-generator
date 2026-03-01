tempalate = {
        "contract_id": "",
        "name": "",
        "domain": "",
        "version": "1.0",
        "dataset": {
            "name": "",
            "logicalType": "object",
            "physicalType": "table",
            "description": "",
            "dataGranularityDescription": "",
            "schema": {
            "type": "object",
            "properties": [
                {
                "physicalName": "",
                "dataType": "",
                "attributeName": "",
                "criticalDataElement": "",
                "description": ""
                }
            ]
            }
        },
        "servers": [
            {
            "server": "",
            "type": "",
            "config": {}
            }
        ],
        "quality": {
            "rules": {
            "completeness": [
                {
                "ruleName": "",
                "ruleDescription": "",
                "ruleCondition": "",
                "ruleCriterion":"",
                "ruleErrorThreshold": ""
                }
            ],
            "validity": [
                {
                "ruleName": "",
                "ruleDescription": "",
                "ruleCondition": "",
                "ruleCriterion":"",
                "ruleErrorThreshold": ""
                }
            ],
            "accuracy": [
                {
                "ruleName": "",
                "ruleDescription": "",
                "ruleCondition": "",
                "ruleCriterion":"",
                "ruleErrorThreshold": ""
                }
            ]
            }
        },
        "sla": {
            "availability": "",
            "support": {
            "source_owner_email": "",
            "tech_support_email": ""
            },
            "slaProperties": {
            "timeOfAvailability": "",
            "latency": "",
            "frequency": "",
            "retention": ""
            }
        },
        "changes": "Initial version",
        "contractCreationDate": "",
        "contractLastUpdatedDate": ""
        }

        from datetime import date

def fill_template(payload):
    template = {
        "contract_id": payload.get("contract_id", ""),
        "name": payload.get("name", ""),
        "domain": payload.get("domain", ""),
        "version": "1.0",
        "dataset": {
            "name": payload.get("target_table", ""),
            "logicalType": "object",
            "physicalType": "table",
            "description": payload.get("summary", ""),
            "dataGranularityDescription": "",  # Optional, can be added
            "schema": {
                "type": "object",
                "properties": []
            }
        },
        "servers": [],  # Fill if server info is available
        "quality": {
            "rules": {
                "completeness": [],
                "validity": [],
                "accuracy": []
            }
        },
        "sla": {
            "availability": payload.get("availability", ""),
            "support": {
                "source_owner_email": payload.get("data_owner", ""),
                "tech_support_email": payload.get("support_email", "")
            },
            "slaProperties": payload.get("slaProperties", {})
        },
        "changes": "Initial version",
        "contractCreationDate": str(date.today()),
        "contractLastUpdatedDate": str(date.today())
    }

    # Map schema
    for column in payload.get("schema", []):
        property_item = {
            "physicalName": column.get("columnName", ""),
            "dataType": column.get("dataType", ""),
            "attributeName": "",
            "criticalDataElement": "Y",
            "description": ""  # Optional
        }
        if column.get("dataType", "").lower() == "date":
            property_item["format"] = "YYYY-MM-DD"
        # Append to schema properties
        template["dataset"]["schema"]["properties"].append(property_item)
    
    return template


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

response["servers"] = servers_list

# Now, the template is populated with the server details, including the mapped "type" and config.
# You can print or save this template as needed.
print(json.dumps(response, indent=4))


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
response["quality"]["rules"]["completeness"] = rules_to_list(completeness_df)
response["quality"]["rules"]["validity"] = rules_to_list(validity_df)
response["quality"]["rules"]["accuracy"] = rules_to_list(accuracy_df)
response["quality"]["rules"]["uniqueness"] = rules_to_list(uniqueness_df)

print(json.dumps(response, indent=4))

import json
import mlflow.deployments
import time
contract = response

print("Contract Loaded successfully!")

client = mlflow.deployments.get_deploy_client("databricks")
print("Available endpoints:")
for endpoint in client.list_endpoints():
    print(endpoint)

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

def ask_llm(prompt: str, retries: int = 3) -> str: 
    """ Call the LLM endpoint with retry logic. Returns raw text response from the model. """    
    for attempt in range(retries):
        try:
            response = client.predict(
                endpoint=MODEL_ENDPOINT,
                inputs={
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a data governance and insurance domain expert. "
                                "Always respond with ONLY a valid JSON object. "
                                "No explanation, no markdown, no code blocks. Just raw JSON."
                            )
                        },
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 300,
                    "temperature": 0.1
                }
            )
            return response["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed for prompt: {e}")
            time.sleep(2)
    return ""

def parse_json_response(raw: str, field_name: str) -> dict: 
    """ Safely parse JSON from LLM response. Strips markdown code fences if present. """ 
    try: # Strip markdown code fences if model adds them 
        clean = raw.replace("json", "").replace("", "").strip() 
        return json.loads(clean) 
    except json.JSONDecodeError: 
        print(f"⚠️ JSON parse failed for [{field_name}]. Raw response:\n{raw}\n") 
        return {}
    
print("✅ LLM helper functions ready")


column_names = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"]]

prompt = f""" Dataset name : {contract['dataset']['name']} Dataset description: {contract['dataset']['description']} Columns : {', '.join(column_names)}

Write a dataGranularityDescription explaining what ONE single row in this dataset represents. Keep it to 1-2 sentences. Be specific to insurance context.

Return ONLY this JSON: {{ "dataGranularityDescription": "" }} """

raw = ask_llm(prompt) 
parsed = parse_json_response(raw, "dataGranularityDescription")

if parsed.get("dataGranularityDescription"): 
  contract["dataset"]["dataGranularityDescription"] = parsed["dataGranularityDescription"] 
  print(f"✅ dataGranularityDescription:\n {contract['dataset']['dataGranularityDescription']}")
  
else: 
  print("⚠️ Could not enrich dataGranularityDescription")

dataset_desc = contract["dataset"]["description"] 
schema_props = contract["dataset"]["schema"]["properties"]

print(f"Enriching {len(schema_props)} columns...\n")

for i, prop in enumerate(schema_props): 
    physical_name = prop["physicalName"] 
    data_type = prop["dataType"] 
    fmt = prop.get("format", "N/A")

    prompt = f"""
    You are enriching a data contract for a motor insurance dataset. Dataset context: "{dataset_desc}"

    Column physical name : {physical_name} Data type : {data_type} Format : {fmt}

    PII (Personally Identifiable Information) includes any data that could directly or indirectly identify a real person — such as names, addresses, phone numbers, email, ID numbers, zip codes, dates of birth, etc.

    Return ONLY this JSON (no extra text): {{ "attributeName": "<human-friendly display name, e.g. 'Policy Holder Name'>", "description" : "<1-2 sentence business description explaining what this field means in motor insurance context>", "isPII" : "" }} """

    raw    = ask_llm(prompt)
    parsed = parse_json_response(raw, physical_name)

    if parsed:
        prop["attributeName"] = parsed.get("attributeName", "")
        prop["description"]   = parsed.get("description", "")
        prop["isPII"]         = parsed.get("isPII", "false")
        
        pii_flag = "🔴 PII" if prop["isPII"] == "true" else "🟢 Non-PII"

        print(f"  ✅ [{i+1}/{len(schema_props)}] {physical_name} {pii_flag}")
        print(f"       attributeName : {prop['attributeName']}")
        print(f"       description   : {prop['description']}")
        print(f"       isPII         : {prop['isPII']}\n")
    else:
        print(f"  ❌ [{i+1}/{len(schema_props)}] {physical_name} — skipped\n")
        
print("✅ Column enrichment complete") 

def enrich_rules(rule_list: list, rule_type: str): 
    print(f"\n── Enriching [{rule_type.upper()}] rules ({len(rule_list)} rules) ──")

    for i, rule in enumerate(rule_list):
        col       = rule["column_name"]
        condition = rule["ruleCondition"]
        criterion = rule["ruleCriterion"]

        prompt = f"""
            You are a data quality engineer for a motor insurance dataset.

            Rule type : {rule_type} Column : {col} Rule condition : {condition} Rule criterion : {criterion}

            Return ONLY this JSON (no extra text): {{ "ruleName" : "<short snake_case name, e.g. chk_policy_number_not_null>", "ruleDescription" : "<1 sentence plain-English explanation of what this rule checks and why it matters>" }} """

        raw    = ask_llm(prompt)
        parsed = parse_json_response(raw, f"{rule_type}_{col}")

        if parsed:
            rule["ruleName"]        = parsed.get("ruleName", "")
            rule["ruleDescription"] = parsed.get("ruleDescription", "")
            print(f"  ✅ [{i+1}] {col}")
            print(f"       ruleName        : {rule['ruleName']}")
            print(f"       ruleDescription : {rule['ruleDescription']}\n")
        else:
            print(f"  ❌ [{i+1}] {col} — skipped\n")

enrich_rules(contract["quality"]["rules"]["completeness"], "completeness") 
enrich_rules(contract["quality"]["rules"]["validity"], "validity") 
enrich_rules(contract["quality"]["rules"]["uniqueness"], "uniqueness")

print("\n✅ All quality rules enriched")

print("=" * 60) 
print("VALIDATION REPORT") 
print("=" * 60)

issues = []

if not contract["dataset"]["dataGranularityDescription"]: 
    issues.append("❌ dataGranularityDescription is still empty") 
else: 
    print("✅ dataGranularityDescription : OK")

for prop in contract["dataset"]["schema"]["properties"]: 
    name = prop["physicalName"] 
    if not prop.get("attributeName"): 
        issues.append(f"❌ Column [{name}] — attributeName is empty") 
    if not prop.get("description"): 
        issues.append(f"❌ Column [{name}] — description is empty")
    if prop.get("isPII") not in ["true", "false"]: 
        issues.append(f"❌ Column [{name}] — isPII is missing or invalid (must be true or false)")

cols_ok = len([p for p in contract["dataset"]["schema"]["properties"] if p.get("attributeName") and p.get("description")]) 
print(f"✅ Columns enriched : {cols_ok}/{len(contract['dataset']['schema']['properties'])}")

for rule_type in ["completeness", "validity", "uniqueness"]: 
    rules = contract["quality"]["rules"][rule_type] 
    rules_ok = 0 
    for rule in rules: 
        if not rule.get("ruleName"): 
            issues.append(f"❌ [{rule_type}] rule for [{rule['column_name']}] — ruleName is empty") 
        elif not rule.get("ruleDescription"): 
            issues.append(f"❌ [{rule_type}] rule for [{rule['column_name']}] — ruleDescription is empty") 
        else: 
            rules_ok += 1 
            print(f"✅ {rule_type.capitalize()} rules OK : {rules_ok}/{len(rules)}")

if issues: 
    print("\n⚠️ Issues found:") 
    for issue in issues: 
        print(f" {issue}")
else: 
    print("\n🎉 All fields enriched successfully!")

pii_cols = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"] if p.get("isPII") == "true"] 
non_pii_cols = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"] if p.get("isPII") == "false"]

print(f"\n🔴 PII columns ({len(pii_cols)}) : {pii_cols}") 
print(f"🟢 Non-PII columns ({len(non_pii_cols)}) : {non_pii_cols}")

print(json.dumps(contract, indent=4))
