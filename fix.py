"""
contract_enricher.py
────────────────────
Enriches a data contract JSON with AI-generated metadata using
Databricks-hosted LLM endpoints (MLflow Deployments).

Designed to be triggered as a Databricks Job by a frontend application
via the Databricks Jobs REST API (POST /api/2.1/jobs/run-now).

Data flow
─────────
  Frontend App
      │  POST /api/2.1/jobs/run-now
      │  {
      │    "job_id": 123,
      │    "python_params": [
      │      "--payload-json", "<escaped JSON string>",
      │      "--run-id",       "req-abc-123"
      │    ]
      │  }
      ▼
  Databricks Job  ──  contract_enricher.py
      │
      ├─ Step 1  ContractTemplateBuilder    payload → contract skeleton
      ├─ Step 2  ServerDetailsLoader        Spark tables → servers[]
      ├─ Step 3  DQRulesLoader              Spark table  → quality.rules[]
      ├─ Step 4  ContractEnricher           LLM → AI fields
      ├─ Step 5  ContractValidator          field checks + PII report
      └─ Step 6  ContractPersistenceManager write to two Delta tables:
                   dmg_target.stg_data_contracts  ← enriched contract (append)
                   dmg_target.data_contracts      ← versioned, rn=1 → Active

  dmg_target.data_contracts versioning logic (mirrors screenshot):
      window = partitionBy(contract_id).orderBy(version DESC)
      rn = row_number() over window
      status = WHEN rn == 1 THEN 'Active' ELSE 'Inactive'

CLI Usage
─────────
# Triggered by Databricks Job (primary use case)
python contract_enricher.py \\
  --payload-json  '<escaped JSON string from frontend>' \\
  --run-id        'req-abc-123'

# Local dev / testing from a file
python contract_enricher.py \\
  --payload  payload.json

# Dry-run: build skeleton + load Spark data, skip LLM
python contract_enricher.py --payload payload.json --dry-run

# Override staging/final table names if needed
python contract_enricher.py \\
  --payload-json '<json>' \\
  --staging-table my_catalog.dmg_target.stg_data_contracts \\
  --final-table   my_catalog.dmg_target.data_contracts

# List available LLM endpoints
python contract_enricher.py --payload payload.json --list-endpoints
"""

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import date, datetime
from typing import Any

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 1. CONTRACT TEMPLATE BUILDER
#    Builds the contract skeleton from the frontend payload only.
#    servers[] and quality.rules[] are intentionally left empty here —
#    they are populated by ServerDetailsLoader and DQRulesLoader respectively.
# ══════════════════════════════════════════════════════════════════════════════

class ContractTemplateBuilder:
    """
    Converts the frontend payload into the initial data-contract skeleton.

    From payload
    ─────────────
      contract_id, name, domain
      target_table   →  dataset.name
      summary        →  dataset.description
      schema         →  list of {columnName, dataType}
      availability, data_owner, support_email, slaProperties  →  sla block

    NOT from payload (populated by dedicated loaders)
    ──────────────────────────────────────────────────
      servers[]       ← ServerDetailsLoader  (Spark tables)
      quality.rules[] ← DQRulesLoader        (Spark table)
    """

    def build(self, payload: dict) -> dict:
        contract = {
            "contract_id": payload.get("contract_id", ""),
            "name":        payload.get("name", ""),
            "domain":      payload.get("domain", ""),
            "version":     payload.get("version", "1.0"),
            "dataset": {
                "name":                       payload.get("target_table", ""),
                "logicalType":                "object",
                "physicalType":               "table",
                "description":                payload.get("summary", ""),
                "dataGranularityDescription": "",   # ← filled by LLM
                "schema": {
                    "type":       "object",
                    "properties": [],
                },
            },
            "servers": [],          # ← filled by ServerDetailsLoader
            "quality": {
                "rules": {
                    "completeness": [],   # ← filled by DQRulesLoader
                    "validity":     [],
                    "accuracy":     [],
                    "uniqueness":   [],
                },
            },
            "sla": {
                "availability": payload.get("availability", ""),
                "support": {
                    "source_owner_email": payload.get("data_owner", ""),
                    "tech_support_email": payload.get("support_email", ""),
                },
                "slaProperties": payload.get("slaProperties", {}),
            },
            "changes":                payload.get("changes", "Initial version"),
            "contractCreationDate":   str(date.today()),
            "contractLastUpdatedDate": str(date.today()),
        }

        for col in payload.get("schema", []):
            prop: dict[str, Any] = {
                "physicalName":        col.get("columnName", ""),
                "dataType":            col.get("dataType", ""),
                "attributeName":       "",   # ← filled by LLM
                "criticalDataElement": "Y",
                "description":         "",   # ← filled by LLM
                "isPII":               "",   # ← filled by LLM
            }
            if col.get("dataType", "").lower() == "date":
                prop["format"] = "YYYY-MM-DD"
            contract["dataset"]["schema"]["properties"].append(prop)

        return contract


# ══════════════════════════════════════════════════════════════════════════════
# 2. SERVER DETAILS LOADER
#    Queries three Spark Delta tables to build the servers[] block.
#
#    Tables:
#      dmg_target.metaport_product_list  (mp)
#      dmg_target.entity_master          (em)
#      dmg_target.connection_master      (cm)
#
#    Join:
#      mp JOIN em ON mp.connection_id = em.CONNECTION_MASTER_ID
#                AND mp.bronze_table  = "metaport_bronze." + em.target_entity_name
#         JOIN cm ON em.CONNECTION_MASTER_ID = cm.CONNECTION_MASTER_ID
# ══════════════════════════════════════════════════════════════════════════════

class ServerDetailsLoader:

    TABLE_METAPORT   = "dmg_target.metaport_product_list"
    TABLE_ENTITY     = "dmg_target.entity_master"
    TABLE_CONNECTION = "dmg_target.connection_master"

    def __init__(
        self,
        table_metaport:   str = TABLE_METAPORT,
        table_entity:     str = TABLE_ENTITY,
        table_connection: str = TABLE_CONNECTION,
    ):
        self.table_metaport   = table_metaport
        self.table_entity     = table_entity
        self.table_connection = table_connection

    def load(self, contract: dict) -> None:
        """Query Spark tables and populate contract['servers'] in-place."""
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        spark = SparkSession.builder.getOrCreate()
        logger.info("Loading server details …")

        metaport_df   = spark.table(self.table_metaport)
        entity_df     = spark.table(self.table_entity)
        connection_df = spark.table(self.table_connection)

        # Build full bronze table name
        entity_df = entity_df.withColumn(
            "target_bronze_table",
            F.concat(F.lit("metaport_bronze."), F.col("target_entity_name")),
        ).drop("target_entity_name")

        # Three-way join
        joined_df = (
            metaport_df.alias("mp")
            .join(
                entity_df.alias("em"),
                (F.col("mp.connection_id") == F.col("em.CONNECTION_MASTER_ID"))
                & (F.col("mp.bronze_table") == F.col("em.target_bronze_table")),
            )
            .join(
                connection_df.alias("cm"),
                F.col("em.CONNECTION_MASTER_ID") == F.col("cm.CONNECTION_MASTER_ID"),
            )
            .select(
                "mp.connection_id",
                "mp.bronze_table",
                "em.target_bronze_table",
                "em.SOURCE_ENTITY_NAME",
                "cm.SOURCE_SYSTEM_NAME",
                "cm.SOURCE_CONNECTION_STRING",
                "cm.SOURCE_CREDENTIALS",
                "cm.SOURCE_TYPE",
            )
        )

        # Deduplicate by server identity
        grouped_df = joined_df.groupBy(
            "SOURCE_SYSTEM_NAME",
            "SOURCE_TYPE",
            "SOURCE_CONNECTION_STRING",
            "SOURCE_ENTITY_NAME",
        ).agg(
            F.first("SOURCE_SYSTEM_NAME").alias("server"),
            F.first("SOURCE_TYPE").alias("type"),
            F.first("SOURCE_CONNECTION_STRING").alias("SOURCE_CONNECTION_STRING"),
            F.first("SOURCE_ENTITY_NAME").alias("SOURCE_ENTITY_NAME"),
        )

        servers_list = []
        for row in grouped_df.collect():
            # Fetch credentials for this specific server entry
            cred_rows = (
                joined_df
                .filter(
                    (F.col("SOURCE_SYSTEM_NAME")         == row.server)
                    & (F.col("SOURCE_TYPE")              == row.type)
                    & (F.col("SOURCE_CONNECTION_STRING") == row.SOURCE_CONNECTION_STRING)
                )
                .select("SOURCE_CREDENTIALS")
                .limit(1)
                .collect()
            )

            try:
                raw_creds        = cred_rows[0].SOURCE_CREDENTIALS if cred_rows else "{}"
                credentials_dict = json.loads(raw_creds) if raw_creds else {}
            except (json.JSONDecodeError, TypeError):
                credentials_dict = {}

            config_dict = {
                "SOURCE_CONNECTION_STRING": row.SOURCE_CONNECTION_STRING,
                "file_path": row.SOURCE_ENTITY_NAME,
            }
            config_dict.update(credentials_dict)

            servers_list.append({
                "server": row.server,
                "type":   row.type,
                "config": config_dict,
            })

        contract["servers"] = servers_list
        logger.info("✅ %d server(s) loaded", len(servers_list))


# ══════════════════════════════════════════════════════════════════════════════
# 3. DQ RULES LOADER
#    Queries dmg_target.master_rules and populates all four rule dimensions.
#
#    Expected table columns:
#      dq_dimension  — completeness | validity | accuracy | uniqueness
#      column_name
#      condition
#      criterion
#      weight
# ══════════════════════════════════════════════════════════════════════════════

class DQRulesLoader:

    TABLE_RULES = "dmg_target.master_rules"
    DIMENSIONS  = ("completeness", "validity", "accuracy", "uniqueness")

    def __init__(self, table_rules: str = TABLE_RULES):
        self.table_rules = table_rules

    def load(self, contract: dict) -> None:
        """Query Spark table and populate quality.rules[] in-place."""
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        spark = SparkSession.builder.getOrCreate()
        logger.info("Loading DQ rules from [%s] …", self.table_rules)

        rules_df = spark.table(self.table_rules).withColumn(
            "dq_dimension_lower", F.lower(F.col("dq_dimension"))
        )

        total = 0
        for dimension in self.DIMENSIONS:
            dim_df = rules_df.filter(F.col("dq_dimension_lower") == dimension)
            rules  = [
                {
                    "ruleName":           "",   # ← filled by LLM
                    "ruleDescription":    "",   # ← filled by LLM
                    "column_name":        row["column_name"],
                    "ruleCondition":      row["condition"],
                    "ruleCriterion":      row["criterion"],
                    "ruleErrorThreshold": 5,
                    "weight":             row["weight"],
                }
                for row in dim_df.collect()
            ]
            contract["quality"]["rules"][dimension] = rules
            total += len(rules)
            logger.info("  %-15s → %d rule(s)", dimension, len(rules))

        logger.info("✅ %d DQ rule(s) loaded in total", total)


# ══════════════════════════════════════════════════════════════════════════════
# 4. LLM CLIENT
#    Wraps mlflow.deployments with retry logic and JSON parsing.
# ══════════════════════════════════════════════════════════════════════════════

class LLMClient:

    SYSTEM_PROMPT = (
        "You are a data governance and insurance domain expert. "
        "Always respond with ONLY a valid JSON object. "
        "No explanation, no markdown, no code blocks. Just raw JSON."
    )

    def __init__(
        self,
        endpoint:    str   = "databricks-meta-llama-3-3-70b-instruct",
        max_tokens:  int   = 300,
        temperature: float = 0.1,
        retries:     int   = 3,
        retry_delay: float = 2.0,
    ):
        try:
            import mlflow.deployments
            self._client = mlflow.deployments.get_deploy_client("databricks")
        except ImportError as exc:
            raise RuntimeError("mlflow not installed. Run: pip install mlflow") from exc

        self.endpoint    = endpoint
        self.max_tokens  = max_tokens
        self.temperature = temperature
        self.retries     = retries
        self.retry_delay = retry_delay

    def ask(self, prompt: str) -> str:
        """Call the LLM; return raw text or '' on total failure."""
        for attempt in range(1, self.retries + 1):
            try:
                response = self._client.predict(
                    endpoint=self.endpoint,
                    inputs={
                        "messages": [
                            {"role": "system", "content": self.SYSTEM_PROMPT},
                            {"role": "user",   "content": prompt},
                        ],
                        "max_tokens":  self.max_tokens,
                        "temperature": self.temperature,
                    },
                )
                return response["choices"][0]["message"]["content"].strip()
            except Exception as exc:  # noqa: BLE001
                logger.warning("LLM attempt %d/%d failed: %s", attempt, self.retries, exc)
                if attempt < self.retries:
                    time.sleep(self.retry_delay)
        logger.error("All %d LLM attempts exhausted.", self.retries)
        return ""

    def ask_json(self, prompt: str, context: str = "") -> dict:
        """Call the LLM and parse response as JSON dict."""
        raw = self.ask(prompt)
        if not raw:
            return {}
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            logger.warning("JSON parse failed [%s]:\n%s", context, raw)
            return {}

    def list_endpoints(self) -> list:
        return self._client.list_endpoints()


# ══════════════════════════════════════════════════════════════════════════════
# 5. CONTRACT ENRICHER
#    Uses LLMClient to fill the AI-generated fields.
#    Called AFTER servers[] and quality.rules[] are already populated.
# ══════════════════════════════════════════════════════════════════════════════

class ContractEnricher:

    def __init__(self, llm: LLMClient):
        self.llm = llm

    def enrich(self, contract: dict) -> None:
        """Enrich all AI fields in *contract* in-place."""
        self._enrich_granularity(contract)
        self._enrich_columns(contract)
        self._enrich_all_rules(contract)

    # ── private ───────────────────────────────────────────────────────────

    def _enrich_granularity(self, contract: dict) -> None:
        dataset   = contract["dataset"]
        col_names = [p["physicalName"] for p in dataset["schema"]["properties"]]

        prompt = f"""
Dataset name        : {dataset['name']}
Dataset description : {dataset['description']}
Columns             : {', '.join(col_names)}

Write a dataGranularityDescription (1-2 sentences) explaining what ONE single
row in this dataset represents. Be specific to the insurance context.

Return ONLY:
{{
  "dataGranularityDescription": "<your answer>"
}}
"""
        parsed = self.llm.ask_json(prompt, "dataGranularityDescription")
        value  = parsed.get("dataGranularityDescription", "")
        if value:
            dataset["dataGranularityDescription"] = value
            logger.info("✅ dataGranularityDescription set")
        else:
            logger.warning("⚠️  dataGranularityDescription — no value returned")

    def _enrich_columns(self, contract: dict) -> None:
        dataset_desc = contract["dataset"]["description"]
        props        = contract["dataset"]["schema"]["properties"]
        total        = len(props)
        logger.info("Enriching %d column(s) …", total)

        for idx, prop in enumerate(props, start=1):
            prompt = f"""
You are enriching a data contract for an insurance dataset.
Dataset context : "{dataset_desc}"

Column physical name : {prop['physicalName']}
Data type            : {prop['dataType']}
Format               : {prop.get('format', 'N/A')}

PII includes data that could directly or indirectly identify a real person
(names, addresses, phone numbers, email, IDs, zip codes, dates of birth, etc.).

Return ONLY:
{{
  "attributeName" : "<human-friendly display name>",
  "description"   : "<1-2 sentence business description in insurance context>",
  "isPII"         : "true" or "false"
}}
"""
            parsed = self.llm.ask_json(prompt, prop["physicalName"])
            if parsed:
                prop["attributeName"] = parsed.get("attributeName", "")
                prop["description"]   = parsed.get("description", "")
                prop["isPII"]         = parsed.get("isPII", "false")
                flag = "🔴 PII" if prop["isPII"] == "true" else "🟢 Non-PII"
                logger.info(
                    "  [%d/%d] %-30s %s | %s",
                    idx, total, prop["physicalName"], flag, prop["attributeName"],
                )
            else:
                logger.warning("  [%d/%d] %-30s ❌ skipped", idx, total, prop["physicalName"])

        logger.info("✅ Column enrichment complete")

    def _enrich_all_rules(self, contract: dict) -> None:
        for dim in ("completeness", "validity", "accuracy", "uniqueness"):
            rules = contract["quality"]["rules"].get(dim, [])
            if rules:
                self._enrich_rules(rules, dim)

    def _enrich_rules(self, rule_list: list, rule_type: str) -> None:
        total = len(rule_list)
        logger.info("Enriching %d [%s] rule(s) …", total, rule_type.upper())

        for idx, rule in enumerate(rule_list, start=1):
            prompt = f"""
You are a data quality engineer for an insurance dataset.

Rule type      : {rule_type}
Column         : {rule.get('column_name', '')}
Rule condition : {rule.get('ruleCondition', '')}
Rule criterion : {rule.get('ruleCriterion', '')}

Return ONLY:
{{
  "ruleName"        : "<snake_case name, e.g. chk_policy_number_not_null>",
  "ruleDescription" : "<1 sentence explaining what this rule checks and why>"
}}
"""
            parsed = self.llm.ask_json(prompt, f"{rule_type}_{rule.get('column_name', '')}")
            if parsed:
                rule["ruleName"]        = parsed.get("ruleName", "")
                rule["ruleDescription"] = parsed.get("ruleDescription", "")
                logger.info(
                    "  [%d/%d] %-30s → %s",
                    idx, total, rule.get("column_name", ""), rule["ruleName"],
                )
            else:
                logger.warning(
                    "  [%d/%d] %-30s ❌ skipped", idx, total, rule.get("column_name", "")
                )

        logger.info("✅ [%s] rules enriched", rule_type)


# ══════════════════════════════════════════════════════════════════════════════
# 6. CONTRACT VALIDATOR
#    Post-enrichment field checks + PII summary report.
# ══════════════════════════════════════════════════════════════════════════════

class ContractValidator:

    def validate(self, contract: dict) -> bool:
        """Validate *contract*, print report, return True if no issues."""
        issues: list[str] = []
        self._check_servers(contract, issues)
        self._check_granularity(contract, issues)
        self._check_columns(contract, issues)
        self._check_rules(contract, issues)

        print("\n" + "=" * 60)
        print("VALIDATION REPORT")
        print("=" * 60)

        if not issues:
            print("🎉 All fields enriched successfully!")
        else:
            print(f"⚠️  {len(issues)} issue(s) found:")
            for issue in issues:
                print(f"   {issue}")

        self._print_pii_summary(contract)
        return len(issues) == 0

    def _check_servers(self, contract: dict, issues: list) -> None:
        count = len(contract.get("servers", []))
        if count == 0:
            issues.append("❌ servers[] is empty — ServerDetailsLoader may have failed")
        else:
            print(f"✅ Servers loaded           : {count}")

    def _check_granularity(self, contract: dict, issues: list) -> None:
        if not contract["dataset"].get("dataGranularityDescription"):
            issues.append("❌ dataGranularityDescription is empty")
        else:
            print("✅ dataGranularityDescription : OK")

    def _check_columns(self, contract: dict, issues: list) -> None:
        props = contract["dataset"]["schema"]["properties"]
        ok    = 0
        for prop in props:
            name   = prop["physicalName"]
            col_ok = True
            if not prop.get("attributeName"):
                issues.append(f"❌ Column [{name}] — attributeName empty")
                col_ok = False
            if not prop.get("description"):
                issues.append(f"❌ Column [{name}] — description empty")
                col_ok = False
            if prop.get("isPII") not in ("true", "false"):
                issues.append(
                    f"❌ Column [{name}] — isPII must be 'true'/'false', "
                    f"got '{prop.get('isPII')}'"
                )
                col_ok = False
            if col_ok:
                ok += 1
        print(f"✅ Columns fully enriched   : {ok}/{len(props)}")

    def _check_rules(self, contract: dict, issues: list) -> None:
        for dim in ("completeness", "validity", "accuracy", "uniqueness"):
            rules = contract["quality"]["rules"].get(dim, [])
            ok    = 0
            for rule in rules:
                col = rule.get("column_name", "?")
                if not rule.get("ruleName"):
                    issues.append(f"❌ [{dim}] rule [{col}] — ruleName empty")
                elif not rule.get("ruleDescription"):
                    issues.append(f"❌ [{dim}] rule [{col}] — ruleDescription empty")
                else:
                    ok += 1
            if rules:
                print(f"✅ {dim.capitalize():<15s} rules OK  : {ok}/{len(rules)}")

    def _print_pii_summary(self, contract: dict) -> None:
        props   = contract["dataset"]["schema"]["properties"]
        pii     = [p["physicalName"] for p in props if p.get("isPII") == "true"]
        non_pii = [p["physicalName"] for p in props if p.get("isPII") == "false"]
        print(f"\n🔴 PII columns     ({len(pii)}) : {pii}")
        print(f"🟢 Non-PII columns ({len(non_pii)}) : {non_pii}")


# ══════════════════════════════════════════════════════════════════════════════
# 7. CONTRACT PERSISTENCE MANAGER
#    Writes to two Delta tables:
#
#    dmg_target.stg_data_contracts  (staging)
#      → Append-only. Every enriched contract version lands here.
#        Columns: contract_id, name, version, domain, dataset (JSON string),
#                 quality (JSON string), servers (JSON string), sla (JSON string),
#                 changes, contractCreationDate, contractLastUpdatedDate,
#                 run_id, enriched_at
#
#    dmg_target.data_contracts  (final / managed)
#      → Rebuilt from staging on every run using the versioning logic
#        from the screenshot:
#          window = partitionBy(contract_id).orderBy(version DESC)
#          rn     = row_number() over window
#          status = WHEN rn == 1 THEN 'Active' ELSE 'Inactive'
#        Written with mode("overwrite") + mergeSchema = true.
# ══════════════════════════════════════════════════════════════════════════════

class ContractPersistenceManager:
    """
    Manages the two-table contract lifecycle:

      stg_data_contracts  — staging, append-only, all versions
      data_contracts      — final, versioned with Active/Inactive status
    """

    STG_TABLE   = "dmg_target.stg_data_contracts"
    FINAL_TABLE = "dmg_target.data_contracts"

    # Staging table DDL
    STG_CREATE_DDL = """
        CREATE TABLE IF NOT EXISTS {table} (
            contract_id              STRING  NOT NULL,
            name                     STRING,
            version                  STRING,
            domain                   STRING,
            dataset                  STRING,
            quality                  STRING,
            servers                  STRING,
            sla                      STRING,
            changes                  STRING,
            contractCreationDate     STRING,
            contractLastUpdatedDate  STRING,
            run_id                   STRING,
            enriched_at              STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """

    def __init__(
        self,
        stg_table:   str = STG_TABLE,
        final_table: str = FINAL_TABLE,
    ):
        self.stg_table   = stg_table
        self.final_table = final_table

    def persist(self, contract: dict, run_id: str) -> None:
        """
        1. Append enriched contract to staging table.
        2. Rebuild final table from staging with Active/Inactive versioning.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql import Window

        spark = SparkSession.builder.getOrCreate()

        # ── Step 1: Append to staging ─────────────────────────────────────
        self._append_to_staging(spark, contract, run_id)

        # ── Step 2: Rebuild final table from staging ──────────────────────
        self._rebuild_final(spark, F, Window)

    # ── private ───────────────────────────────────────────────────────────

    def _append_to_staging(self, spark, contract: dict, run_id: str) -> None:
        """Append one record to stg_data_contracts."""
        logger.info("Writing to staging table [%s] …", self.stg_table)

        spark.sql(self.STG_CREATE_DDL.format(table=self.stg_table))

        record = [{
            "contract_id":             contract.get("contract_id", ""),
            "name":                    contract.get("name", ""),
            "version":                 contract.get("version", "1.0"),
            "domain":                  contract.get("domain", ""),
            "dataset":                 json.dumps(contract.get("dataset", {})),
            "quality":                 json.dumps(contract.get("quality", {})),
            "servers":                 json.dumps(contract.get("servers", [])),
            "sla":                     json.dumps(contract.get("sla", {})),
            "changes":                 contract.get("changes", ""),
            "contractCreationDate":    contract.get("contractCreationDate", ""),
            "contractLastUpdatedDate": contract.get("contractLastUpdatedDate", ""),
            "run_id":                  run_id,
            "enriched_at":             datetime.utcnow().isoformat() + "Z",
        }]

        df = spark.createDataFrame(record)
        df.write.mode("append").saveAsTable(self.stg_table)

        logger.info("✅ Appended to staging  run_id=%s", run_id)

    def _rebuild_final(self, spark, F, Window) -> None:
        """
        Rebuild dmg_target.data_contracts from staging.

        Mirrors the logic in the screenshot:
          window_spec = Window.partitionBy("contract_id").orderBy(col("version").desc())
          cte_with_rn = staging_df.withColumn("rn", row_number().over(window_spec))
          managed_df  = cte_with_rn.select(
              ...,
              when(cte_with_rn.rn == 1, 'Active').otherwise('Inactive').alias("status")
          )
          managed_df.write.option("mergeSchema","true").mode("overwrite").saveAsTable(final_table)
        """
        logger.info("Rebuilding final table [%s] from staging …", self.final_table)

        from pyspark.sql.functions import col, row_number, when

        contract_staging_df = spark.table(self.stg_table)

        window_spec = Window.partitionBy("contract_id").orderBy(col("version").desc())

        cte_with_rn = contract_staging_df.withColumn(
            "rn", row_number().over(window_spec)
        )

        managed_contract_df = cte_with_rn.select(
            cte_with_rn.contract_id,
            cte_with_rn.name,
            cte_with_rn.version,
            cte_with_rn.domain,
            cte_with_rn.dataset,
            cte_with_rn.quality,
            cte_with_rn.servers,
            cte_with_rn.sla,
            cte_with_rn.changes,
            cte_with_rn.contractCreationDate,
            cte_with_rn.contractLastUpdatedDate,
            cte_with_rn.run_id,
            cte_with_rn.enriched_at,
            when(cte_with_rn.rn == 1, "Active")
            .otherwise("Inactive")
            .alias("status"),
        )

        managed_contract_df.write \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .saveAsTable(self.final_table)

        logger.info("✅ Final table rebuilt  [%s]", self.final_table)


# ══════════════════════════════════════════════════════════════════════════════
# 8. DATA CONTRACT PIPELINE  (orchestrator)
#
#  Step 1  ContractTemplateBuilder    payload → skeleton
#  Step 2  ServerDetailsLoader        Spark tables → servers[]
#  Step 3  DQRulesLoader              Spark table  → quality.rules[]
#  Step 4  ContractEnricher           LLM → AI fields
#  Step 5  ContractValidator          validates + PII report
#  Step 6  ContractPersistenceManager staging append → final rebuild
# ══════════════════════════════════════════════════════════════════════════════

class DataContractPipeline:

    DEFAULT_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

    def __init__(
        self,
        # LLM
        endpoint:    str   = DEFAULT_ENDPOINT,
        max_tokens:  int   = 300,
        temperature: float = 0.1,
        retries:     int   = 3,
        # Spark source tables
        table_metaport:   str = ServerDetailsLoader.TABLE_METAPORT,
        table_entity:     str = ServerDetailsLoader.TABLE_ENTITY,
        table_connection: str = ServerDetailsLoader.TABLE_CONNECTION,
        table_rules:      str = DQRulesLoader.TABLE_RULES,
        # Spark output tables
        stg_table:   str = ContractPersistenceManager.STG_TABLE,
        final_table: str = ContractPersistenceManager.FINAL_TABLE,
        # Optional: save a local JSON copy too
        output_path: str | None = None,
        # Flags
        dry_run: bool = False,
    ):
        self.dry_run     = dry_run
        self.output_path = output_path

        self.builder     = ContractTemplateBuilder()
        self.srv_loader  = ServerDetailsLoader(table_metaport, table_entity, table_connection)
        self.dq_loader   = DQRulesLoader(table_rules)
        self.validator   = ContractValidator()
        self.persistence = ContractPersistenceManager(stg_table, final_table)

        if not dry_run:
            self.llm      = LLMClient(endpoint, max_tokens, temperature, retries)
            self.enricher = ContractEnricher(self.llm)
        else:
            self.llm      = None
            self.enricher = None

    def run(self, payload: dict, run_id: str | None = None) -> dict:
        """
        Execute the full pipeline.

        Parameters
        ----------
        payload : dict   — frontend request body
        run_id  : str    — correlation ID from frontend (auto-generated if None)

        Returns
        -------
        dict — the fully enriched contract
        """
        run_id   = run_id or str(uuid.uuid4())
        status   = "failed"
        contract = {}

        logger.info("▶ Pipeline start  run_id=%s", run_id)

        try:
            logger.info("Step 1/5 — Building contract skeleton from payload …")
            contract = self.builder.build(payload)

            logger.info("Step 2/5 — Loading server details from Spark tables …")
            self.srv_loader.load(contract)

            logger.info("Step 3/5 — Loading DQ rules from Spark table …")
            self.dq_loader.load(contract)

            if self.dry_run:
                logger.info("Step 4/5 — Dry-run: skipping LLM enrichment")
            else:
                logger.info("Step 4/5 — LLM enrichment …")
                self.enricher.enrich(contract)

            self.validator.validate(contract)

            logger.info("Step 5/5 — Persisting contract to Delta tables …")
            self.persistence.persist(contract, run_id)

            status = "success"

        except Exception as exc:  # noqa: BLE001
            logger.error("Pipeline error: %s", exc, exc_info=True)
            contract["_pipeline_error"] = str(exc)

        finally:
            if self.output_path:
                self._save_file(contract)

        if status == "failed":
            sys.exit(1)   # marks the Databricks job run as Failed

        logger.info("✅ Pipeline complete  run_id=%s", run_id)
        return contract

    def list_endpoints(self) -> None:
        if self.llm is None:
            logger.warning("Cannot list endpoints in dry-run mode.")
            return
        for ep in self.llm.list_endpoints():
            print(f"  → {ep.get('name', ep)}  [{ep.get('task', '')}]")

    def _save_file(self, contract: dict) -> None:
        try:
            with open(self.output_path, "w", encoding="utf-8") as fh:
                json.dump(contract, fh, indent=4)
            logger.info("✅ Contract saved → %s", self.output_path)
        except OSError as exc:
            logger.error("Could not save contract file: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# 9. CLI
# ══════════════════════════════════════════════════════════════════════════════

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="contract_enricher",
        description=(
            "Enrich a data contract JSON using AI. "
            "Triggered as a Databricks Job by a frontend application."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── Input ──────────────────────────────────────────────────────────────
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--payload",
        metavar="FILE",
        help="Path to a JSON payload file (local dev / testing).",
    )
    input_group.add_argument(
        "--payload-json",
        metavar="JSON_STRING",
        help=(
            "Payload as an escaped JSON string — the Databricks Jobs way: "
            "python_params: ['--payload-json', '<json>']"
        ),
    )

    # ── Correlation ID ─────────────────────────────────────────────────────
    parser.add_argument(
        "--run-id", metavar="RUN_ID", default=None,
        help="Frontend correlation ID. Auto-generated if not provided.",
    )

    # ── LLM ───────────────────────────────────────────────────────────────
    parser.add_argument(
        "--endpoint", default=DataContractPipeline.DEFAULT_ENDPOINT,
        help="Databricks model serving endpoint name.",
    )
    parser.add_argument("--max-tokens",  type=int,   default=300,  help="Max tokens per LLM call.")
    parser.add_argument("--temperature", type=float, default=0.1,  help="LLM sampling temperature.")
    parser.add_argument("--retries",     type=int,   default=3,    help="LLM retry count.")

    # ── Spark source table overrides ──────────────────────────────────────
    parser.add_argument(
        "--table-metaport", default=ServerDetailsLoader.TABLE_METAPORT,
        help="Fully-qualified metaport product list table.",
    )
    parser.add_argument(
        "--table-entity", default=ServerDetailsLoader.TABLE_ENTITY,
        help="Fully-qualified entity master table.",
    )
    parser.add_argument(
        "--table-connection", default=ServerDetailsLoader.TABLE_CONNECTION,
        help="Fully-qualified connection master table.",
    )
    parser.add_argument(
        "--table-rules", default=DQRulesLoader.TABLE_RULES,
        help="Fully-qualified master rules table.",
    )

    # ── Spark output table overrides ──────────────────────────────────────
    parser.add_argument(
        "--staging-table", default=ContractPersistenceManager.STG_TABLE,
        metavar="CATALOG.SCHEMA.TABLE",
        help="Staging Delta table (append-only, all versions).",
    )
    parser.add_argument(
        "--final-table", default=ContractPersistenceManager.FINAL_TABLE,
        metavar="CATALOG.SCHEMA.TABLE",
        help="Final Delta table (versioned, Active/Inactive status).",
    )

    # ── Optional local file output ────────────────────────────────────────
    parser.add_argument(
        "--output", default=None,
        help="Optional DBFS/Volume path to also save a JSON copy.",
    )

    # ── Flags ──────────────────────────────────────────────────────────────
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build skeleton + load Spark data; skip LLM and table writes.",
    )
    parser.add_argument(
        "--list-endpoints", action="store_true",
        help="Print available Databricks LLM endpoints and exit.",
    )
    parser.add_argument(
        "--print-contract", action="store_true",
        help="Print the final enriched contract JSON to stdout.",
    )

    return parser


def load_payload(args: argparse.Namespace) -> dict:
    if args.payload:
        with open(args.payload, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(args.payload_json)


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args   = parser.parse_args(argv)

    pipeline = DataContractPipeline(
        endpoint         = args.endpoint,
        max_tokens       = args.max_tokens,
        temperature      = args.temperature,
        retries          = args.retries,
        table_metaport   = args.table_metaport,
        table_entity     = args.table_entity,
        table_connection = args.table_connection,
        table_rules      = args.table_rules,
        stg_table        = args.staging_table,
        final_table      = args.final_table,
        output_path      = args.output,
        dry_run          = args.dry_run,
    )

    if args.list_endpoints:
        pipeline.list_endpoints()
        return

    payload  = load_payload(args)
    contract = pipeline.run(payload, run_id=args.run_id)

    if args.print_contract:
        print(json.dumps(contract, indent=4))


if __name__ == "__main__":
    main()
