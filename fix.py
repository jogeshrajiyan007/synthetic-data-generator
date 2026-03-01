"""
contract_enricher.py
────────────────────
Enriches a data contract JSON with AI-generated metadata using
Databricks-hosted LLM endpoints (MLflow Deployments).

Designed to be triggered as a Databricks Job by a frontend application
via the Databricks Jobs REST API (POST /api/2.1/jobs/run-now).

Flow
----
  Frontend App
      │  POST /api/2.1/jobs/run-now
      │  {
      │    "job_id": 123,
      │    "python_params": [
      │      "--payload-json", "<escaped JSON string>",
      │      "--output-table", "catalog.schema.contracts",
      │      "--run-id",       "req-abc-123"
      │    ]
      │  }
      ▼
  Databricks Job (this script)
      │  Enriches contract with LLM
      │  Writes result to Delta table
      │  (optionally POSTs result back to callback URL)
      ▼
  Frontend polls job status via GET /api/2.1/jobs/runs/get?run_id=...

CLI Usage
---------
# Triggered by Databricks Job (frontend path)
python contract_enricher.py \\
  --payload-json  '<json string from frontend>' \\
  --output-table  'catalog.schema.enriched_contracts' \\
  --run-id        'req-abc-123'

# Local dev / testing from a file
python contract_enricher.py \\
  --payload       payload.json \\
  --output        /dbfs/tmp/enriched_contract.json

# Dry-run: build skeleton only, skip LLM calls
python contract_enricher.py --payload payload.json --dry-run

# List available endpoints
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

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# 1. CONTRACT TEMPLATE BUILDER
# ════════════════════════════════════════════════════════════════════════════

class ContractTemplateBuilder:
    """
    Converts a flat payload dict (sourced from Databricks tables or API)
    into the canonical data-contract JSON structure.
    """

    def build(self, payload: dict) -> dict:
        """Return a fully-structured contract dict from *payload*."""
        contract = {
            "contract_id":   payload.get("contract_id", ""),
            "name":          payload.get("name", ""),
            "domain":        payload.get("domain", ""),
            "version":       "1.0",
            "dataset": {
                "name":                     payload.get("target_table", ""),
                "logicalType":              "object",
                "physicalType":             "table",
                "description":              payload.get("summary", ""),
                "dataGranularityDescription": "",
                "schema": {
                    "type":       "object",
                    "properties": [],
                },
            },
            "servers": [],
            "quality": {
                "rules": {
                    "completeness": [],
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
            "changes":               "Initial version",
            "contractCreationDate":  str(date.today()),
            "contractLastUpdatedDate": str(date.today()),
        }

        # ── Schema properties ────────────────────────────────────────────
        for col in payload.get("schema", []):
            prop: dict[str, Any] = {
                "physicalName":       col.get("columnName", ""),
                "dataType":           col.get("dataType", ""),
                "attributeName":      "",
                "criticalDataElement": "Y",
                "description":        "",
                "isPII":              "",
            }
            if col.get("dataType", "").lower() == "date":
                prop["format"] = "YYYY-MM-DD"
            contract["dataset"]["schema"]["properties"].append(prop)

        # ── Servers ──────────────────────────────────────────────────────
        for srv in payload.get("servers", []):
            contract["servers"].append({
                "server": srv.get("server", ""),
                "type":   srv.get("type", ""),
                "config": srv.get("config", {}),
            })

        # ── Quality rules ────────────────────────────────────────────────
        for dimension in ("completeness", "validity", "accuracy", "uniqueness"):
            for rule in payload.get("quality", {}).get(dimension, []):
                contract["quality"]["rules"][dimension].append({
                    "ruleName":           "",
                    "ruleDescription":    "",
                    "column_name":        rule.get("column_name", ""),
                    "ruleCondition":      rule.get("condition", rule.get("ruleCondition", "")),
                    "ruleCriterion":      rule.get("criterion",  rule.get("ruleCriterion", "")),
                    "ruleErrorThreshold": rule.get("ruleErrorThreshold", 5),
                    "weight":             rule.get("weight", 1),
                })

        return contract


# ════════════════════════════════════════════════════════════════════════════
# 2. LLM CLIENT
# ════════════════════════════════════════════════════════════════════════════

class LLMClient:
    """
    Thin wrapper around mlflow.deployments for Databricks-hosted models.
    Handles retry logic and JSON parsing.
    """

    SYSTEM_PROMPT = (
        "You are a data governance and insurance domain expert. "
        "Always respond with ONLY a valid JSON object. "
        "No explanation, no markdown, no code blocks. Just raw JSON."
    )

    def __init__(self, endpoint: str, max_tokens: int = 300,
                 temperature: float = 0.1, retries: int = 3,
                 retry_delay: float = 2.0):
        try:
            import mlflow.deployments
            self._client   = mlflow.deployments.get_deploy_client("databricks")
        except ImportError as exc:
            raise RuntimeError(
                "mlflow is not installed. Run: pip install mlflow"
            ) from exc

        self.endpoint    = endpoint
        self.max_tokens  = max_tokens
        self.temperature = temperature
        self.retries     = retries
        self.retry_delay = retry_delay

    # ── public ───────────────────────────────────────────────────────────

    def ask(self, prompt: str) -> str:
        """Call the LLM and return the raw text response."""
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
                logger.warning("Attempt %d/%d failed: %s", attempt, self.retries, exc)
                if attempt < self.retries:
                    time.sleep(self.retry_delay)
        logger.error("All %d attempts failed.", self.retries)
        return ""

    def ask_json(self, prompt: str, context: str = "") -> dict:
        """Call the LLM and parse the response as JSON."""
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
        """Return all available serving endpoints."""
        return self._client.list_endpoints()


# ════════════════════════════════════════════════════════════════════════════
# 3. CONTRACT ENRICHER
# ════════════════════════════════════════════════════════════════════════════

class ContractEnricher:
    """
    Uses an LLMClient to fill in the empty fields of a data contract:
      - dataset.dataGranularityDescription
      - schema properties: attributeName, description, isPII
      - quality rules: ruleName, ruleDescription
    """

    def __init__(self, llm: LLMClient):
        self.llm = llm

    # ── public entry point ───────────────────────────────────────────────

    def enrich(self, contract: dict) -> dict:
        """Enrich *contract* in-place and return it."""
        self._enrich_granularity(contract)
        self._enrich_columns(contract)
        self._enrich_all_rules(contract)
        return contract

    # ── private helpers ──────────────────────────────────────────────────

    def _enrich_granularity(self, contract: dict) -> None:
        dataset  = contract["dataset"]
        col_names = [p["physicalName"]
                     for p in dataset["schema"]["properties"]]

        prompt = f"""
Dataset name        : {dataset['name']}
Dataset description : {dataset['description']}
Columns             : {', '.join(col_names)}

Write a dataGranularityDescription (1-2 sentences) explaining what ONE
single row in this dataset represents. Be specific to the insurance context.

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

        logger.info("Enriching %d columns …", total)

        for idx, prop in enumerate(props, start=1):
            name  = prop["physicalName"]
            dtype = prop["dataType"]
            fmt   = prop.get("format", "N/A")

            prompt = f"""
You are enriching a data contract for an insurance dataset.
Dataset context : "{dataset_desc}"

Column physical name : {name}
Data type            : {dtype}
Format               : {fmt}

PII means any data that could directly or indirectly identify a real person
(names, addresses, phone numbers, email, ID numbers, zip codes, DOB, etc.).

Return ONLY:
{{
  "attributeName" : "<human-friendly display name>",
  "description"   : "<1-2 sentence business description in insurance context>",
  "isPII"         : "true" or "false"
}}
"""
            parsed = self.llm.ask_json(prompt, name)
            if parsed:
                prop["attributeName"] = parsed.get("attributeName", "")
                prop["description"]   = parsed.get("description", "")
                prop["isPII"]         = parsed.get("isPII", "false")
                flag = "🔴 PII" if prop["isPII"] == "true" else "🟢 Non-PII"
                logger.info(
                    "  [%d/%d] %-30s %s | %s",
                    idx, total, name, flag, prop["attributeName"],
                )
            else:
                logger.warning("  [%d/%d] %-30s ❌ skipped", idx, total, name)

        logger.info("✅ Column enrichment complete")

    def _enrich_all_rules(self, contract: dict) -> None:
        dimensions = ("completeness", "validity", "accuracy", "uniqueness")
        for dim in dimensions:
            rules = contract["quality"]["rules"].get(dim, [])
            if rules:
                self._enrich_rules(rules, dim)

    def _enrich_rules(self, rule_list: list, rule_type: str) -> None:
        total = len(rule_list)
        logger.info("Enriching %d [%s] rules …", total, rule_type.upper())

        for idx, rule in enumerate(rule_list, start=1):
            col       = rule.get("column_name", "")
            condition = rule.get("ruleCondition", "")
            criterion = rule.get("ruleCriterion", "")

            prompt = f"""
You are a data quality engineer for an insurance dataset.

Rule type      : {rule_type}
Column         : {col}
Rule condition : {condition}
Rule criterion : {criterion}

Return ONLY:
{{
  "ruleName"        : "<short snake_case name, e.g. chk_policy_number_not_null>",
  "ruleDescription" : "<1 sentence plain-English explanation of what this rule checks and why it matters>"
}}
"""
            parsed = self.llm.ask_json(prompt, f"{rule_type}_{col}")
            if parsed:
                rule["ruleName"]        = parsed.get("ruleName", "")
                rule["ruleDescription"] = parsed.get("ruleDescription", "")
                logger.info(
                    "  [%d/%d] %-30s → %s",
                    idx, total, col, rule["ruleName"],
                )
            else:
                logger.warning("  [%d/%d] %-30s ❌ skipped", idx, total, col)

        logger.info("✅ [%s] rules enriched", rule_type)


# ════════════════════════════════════════════════════════════════════════════
# 4. VALIDATOR
# ════════════════════════════════════════════════════════════════════════════

class ContractValidator:
    """
    Post-enrichment validation: checks that all expected fields are non-empty
    and prints a summary report.
    """

    def validate(self, contract: dict) -> bool:
        """
        Validate *contract* and print a report.
        Returns True if no issues were found.
        """
        issues: list[str] = []
        self._check_granularity(contract, issues)
        self._check_columns(contract, issues)
        self._check_rules(contract, issues)

        print("\n" + "=" * 60)
        print("VALIDATION REPORT")
        print("=" * 60)

        if not issues:
            print("🎉 All fields enriched successfully!")
            self._print_pii_summary(contract)
            return True

        print(f"⚠️  {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"   {issue}")
        self._print_pii_summary(contract)
        return False

    # ── private ──────────────────────────────────────────────────────────

    def _check_granularity(self, contract: dict, issues: list) -> None:
        if not contract["dataset"].get("dataGranularityDescription"):
            issues.append("❌ dataGranularityDescription is empty")
        else:
            print("✅ dataGranularityDescription : OK")

    def _check_columns(self, contract: dict, issues: list) -> None:
        props = contract["dataset"]["schema"]["properties"]
        ok    = 0
        for prop in props:
            name = prop["physicalName"]
            col_ok = True
            if not prop.get("attributeName"):
                issues.append(f"❌ Column [{name}] — attributeName is empty")
                col_ok = False
            if not prop.get("description"):
                issues.append(f"❌ Column [{name}] — description is empty")
                col_ok = False
            if prop.get("isPII") not in ("true", "false"):
                issues.append(
                    f"❌ Column [{name}] — isPII must be 'true' or 'false', "
                    f"got: '{prop.get('isPII')}'"
                )
                col_ok = False
            if col_ok:
                ok += 1
        print(f"✅ Columns fully enriched : {ok}/{len(props)}")

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
                print(f"✅ {dim.capitalize():15s} rules OK : {ok}/{len(rules)}")

    def _print_pii_summary(self, contract: dict) -> None:
        props    = contract["dataset"]["schema"]["properties"]
        pii      = [p["physicalName"] for p in props if p.get("isPII") == "true"]
        non_pii  = [p["physicalName"] for p in props if p.get("isPII") == "false"]
        print(f"\n🔴 PII columns     ({len(pii)}) : {pii}")
        print(f"🟢 Non-PII columns ({len(non_pii)}) : {non_pii}")


# ════════════════════════════════════════════════════════════════════════════
# 5. JOB RESULT WRITER  (Delta table output for Databricks Job use case)
# ════════════════════════════════════════════════════════════════════════════

class JobResultWriter:
    """
    Writes the enriched contract to a Delta table so the frontend
    can query the result by run_id.

    Table schema (auto-created if it does not exist):
      run_id          STRING    – correlates back to the frontend request
      contract_id     STRING
      status          STRING    – 'success' | 'failed'
      enriched_json   STRING    – full contract as a JSON string
      created_at      STRING    – ISO-8601 UTC timestamp
    """

    CREATE_DDL = """
        CREATE TABLE IF NOT EXISTS {table} (
            run_id        STRING    NOT NULL,
            contract_id   STRING,
            status        STRING,
            enriched_json STRING,
            created_at    STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def write(self, contract: dict, run_id: str, status: str = "success") -> None:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()

            # Ensure table exists
            spark.sql(self.CREATE_DDL.format(table=self.table_name))

            record = [{
                "run_id":        run_id,
                "contract_id":   contract.get("contract_id", ""),
                "status":        status,
                "enriched_json": json.dumps(contract),
                "created_at":    datetime.utcnow().isoformat() + "Z",
            }]

            df = spark.createDataFrame(record)
            df.createOrReplaceTempView("_contract_incoming")

            # Upsert so re-runs overwrite the same run_id
            spark.sql(f"""
                MERGE INTO {self.table_name} AS target
                USING _contract_incoming     AS source
                ON target.run_id = source.run_id
                WHEN MATCHED  THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            logger.info("✅ Result written to [%s]  run_id=%s  status=%s",
                        self.table_name, run_id, status)

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to write to Delta table: %s", exc)
            raise


# ════════════════════════════════════════════════════════════════════════════
# 5b. CALLBACK NOTIFIER  (optional POST-back to frontend)
# ════════════════════════════════════════════════════════════════════════════

class CallbackNotifier:
    """
    Optionally POSTs the enriched contract back to a frontend callback URL.
    Lets the frontend receive results without polling the Databricks Jobs API.
    Pass --callback-url when triggering the job to enable this.
    """

    def __init__(self, callback_url: str | None):
        self.callback_url = callback_url

    def notify(self, contract: dict, run_id: str, status: str = "success") -> None:
        if not self.callback_url:
            return
        try:
            import urllib.request

            body = json.dumps({
                "run_id":   run_id,
                "status":   status,
                "contract": contract,
            }).encode("utf-8")

            req = urllib.request.Request(
                self.callback_url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("✅ Callback → %s  HTTP %s",
                            self.callback_url, resp.status)
        except Exception as exc:  # noqa: BLE001
            # Never crash the job over a failed callback
            logger.warning("Callback failed (non-fatal): %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# 6. ORCHESTRATOR
# ════════════════════════════════════════════════════════════════════════════

class DataContractPipeline:
    """
    Top-level orchestrator. Wires together all components:
      ContractTemplateBuilder → ContractEnricher → ContractValidator
      → JobResultWriter (Delta) → CallbackNotifier (HTTP POST-back)

    Designed to be triggered as a Databricks Job by a frontend application.
    """

    DEFAULT_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

    def __init__(
        self,
        endpoint:      str        = DEFAULT_ENDPOINT,
        output_path:   str | None = "/dbfs/tmp/enriched_contract.json",
        output_table:  str | None = None,
        callback_url:  str | None = None,
        dry_run:       bool       = False,
        max_tokens:    int        = 300,
        temperature:   float      = 0.1,
        retries:       int        = 3,
    ):
        self.output_path  = output_path
        self.dry_run      = dry_run

        self.builder      = ContractTemplateBuilder()
        self.validator    = ContractValidator()
        self.result_writer = JobResultWriter(output_table) if output_table else None
        self.notifier     = CallbackNotifier(callback_url)

        if not dry_run:
            self.llm      = LLMClient(
                endpoint    = endpoint,
                max_tokens  = max_tokens,
                temperature = temperature,
                retries     = retries,
            )
            self.enricher = ContractEnricher(self.llm)
        else:
            self.llm      = None
            self.enricher = None

    # ── public entry point ───────────────────────────────────────────────

    def run(self, payload: dict, run_id: str | None = None) -> dict:
        """
        Full pipeline:
          1. Build contract skeleton from payload
          2. Enrich with LLM  (skipped in dry-run)
          3. Validate
          4. Save to DBFS file  (if --output given)
          5. Write to Delta table  (if --output-table given)
          6. POST result to callback URL  (if --callback-url given)

        Parameters
        ----------
        payload : dict   — raw input from frontend
        run_id  : str    — correlation ID from frontend request (auto-generated if None)

        Returns the enriched contract dict.
        """
        run_id = run_id or str(uuid.uuid4())
        logger.info("▶ Pipeline start  run_id=%s", run_id)

        status   = "failed"
        contract = {}

        try:
            logger.info("Building contract template …")
            contract = self.builder.build(payload)

            if self.dry_run:
                logger.info("Dry-run mode — skipping LLM enrichment")
            else:
                logger.info("Starting LLM enrichment …")
                self.enricher.enrich(contract)

            self.validator.validate(contract)
            status = "success"

        except Exception as exc:  # noqa: BLE001
            logger.error("Pipeline error: %s", exc)
            contract["_error"] = str(exc)

        finally:
            # Always attempt to persist results
            if self.output_path:
                self._save_file(contract)
            if self.result_writer:
                self.result_writer.write(contract, run_id, status)
            self.notifier.notify(contract, run_id, status)

        if status == "failed":
            sys.exit(1)   # Non-zero exit marks the Databricks job as failed

        logger.info("✅ Pipeline complete  run_id=%s", run_id)
        return contract

    def list_endpoints(self) -> None:
        if self.llm is None:
            logger.warning("Cannot list endpoints in dry-run mode.")
            return
        for ep in self.llm.list_endpoints():
            print(f"  → {ep.get('name', ep)}  [{ep.get('task', '')}]")

    # ── private ──────────────────────────────────────────────────────────

    def _save_file(self, contract: dict) -> None:
        try:
            with open(self.output_path, "w", encoding="utf-8") as fh:
                json.dump(contract, fh, indent=4)
            logger.info("✅ Contract saved → %s", self.output_path)
        except OSError as exc:
            logger.error("Could not save contract file: %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# 7. CLI
# ════════════════════════════════════════════════════════════════════════════

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="contract_enricher",
        description=(
            "Enrich a data contract JSON with AI-generated metadata. "
            "Designed to be triggered as a Databricks Job by a frontend application."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── Input: payload (file path OR inline JSON string) ─────────────────
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--payload",
        metavar="FILE",
        help="Path to a JSON file containing the payload (local dev / testing).",
    )
    input_group.add_argument(
        "--payload-json",
        metavar="JSON_STRING",
        help=(
            "Payload as an escaped JSON string. "
            "This is how Databricks Jobs pass parameters: "
            "python_params: ['--payload-json', '<json>']"
        ),
    )

    # ── Correlation: run ID from frontend ─────────────────────────────────
    parser.add_argument(
        "--run-id",
        metavar="RUN_ID",
        default=None,
        help=(
            "Correlation ID from the frontend request (e.g. a UUID). "
            "Auto-generated if not provided. "
            "Stored in the output Delta table for the frontend to query."
        ),
    )

    # ── LLM / model endpoint ──────────────────────────────────────────────
    parser.add_argument(
        "--endpoint",
        default=DataContractPipeline.DEFAULT_ENDPOINT,
        help="Databricks model serving endpoint name.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=300,
        help="Maximum tokens per LLM call.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Sampling temperature (lower = more deterministic).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of LLM retries on transient failure.",
    )

    # ── Output: file path (local/DBFS) ────────────────────────────────────
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "File path for the enriched contract JSON "
            "(e.g. /dbfs/tmp/contract.json or a Unity Catalog Volume path). "
            "Optional when --output-table is given."
        ),
    )

    # ── Output: Delta table (primary output for job use case) ─────────────
    parser.add_argument(
        "--output-table",
        default=None,
        metavar="CATALOG.SCHEMA.TABLE",
        help=(
            "Fully-qualified Delta table to write the enriched contract into. "
            "The table is created automatically if it does not exist. "
            "The frontend can poll this table using the run_id column."
        ),
    )

    # ── Callback: optional POST-back to frontend ──────────────────────────
    parser.add_argument(
        "--callback-url",
        default=None,
        metavar="URL",
        help=(
            "HTTP(S) endpoint to POST the enriched contract to on completion. "
            "Lets the frontend receive results without polling. "
            "A failed POST is logged as a warning but does not fail the job."
        ),
    )

    # ── Utility flags ─────────────────────────────────────────────────────
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the contract skeleton without calling the LLM (for testing).",
    )
    parser.add_argument(
        "--list-endpoints",
        action="store_true",
        help="Print available Databricks serving endpoints and exit.",
    )
    parser.add_argument(
        "--print-contract",
        action="store_true",
        help="Print the final enriched contract JSON to stdout.",
    )

    return parser


def load_payload(args: argparse.Namespace) -> dict:
    """Load and return the payload dict from CLI args."""
    if args.payload:
        with open(args.payload, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(args.payload_json)


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args   = parser.parse_args(argv)

    # Validate: at least one output must be specified
    if not args.dry_run and not args.output and not args.output_table:
        parser.error(
            "Specify at least one output: --output <file> or --output-table <table>"
        )

    pipeline = DataContractPipeline(
        endpoint     = args.endpoint,
        output_path  = args.output,
        output_table = args.output_table,
        callback_url = args.callback_url,
        dry_run      = args.dry_run,
        max_tokens   = args.max_tokens,
        temperature  = args.temperature,
        retries      = args.retries,
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
