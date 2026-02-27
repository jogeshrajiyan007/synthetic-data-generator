
def ask_llm(prompt: str, retries: int = 3) -> str:
    """
    Call the LLM endpoint with retry logic.
    Returns raw text response from the model.
    """
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
            print(f"⚠️  Attempt {attempt+1} failed for prompt: {e}")
            time.sleep(2)
    return ""


def parse_json_response(raw: str, field_name: str) -> dict:
    """
    Safely parse JSON from LLM response.
    Strips markdown code fences if present.
    """
    try:
        # Strip markdown code fences if model adds them
        clean = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(clean)
    except json.JSONDecodeError:
        print(f"⚠️  JSON parse failed for [{field_name}]. Raw response:\n{raw}\n")
        return {}


print("✅ LLM helper functions ready")
# ============================================================
# Cell 5: Enrich dataGranularityDescription
# ============================================================

column_names = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"]]

prompt = f"""
Dataset name       : {contract['dataset']['name']}
Dataset description: {contract['dataset']['description']}
Columns            : {', '.join(column_names)}

Write a dataGranularityDescription explaining what ONE single row in this dataset represents.
Keep it to 1-2 sentences. Be specific to motor insurance context.

Return ONLY this JSON:
{{
  "dataGranularityDescription": "<your answer here>"
}}
"""

raw = ask_llm(prompt)
parsed = parse_json_response(raw, "dataGranularityDescription")

if parsed.get("dataGranularityDescription"):
    contract["dataset"]["dataGranularityDescription"] = parsed["dataGranularityDescription"]
    print(f"✅ dataGranularityDescription:\n   {contract['dataset']['dataGranularityDescription']}")
else:
    print("⚠️  Could not enrich dataGranularityDescription")
# ============================================================
# Cell 6: Enrich attributeName & description for Each Column
# ============================================================

dataset_desc = contract["dataset"]["description"]
schema_props = contract["dataset"]["schema"]["properties"]

print(f"Enriching {len(schema_props)} columns...\n")

for i, prop in enumerate(schema_props):
    physical_name = prop["physicalName"]
    data_type     = prop["dataType"]
    fmt           = prop.get("format", "N/A")

    prompt = f"""
You are enriching a data contract for a motor insurance dataset.
Dataset context: "{dataset_desc}"

Column physical name : {physical_name}
Data type            : {data_type}
Format               : {fmt}

Return ONLY this JSON (no extra text):
{{
  "attributeName": "<human-friendly display name, e.g. 'Policy Holder Name'>",
  "description"  : "<1-2 sentence business description explaining what this field means in motor insurance context>"
}}
"""

    raw    = ask_llm(prompt)
    parsed = parse_json_response(raw, physical_name)

    if parsed:
        prop["attributeName"] = parsed.get("attributeName", "")
        prop["description"]   = parsed.get("description", "")
        print(f"  ✅ [{i+1}/{len(schema_props)}] {physical_name}")
        print(f"       attributeName : {prop['attributeName']}")
        print(f"       description   : {prop['description']}\n")
    else:
        print(f"  ❌ [{i+1}/{len(schema_props)}] {physical_name} — skipped\n")

print("✅ Column enrichment complete")
# ============================================================
# Cell 7: Enrich ruleName & ruleDescription for Quality Rules
# ============================================================

def enrich_rules(rule_list: list, rule_type: str):
    print(f"\n── Enriching [{rule_type.upper()}] rules ({len(rule_list)} rules) ──")
    
    for i, rule in enumerate(rule_list):
        col       = rule["column_name"]
        condition = rule["ruleCondition"]
        criterion = rule["ruleCriterion"]

        prompt = f"""
You are a data quality engineer for a motor insurance dataset.

Rule type      : {rule_type}
Column         : {col}
Rule condition : {condition}
Rule criterion : {criterion}

Return ONLY this JSON (no extra text):
{{
  "ruleName"        : "<short snake_case name, e.g. chk_policy_number_not_null>",
  "ruleDescription" : "<1 sentence plain-English explanation of what this rule checks and why it matters>"
}}
"""

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
enrich_rules(contract["quality"]["rules"]["validity"],     "validity")
enrich_rules(contract["quality"]["rules"]["uniqueness"],   "uniqueness")

print("\n✅ All quality rules enriched")
# ============================================================
# Cell 8: Validate Enrichment — Check for Any Empty Fields
# ============================================================

print("=" * 60)
print("VALIDATION REPORT")
print("=" * 60)

issues = []

# Check dataGranularityDescription
if not contract["dataset"]["dataGranularityDescription"]:
    issues.append("❌ dataGranularityDescription is still empty")
else:
    print("✅ dataGranularityDescription : OK")

# Check columns
for prop in contract["dataset"]["schema"]["properties"]:
    name = prop["physicalName"]
    if not prop.get("attributeName"):
        issues.append(f"❌ Column [{name}] — attributeName is empty")
    if not prop.get("description"):
        issues.append(f"❌ Column [{name}] — description is empty")

cols_ok = len([p for p in contract["dataset"]["schema"]["properties"] 
               if p.get("attributeName") and p.get("description")])
print(f"✅ Columns enriched          : {cols_ok}/{len(contract['dataset']['schema']['properties'])}")

# Check rules
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
    print(f"✅ {rule_type.capitalize()} rules OK  : {rules_ok}/{len(rules)}")

if issues:
    print("\n⚠️  Issues found:")
    for issue in issues:
        print(f"   {issue}")
else:
    print("\n🎉 All fields enriched successfully!")
# ============================================================
# Cell 9: Save Enriched Contract to DBFS / Volume
# ============================================================

import json

output_path = "/dbfs/tmp/guidewire_motor_enriched_contract.json"

with open(output_path, "w") as f:
    json.dump(contract, f, indent=4)

print(f"✅ Enriched contract saved to: {output_path}")

# ── Preview the enriched contract ──
print("\n── Preview (first 2 columns) ──")
for prop in contract["dataset"]["schema"]["properties"][:2]:
    print(json.dumps(prop, indent=4))

print("\n── Preview (first completeness rule) ──")
print(json.dumps(contract["quality"]["rules"]["completeness"][0], indent=4))
# ============================================================
# Cell 10 (Optional): Display Full Enriched Contract
# ============================================================

print(json.dumps(contract, indent=4))