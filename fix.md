# ============================================================
# Cell 6 (Updated): Enrich attributeName, description & isPII
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

PII (Personally Identifiable Information) includes any data that could 
directly or indirectly identify a real person — such as names, addresses, 
phone numbers, email, ID numbers, zip codes, dates of birth, etc.

Return ONLY this JSON (no extra text):
{{
  "attributeName": "<human-friendly display name, e.g. 'Policy Holder Name'>",
  "description"  : "<1-2 sentence business description explaining what this field means in motor insurance context>",
  "isPII"        : "<Y or N>"
}}
"""

    raw    = ask_llm(prompt)
    parsed = parse_json_response(raw, physical_name)

    if parsed:
        prop["attributeName"] = parsed.get("attributeName", "")
        prop["description"]   = parsed.get("description", "")
        prop["isPII"]         = parsed.get("isPII", "N")
        
        pii_flag = "🔴 PII" if prop["isPII"] == "Y" else "🟢 Non-PII"
        print(f"  ✅ [{i+1}/{len(schema_props)}] {physical_name} {pii_flag}")
        print(f"       attributeName : {prop['attributeName']}")
        print(f"       description   : {prop['description']}")
        print(f"       isPII         : {prop['isPII']}\n")
    else:
        print(f"  ❌ [{i+1}/{len(schema_props)}] {physical_name} — skipped\n")

print("✅ Column enrichment complete")
Also update the Validation cell (Cell 8) to check the new field:
# Add this inside Cell 8 after the existing column checks

for prop in contract["dataset"]["schema"]["properties"]:
    name = prop["physicalName"]
    if prop.get("isPII") not in ["Y", "N"]:
        issues.append(f"❌ Column [{name}] — isPII is missing or invalid (must be Y or N)")

# Summary of PII columns
pii_cols = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"] if p.get("isPII") == "Y"]
non_pii_cols = [p["physicalName"] for p in contract["dataset"]["schema"]["properties"] if p.get("isPII") == "N"]

print(f"\n🔴 PII columns     ({len(pii_cols)})  : {pii_cols}")
print(f"🟢 Non-PII columns ({len(non_pii_cols)}) : {non_pii_cols}")