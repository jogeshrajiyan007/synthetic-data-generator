# FCR LLM Processor — Complete Execution Guide

**Starting point:** `finalcombinedchannels` (your existing combined MOL+Telephony episodes table)  
**End point:** `fcr_analysis` (clean, analysis-ready dataset with demand/topic/repeat classification)

---

## Part 1: Create Checkpoint & Output Tables

```sql
%sql

-- =====================================================================
-- PASS 1: CREATE CHECKPOINT TABLE (per-episode analysis)
-- =====================================================================

CREATE TABLE IF NOT EXISTS axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.fcr_llm_pass1_checkpoint (
    ConversationId STRING,
    ClaimNumber STRING,
    ContactEpisode LONG,
    EpisodeStart TIMESTAMP,
    MembershipNumber STRING,
    primary_demand_type STRING,
    primary_demand_evidence STRING,
    primary_demand_confidence DOUBLE,
    secondary_demand_types_json STRING,
    topics_json STRING,
    primary_topic STRING,
    primary_topic_coverage_pct DOUBLE,
    resolution_status STRING,
    resolution_evidence STRING,
    resolution_agent_action STRING,
    resolution_customer_indication STRING,
    resolution_confidence DOUBLE,
    sentiment_score DOUBLE,
    sentiment_category STRING,
    sentiment_evidence STRING,
    sentiment_confidence DOUBLE,
    overall_confidence DOUBLE,
    prompt_tokens LONG,
    completion_tokens LONG,
    total_tokens LONG,
    api_latency DOUBLE,
    task_latency DOUBLE,
    request_id STRING,
    error STRING,
    processed_at TIMESTAMP
)
USING DELTA
COMMENT "Checkpoint for FCR Pass 1 - per-episode demand/topic/sentiment analysis";

-- =====================================================================
-- PASS 2: CREATE CHECKPOINT TABLE (repeat-contact comparison)
-- =====================================================================

CREATE TABLE IF NOT EXISTS axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.fcr_llm_pass2_checkpoint (
    ConversationId STRING,
    ClaimNumber STRING,
    ContactEpisode LONG,
    PreviousContactEpisode LONG,
    topic_similarity DOUBLE,
    same_underlying_issue BOOLEAN,
    continuing_previous_issue BOOLEAN,
    genuinely_new_issue BOOLEAN,
    repeat_contact BOOLEAN,
    repeat_contact_llm_confidence DOUBLE,
    repeat_contact_reason STRING,
    repeat_contact_evidence STRING,
    true_failure_supported BOOLEAN,
    prompt_tokens LONG,
    completion_tokens LONG,
    total_tokens LONG,
    api_latency DOUBLE,
    task_latency DOUBLE,
    request_id STRING,
    error STRING,
    processed_at TIMESTAMP
)
USING DELTA
COMMENT "Checkpoint for FCR Pass 2 - repeat-contact comparison";

-- Verify both created
SHOW TABLES LIKE 'fcr_llm_*';
```

---

## Part 2: Validate Input Table

```sql
%sql

-- =====================================================================
-- CHECK finalcombinedchannels STRUCTURE
-- =====================================================================

DESC TABLE axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels;

-- Check required columns exist
SELECT 
  CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as has_claim_number,
  CASE WHEN SUM(CASE WHEN ContactEpisode IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 'YES' ELSE 'NO' END as has_episode,
  CASE WHEN SUM(CASE WHEN CustomerEpisodeConversation IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 'YES' ELSE 'NO' END as has_customer_conv
FROM axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels;

-- Check data volume
SELECT COUNT(*) as total_episodes,
       COUNT(DISTINCT ClaimNumber) as unique_claims,
       COUNT(DISTINCT MembershipNumber) as unique_members,
       MIN(EpisodeStart) as earliest_date,
       MAX(EpisodeStart) as latest_date,
       COUNT(CASE WHEN PreviousContactEpisode IS NOT NULL THEN 1 END) as non_first_contact_episodes
FROM axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels;

-- Sample data
SELECT ClaimNumber, ContactEpisode, EpisodeStart, PreviousContactEpisode, 
       LEFT(CustomerEpisodeConversation, 100) as customer_text_sample
FROM axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels
LIMIT 5;
```

---

## Part 3: Python Setup & Authentication

```python
%python

# ==============================================
# CELL: IMPORTS & PATH SETUP
# ==============================================

import asyncio
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from msal import ConfidentialClientApplication
from azure.identity import ConfidentialClientCredential
from azure.keyvault.secrets import SecretClient
import pyspark.sql.functions as F

# Import the FCR LLM processor module
exec(open('/mnt/user-data/outputs/fcr_llm_processor.py').read())

print("[SETUP] All imports successful")
print("[SETUP] FCR LLM processor module loaded")
```

```python
%python

# ==============================================
# CELL: AUTHENTICATE & GET TOKEN
# ==============================================

# Service Connector Details
connector = "z-ppp-pr-dbr-keyvaultcredentials-key05"
vault_url = "https://z-ppp-en1-pr-dala-key05.vault.azure.net/"
client_id_key = "modelgateway-healthsgpt-client-id"
client_secret_key = "modelgateway-healthsgpt-client-secret"

# OAuth tenant and scope
tenant_id = "edd791b6-a6e2-450b-9582-5c29c2cc2d25"
scopes = ["https://axapppuk.onmicrosoft.com/modelgateway-api-pr/.default"]

# Model Gateway Base URL
modelgateway_baseurl = "https://your-model-gateway-url/"  # UPDATE THIS

# ========================
# Get Credentials from Key Vault
# ========================
def get_uc_secrets(connector: str, vault_url: str, *secret_keys: str) -> dict:
    """Retrieve secrets from Azure Key Vault"""
    credential = dbutils.credentials.getServiceCredentialsProvider(connector)
    client = SecretClient(vault_url=vault_url, credential=credential)
    return {k: client.get_secret(k).value for k in secret_keys}

try:
    secrets = get_uc_secrets(connector, vault_url, client_id_key, client_secret_key)
    client_id = secrets[client_id_key]
    client_secret = secrets[client_secret_key]
    print("[AUTH] Successfully retrieved secrets from Key Vault")
except Exception as e:
    print(f"[ERROR] Failed to get secrets: {e}")
    raise

# ========================
# Get OAuth Token
# ========================
def get_oauth_token(tenant_id, client_id, client_secret, scopes):
    """Acquire OAuth token for model gateway"""
    app = ConfidentialClientApplication(
        client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant_id}"
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in result:
        return result["access_token"]
    raise RuntimeError(f"Failed to acquire token: {result}")

try:
    token = get_oauth_token(tenant_id, client_id, client_secret, scopes)
    print(f"[AUTH] Token acquired successfully")
    print(f"[AUTH] Token preview: {token[:30]}...")
except Exception as e:
    print(f"[ERROR] Token acquisition failed: {e}")
    raise
```

---

## Part 4: Load & Prepare Data

```python
%python

# ==============================================
# CELL: LOAD finalcombinedchannels
# ==============================================

# Load the table
df_sp = spark.table("axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels")

print(f"[DATA] Loaded finalcombinedchannels")
print(f"[DATA] Total episodes: {df_sp.count():,}")

# Show schema
print("\n[DATA] Table schema:")
df_sp.printSchema()

# Show sample
print("\n[DATA] Sample episode:")
df_sp.limit(1).display()
```

```python
%python

# ==============================================
# CELL: VALIDATE INPUT SCHEMA
# ==============================================

# Check column existence using the validator
available_cols = df_sp.columns
print(f"[SCHEMA] Total columns available: {len(available_cols)}")

try:
    schema_info = inspect_input_schema(available_cols)
    print("\n[SCHEMA] Validation passed")
    print(f"[SCHEMA] Agent conversation column: {schema_info['agent_conversation_col']}")
    print(f"[SCHEMA] Optional columns available: {len(schema_info['available_optional'])}")
except ValueError as e:
    print(f"[ERROR] Schema validation failed: {e}")
    raise
```

```python
%python

# ==============================================
# CELL: CHUNK DATA BY DATE (for large datasets)
# ==============================================

# For 10K-50K episodes: single chunk
# For 50K-200K episodes: chunk by month or quarter
# For 200K+ episodes: chunk by month

# Define chunks based on your data volume
# Adjust dates to match your actual data range
date_chunks = [
    ("2024-01-01", "2024-03-31"),   # Q1 2024
    ("2024-04-01", "2024-06-30"),   # Q2 2024
    ("2024-07-01", "2024-09-30"),   # Q3 2024
    ("2024-10-01", "2024-12-31"),   # Q4 2024
]

# Option A: SMALL DATASET (<10K episodes) - no chunking needed
# df = df_sp.toPandas()

# Option B: LARGE DATASET - load in chunks
pandas_dfs = []
total_records = 0

for start_date, end_date in date_chunks:
    try:
        df_chunk = df_sp.filter(
            (F.col("EpisodeStart") >= start_date) &
            (F.col("EpisodeStart") <= end_date)
        )
        count = df_chunk.count()
        print(f"[CHUNK] {start_date} to {end_date}: {count:,} episodes")
        
        if count > 0:
            pandas_dfs.append(df_chunk.toPandas())
            total_records += count
    except Exception as e:
        print(f"[WARN] Could not load chunk {start_date}-{end_date}: {e}")

if pandas_dfs:
    df = pd.concat(pandas_dfs, ignore_index=True)
else:
    df = df_sp.toPandas()

print(f"\n[DATA] Total episodes loaded: {len(df):,}")
print(f"[DATA] Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
```

---

## Part 5: Run PASS 1 — Per-Episode Analysis (sample test first)

```python
%python

# ==============================================
# CELL: TEST ON SMALL SAMPLE FIRST
# ==============================================

df_sample = df.sample(n=min(5, len(df)), random_state=42)

print(f"[TEST] Running Pass 1 on {len(df_sample)} sample episodes")
print("[TEST] This will verify LLM connectivity and output format before full batch\n")

try:
    pass1_sample_results = run_fcr_pass1(
        df_sample, 
        schema_info,
        token=token,
        modelgateway_baseurl=modelgateway_baseurl,
        checkpoint_table=None,  # no checkpoint for sample test
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes
    )
    
    print(f"\n[TEST] Pass 1 sample complete: {len(pass1_sample_results)} episodes analyzed")
    print(f"[TEST] Columns in output: {len(pass1_sample_results.columns)}")
    
    # Show sample results
    print("\n[TEST] Sample output:")
    display(pass1_sample_results[['ClaimNumber', 'ContactEpisode', 'primary_demand_type', 
                                   'primary_topic', 'resolution_status', 'sentiment_score', 
                                   'error']].head())
    
except Exception as e:
    print(f"\n[ERROR] Sample test failed: {e}")
    import traceback
    traceback.print_exc()
    raise
```

```python
%python

# ==============================================
# CELL: RUN PASS 1 - FULL BATCH
# ==============================================

print("=" * 80)
print("PASS 1 - FULL BATCH")
print("=" * 80)

PASS1_CHECKPOINT = "axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.fcr_llm_pass1_checkpoint"

try:
    pass1_results = run_fcr_pass1(
        df,
        schema_info,
        token=token,
        modelgateway_baseurl=modelgateway_baseurl,
        checkpoint_table=PASS1_CHECKPOINT,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes
    )
    
    print(f"\n[SUCCESS] Pass 1 complete: {len(pass1_results):,} episodes processed")
    
except Exception as e:
    print(f"\n[ERROR] Pass 1 failed: {e}")
    import traceback
    traceback.print_exc()
    raise
```

---

## Part 6: Quality Check Pass 1 Results

```python
%python

# ==============================================
# CELL: CHECK PASS 1 RESULTS
# ==============================================

print("\n" + "=" * 80)
print("PASS 1 - QUALITY CHECK")
print("=" * 80)

total = len(pass1_results)
successful = (pass1_results['error'].isna()).sum()
failed = (pass1_results['error'].notna()).sum()

print(f"\nTotal episodes processed: {total:,}")
print(f"Successful: {successful:,} ({100*successful/total:.1f}%)")
print(f"Failed: {failed:,} ({100*failed/total:.1f}%)")

# Demand distribution
print("\n--- Demand Type Distribution ---")
demand_dist = pass1_results['primary_demand_type'].value_counts()
for demand, count in demand_dist.items():
    pct = (count / total) * 100
    print(f"  {demand}: {count:,} ({pct:.1f}%)")

# Resolution distribution
print("\n--- Resolution Status Distribution ---")
resolution_dist = pass1_results['resolution_status'].value_counts()
for status, count in resolution_dist.items():
    pct = (count / total) * 100
    print(f"  {status}: {count:,} ({pct:.1f}%)")

# Sentiment distribution
print("\n--- Sentiment Distribution ---")
sentiment_dist = pass1_results['sentiment_category'].value_counts()
for sent, count in sentiment_dist.items():
    pct = (count / total) * 100
    print(f"  {sent}: {count:,} ({pct:.1f}%)")

# Confidence
print("\n--- LLM Confidence Distribution ---")
conf_buckets = [
    (0.9, 1.0, "Very High (0.9-1.0)"),
    (0.7, 0.9, "High (0.7-0.9)"),
    (0.5, 0.7, "Medium (0.5-0.7)"),
    (0.0, 0.5, "Low (<0.5)"),
]

for min_conf, max_conf, label in conf_buckets:
    count = ((pass1_results['overall_confidence'] >= min_conf) & 
             (pass1_results['overall_confidence'] < max_conf)).sum()
    pct = (count / total) * 100
    print(f"  {label}: {count:,} ({pct:.1f}%)")

# Token usage
print("\n--- Token Usage ---")
total_tokens = pass1_results['total_tokens'].sum()
print(f"Total tokens used: {total_tokens:,}")
print(f"Avg tokens per episode: {total_tokens / successful:.0f}" if successful > 0 else "N/A")

# Errors
if failed > 0:
    print(f"\n--- Top 5 Errors ---")
    error_counts = pass1_results[pass1_results['error'].notna()]['error'].value_counts().head(5)
    for error_msg, count in error_counts.items():
        print(f"  {count:>3}: {error_msg[:60]}")
```

---

## Part 7: Prepare Pass 2 Input (only for non-first contacts)

```python
%python

# ==============================================
# CELL: BUILD PASS 2 INPUT
# ==============================================

# Convert Pass 1 results to Spark for SQL operations
pass1_spark = spark.createDataFrame(pass1_results)
pass1_spark.createOrReplaceTempView("pass1_results_temp")

# Load episodes that have previous contacts
non_first_spark = spark.table(
    "axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.finalcombinedchannels"
).filter(F.col("PreviousContactEpisode").isNotNull())

print(f"[PASS2] Episodes with previous contacts: {non_first_spark.count():,}")

# Build Pass 2 input using build_pass2_input function
# This joins current episode to its previous episode's Pass 1 output
pass2_input_pdf = build_pass2_input(
    non_first_spark.toPandas(),
    pass1_results
)

print(f"[PASS2] Input prepared for Pass 2: {len(pass2_input_pdf):,} episode pairs")

if len(pass2_input_pdf) == 0:
    print("[WARN] No episode pairs ready for Pass 2 yet - previous episodes may still be processing")
```

---

## Part 8: Run PASS 2 — Repeat Contact Comparison (sample then full)

```python
%python

# ==============================================
# CELL: TEST PASS 2 ON SAMPLE
# ==============================================

if len(pass2_input_pdf) > 0:
    pass2_sample = pass2_input_pdf.sample(n=min(3, len(pass2_input_pdf)), random_state=42)
    
    print(f"[TEST] Running Pass 2 on {len(pass2_sample)} sample episode pairs")
    
    try:
        pass2_sample_results = run_fcr_pass2(
            pass2_sample,
            token=token,
            modelgateway_baseurl=modelgateway_baseurl,
            checkpoint_table=None,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes
        )
        
        print(f"\n[TEST] Pass 2 sample complete: {len(pass2_sample_results)} episode pairs analyzed")
        print("\n[TEST] Sample output:")
        display(pass2_sample_results[['ClaimNumber', 'ContactEpisode', 'repeat_contact',
                                       'repeat_contact_llm_confidence', 'same_underlying_issue',
                                       'error']].head())
        
    except Exception as e:
        print(f"\n[ERROR] Pass 2 sample failed: {e}")
        import traceback
        traceback.print_exc()
```

```python
%python

# ==============================================
# CELL: RUN PASS 2 - FULL BATCH
# ==============================================

if len(pass2_input_pdf) > 0:
    print("=" * 80)
    print("PASS 2 - FULL BATCH (REPEAT CONTACT COMPARISON)")
    print("=" * 80)
    
    PASS2_CHECKPOINT = "axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.fcr_llm_pass2_checkpoint"
    
    try:
        pass2_results = run_fcr_pass2(
            pass2_input_pdf,
            token=token,
            modelgateway_baseurl=modelgateway_baseurl,
            checkpoint_table=PASS2_CHECKPOINT,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes
        )
        
        print(f"\n[SUCCESS] Pass 2 complete: {len(pass2_results):,} episode pairs processed")
        
    except Exception as e:
        print(f"\n[ERROR] Pass 2 failed: {e}")
        import traceback
        traceback.print_exc()
        pass2_results = pd.DataFrame()  # fallback to empty
else:
    print("[INFO] No episode pairs to process in Pass 2 yet")
    pass2_results = pd.DataFrame()
```

---

## Part 9: Check Pass 2 Results

```python
%python

# ==============================================
# CELL: CHECK PASS 2 RESULTS
# ==============================================

if len(pass2_results) > 0:
    print("\n" + "=" * 80)
    print("PASS 2 - QUALITY CHECK")
    print("=" * 80)
    
    total = len(pass2_results)
    successful = (pass2_results['error'].isna()).sum()
    failed = (pass2_results['error'].notna()).sum()
    
    print(f"\nTotal episode pairs processed: {total:,}")
    print(f"Successful: {successful:,} ({100*successful/total:.1f}%)")
    print(f"Failed: {failed:,} ({100*failed/total:.1f}%)")
    
    # Repeat contact rate
    print("\n--- Repeat Contact Distribution ---")
    repeat_dist = pass2_results[pass2_results['error'].isna()]['repeat_contact'].value_counts()
    for repeat, count in repeat_dist.items():
        pct = (count / successful) * 100
        label = "Repeat Contact" if repeat else "New Issue"
        print(f"  {label}: {count:,} ({pct:.1f}%)")
    
    # Confidence
    print("\n--- Repeat Contact Confidence (when identified as repeat) ---")
    repeats_only = pass2_results[(pass2_results['error'].isna()) & 
                                  (pass2_results['repeat_contact'] == True)]
    if len(repeats_only) > 0:
        print(f"  Mean confidence: {repeats_only['repeat_contact_llm_confidence'].mean():.2f}")
        print(f"  Median confidence: {repeats_only['repeat_contact_llm_confidence'].median():.2f}")
    
    # Errors
    if failed > 0:
        print(f"\n--- Top 5 Errors ---")
        error_counts = pass2_results[pass2_results['error'].notna()]['error'].value_counts().head(5)
        for error_msg, count in error_counts.items():
            print(f"  {count:>3}: {error_msg[:60]}")
else:
    print("[INFO] No Pass 2 results yet - either no non-first episodes or still processing")
```

---

## Part 10: Build Final Analysis Table

```sql
%sql

-- =====================================================================
-- BUILD FINAL FCR_ANALYSIS TABLE
-- =====================================================================

-- Run the simplified SQL pipeline to build the final table
-- (use the SQL from fcr_sql_pipeline_simplified.sql)

-- This creates:
-- 1. fcr_analysis - clean episode-level analytical dataset
-- 2. fcr_summary - overall FCR metrics
-- 3. fcr_by_demand - repeat rate by demand type
-- 4. fcr_by_topic - repeat rate by topic
-- 5. fcr_by_resolution - repeat rate by resolution status
-- 6. fcr_by_sentiment - repeat rate by sentiment

-- Verify the final table
SELECT COUNT(*) as total_episodes,
       COUNT(CASE WHEN contact_type = 'First Contact' THEN 1 END) as first_contacts,
       COUNT(CASE WHEN contact_type = 'Repeat Contact' THEN 1 END) as repeat_contacts,
       COUNT(CASE WHEN contact_type = 'New Issue' THEN 1 END) as new_issues
FROM axahealth_dataplatform_pd_lab.jogesh_rajiyan_axahealth.fcr_analysis;

-- View summary
SELECT * FROM fcr_summary;

-- View repeat rate by demand
SELECT * FROM fcr_by_demand;

-- View repeat rate by topic
SELECT * FROM fcr_by_topic;
```

---

## Part 11: Export & Archive

```python
%python

# ==============================================
# CELL: ARCHIVE RESULTS TO PARQUET
# ==============================================

# Save Pass 1 results to parquet for records
pass1_results.to_parquet(
    '/dbfs/mnt/data-lake/fcr/pass1_results_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.parquet',
    index=False
)

# Save Pass 2 results
if len(pass2_results) > 0:
    pass2_results.to_parquet(
        '/dbfs/mnt/data-lake/fcr/pass2_results_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.parquet',
        index=False
    )

print("[EXPORT] Results archived to data lake")
```

---

## Configuration Summary

| Component | Parameter | Value | Notes |
|-----------|-----------|-------|-------|
| **Input** | Source table | `finalcombinedchannels` | Episodes from combined MOL+Telephony |
| **Pass 1** | Checkpoint | `fcr_llm_pass1_checkpoint` | Per-episode demand/topic/sentiment |
| **Pass 2** | Checkpoint | `fcr_llm_pass2_checkpoint` | Repeat-contact comparison |
| **Output** | Final table | `fcr_analysis` | Ready for business analysis |
| **Model** | Model name | `gpt-4o-2024-11-20` | Latest available |
| **Concurrency** | Max parallel | 10 (default) | Reduce to 8 if rate-limited |
| **Rate Limits** | Token limit | 15M per 30 min | Automatic backoff if exceeded |
| **Timeouts** | API timeout | 180 sec | Per-request timeout |
| **Retries** | Max retries | 3 | For non-rate-limit errors |

---

## Expected Runtime

| Dataset Size | Pass 1 | Pass 2 | Total | Notes |
|--------------|--------|--------|-------|-------|
| 1K episodes | 10-15 min | 5 min | 15-20 min | Small test run |
| 10K episodes | 1-1.5 hours | 30 min | 1.5-2 hours | Typical batch |
| 50K episodes | 4-5 hours | 2-3 hours | 6-8 hours | Split into 2 chunks |
| 100K+ episodes | 8-12 hours | 4-6 hours | 12-18 hours | Run overnight |

Times vary by:
- Conversation length (longer = slower)
- Model gateway latency (network)
- Cluster resources (parallelism)
- Token volume (rate-limit pauses)

---

## Troubleshooting

### "Token limit exceeded"
```python
# In authentication cell, reduce concurrency:
MAX_CONCURRENCY = 8  # from 10
SAFETY_BUFFER = 0.85  # from 0.90
```

### "Schema validation failed"
```sql
-- Verify agent conversation column exists:
SELECT COUNT(*) 
FROM finalcombinedchannels 
WHERE RelevantAgentEpisodeConversation IS NOT NULL 
   OR AgentEpisodeConversation IS NOT NULL;
```

### "Pass 2 input is empty"
```sql
-- Check if Pass 1 results are still checkpointing:
SELECT COUNT(*) FROM fcr_llm_pass1_checkpoint 
WHERE error IS NULL;

-- Then re-run Part 7 to rebuild Pass 2 input
```

### "Resume after failure"
```python
# The checkpoint tables persist across runs
# Simply re-run the processor with the same parameters
# It will skip already-successful rows automatically
```

### "Check checkpoint progress"
```sql
SELECT 
  'Pass 1' as pass,
  COUNT(*) as total_rows,
  COUNT(CASE WHEN error IS NULL THEN 1 END) as successful,
  COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as failed
FROM fcr_llm_pass1_checkpoint

UNION ALL

SELECT 
  'Pass 2' as pass,
  COUNT(*),
  COUNT(CASE WHEN error IS NULL THEN 1 END),
  COUNT(CASE WHEN error IS NOT NULL THEN 1 END)
FROM fcr_llm_pass2_checkpoint;
```

---

## Key Differences from Telephony Processor

| Aspect | Telephony | FCR |
|--------|-----------|-----|
| **Input grain** | Conversation | Episode |
| **Passes** | 1 (extract messages) | 2 (analyze + compare) |
| **Output complexity** | 2 fields (customer/agent msg) | 20+ fields (demand/topic/sentiment/repeat) |
| **Dependencies** | None | Pass 2 depends on Pass 1 checkpoint |
| **Resume logic** | Row-level dedup | Row-level dedup per pass |
| **Validation** | Basic (message presence) | Structural + semantic |

---

## After Completion

Once `fcr_analysis` is ready:

1. **Query the summary reports** (see FCR_PIPELINE_GUIDE.md)
2. **Identify pain points** (high repeat-rate demand types/topics)
3. **Drill into specific episodes** using ClaimNumber/ContactEpisode keys
4. **Join to downstream business systems** using ClaimNumber
5. **Build dashboards** from the 5 summary views
6. **Archive results** with timestamp for audit trail