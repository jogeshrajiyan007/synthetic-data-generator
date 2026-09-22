# =====================================
# TELEPHONY LLM BATCH PROCESSOR
# =====================================
# Safely process large telephony transcripts in chunks
# with rate limiting, retry logic, checkpointing, and PII redaction
# =====================================

import asyncio
import json
import time
import re
import traceback
import pandas as pd
import numpy as np
from collections import deque
from typing import Dict, Any, Tuple, List
from datetime import datetime

# =====================================
# CONFIGURATION
# =====================================
MODEL = "gpt-4o-2024-11-20"
MAX_CONCURRENCY = 15  # parallel LLM calls
MIN_TRANSCRIPT_CHARS = 100
MAX_TOKENS = 1200
RETRIES = 3

# =====================================
# RATE LIMITING & TOKEN MANAGEMENT
# =====================================
TOKEN_LIMIT = 15_000_000  # 15M tokens per 30 min window
WINDOW_SECONDS = 60 * 30  # 30 minute window
SAFETY_BUFFER = 0.90  # stay at 90% of limit
ESTIMATED_TOKENS_PER_CALL = 1800

DATE_CHUNKS = [
    ("2025-07-01", "2025-11-30"),  # Chunk 1: Jul–Nov 2025 UNCOMMENTED ON 30/8/26 16:00
    # ("2025-12-01", "2026-01-09"),  # Chunk 2: Dec 2025–jan 2026 UNCOMMENT ON NEXT RUN 28/8/26 10:37, RAN ON 30/8/26,
    # ("2026-01-10", "2026-03-01"), # chunk 2.25: jan-feb26 RAN ON 1/9/26 9:33
    # ("2026-03-02", "2026-04-30"), # CHUNK 2.5 (DATA TOO BIG): FEB-APR 2026 UNCOMMENT ON NEXT RUN 30/8/26 19:18
    # ("2026-05-01", "2026-08-26")   # Chunk 3: May–Aug 2026 DO NOT UNCOMMENT ON NEXT RUN 28/8/26 10:37
    # Adjust these based on your data volume
]

# =====================================
# PII REDACTION PATTERNS
# =====================================
PII_PATTERNS = [
    re.compile(r"\b\d{3}\s?\d{3}\s?\d{4}\b"),           # Phone: XXX XXX XXXX
    re.compile(r"\b\d{10,}\b"),                        # Long digit sequences (policy, membership)
    re.compile(r"\b(?:\+?1?[-.\s]?\(?)?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})\b"),  # Various phone formats
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}"),  # Emails
    re.compile(r"\b[A-Z]{2}\d{6}\b"),                  # UK ID format (e.g., AB123456)
    re.compile(r"(?:DOB|dob|date of birth)[:\s]*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", re.IGNORECASE),  # DOB
    re.compile(r"(?:postcode|postal code|zip)[:\s]*[A-Z0-9]{2,4}\s?[A-Z0-9]{1,3}", re.IGNORECASE),  # Postcode
    re.compile(r"(?:sort code|account)[:\s]*\d{2}[-\s]?\d{2}[-\s]?\d{2}", re.IGNORECASE),  # Bank details
]

def redact_pii(text: str) -> str:
    """Redact PII from transcript before sending to LLM"""
    if not isinstance(text, str):
        return ""
    
    redacted = text
    for pattern in PII_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    
    return redacted.strip()

# =====================================
# TRANSCRIPT PARSING
# =====================================
def parse_transcript_into_turns(transcript: str) -> List[Dict[str, str]]:
    """
    Parse transcript into structured turns.
    Expected format:
    Agent: message
    Customer: message
    Agent: message
    ...
    """
    if not transcript:
        return []
    
    turns = []
    lines = transcript.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        if line.startswith('Agent:'):
            turns.append({
                'speaker': 'agent',
                'text': line[6:].strip()
            })
        elif line.startswith('Customer:'):
            turns.append({
                'speaker': 'customer',
                'text': line[9:].strip()
            })
        elif ':' in line:
            # Handle other formats like "Ekaterina McLaughlin (02:30:15): message"
            parts = line.split(':', 1)
            if len(parts) == 2:
                speaker_part = parts[0].strip()
                message = parts[1].strip()
                
                # Determine if agent or customer
                if any(x in speaker_part.lower() for x in ['agent', 'support', 'representative', 'tech', 'customer service']):
                    turns.append({'speaker': 'agent', 'text': message})
                elif any(x in speaker_part.lower() for x in ['customer', 'caller', 'client', 'member']):
                    turns.append({'speaker': 'customer', 'text': message})
    
    return turns

def build_clean_transcript_for_llm(transcript: str) -> str:
    """
    Build a clean, structured transcript for LLM analysis.
    Handles multi-agent calls gracefully by combining agent turns chronologically.
    """
    turns = parse_transcript_into_turns(transcript)
    
    if not turns:
        return ""
    
    # Redact PII
    turns = [
        {'speaker': t['speaker'], 'text': redact_pii(t['text'])}
        for t in turns
    ]
    
    # Rebuild as clean alternating turns
    clean_lines = []
    for turn in turns:
        speaker_label = "Agent" if turn['speaker'] == 'agent' else "Customer"
        clean_lines.append(f"{speaker_label}: {turn['text']}")
    
    return '\n'.join(clean_lines)

# =====================================
# LLM JSON SCHEMA & PROMPTS
# =====================================
TELEPHONY_JSON_SCHEMA = {
    "name": "telephony_message_split",
    "schema": {
        "type": "object",
        "properties": {
            "customer_message": {
                "type": "string",
                "description": "Summary of what the customer asked for, reported, or was concerned about"
            },
            "agent_message": {
                "type": "string",
                "description": "Summary of what the agent(s) explained, did, or outcome communicated"
            },
            "confidence": {
                "type": "number",
                "description": "Confidence score 0-1 on how well the split represents the call"
            }
        },
        "required": ["customer_message", "agent_message", "confidence"],
        "additionalProperties": False
    }
}

def build_telephony_prompt(clean_transcript: str, agents_involved: str = None) -> str:
    """Build the prompt for telephony transcript analysis"""
    
    agent_context = ""
    if agents_involved:
        agent_context = f"\nNote: This call involved {agents_involved}. If multiple agents handled the call, combine their responses into one coherent resolution summary.\n"
    
    return f"""
You are an expert AXA Health customer service analyst. Your task is to analyse a telephone call transcript and extract TWO separate summaries:

1. **customer_message**: What the customer asked for, reported, or was concerned about. Write this from the customer's perspective/intent, covering the entire call. Focus on their needs, problems, or questions.

2. **agent_message**: What the agent(s) explained, did, or what outcomes were communicated. If multiple agents handled this call (transfers, consultations), combine their contributions into one coherent summary showing what was resolved or actioned.

Guidelines:
- Do NOT invent information not present in the transcript.
- Do NOT include internal jargon or system references unless they were explicitly discussed with the customer.
- If the transcript is too short or unclear, return empty strings for the relevant field rather than guessing.
- Both summaries should be 1-3 sentences, clear and actionable.
- Flag any PII redactions ([REDACTED]) if they were critical context (e.g., "customer provided [REDACTED] as confirmation").
{agent_context}

TRANSCRIPT (may contain multiple agent turns or transfers):
----------
{clean_transcript}
----------

Provide your response as JSON with only the three fields: customer_message, agent_message, confidence.
""".strip()

def custom_json_loader(output_text: str) -> Dict[str, Any]:
    """Parse JSON with fallback to extracting outermost object"""
    try:
        return json.loads(output_text)
    except json.JSONDecodeError:
        start = output_text.find("{")
        end = output_text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(output_text[start:end+1])
        raise

def validate_and_repair_telephony_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and repair model response"""
    if not isinstance(response, dict):
        response = {}
    
    customer_msg = str(response.get("customer_message", "")).strip() or None
    agent_msg = str(response.get("agent_message", "")).strip() or None
    confidence = response.get("confidence")
    
    # Validate confidence
    try:
        confidence = float(confidence or 0)
        confidence = max(0.0, min(1.0, confidence))
    except (ValueError, TypeError):
        confidence = 0.5
    
    # If both are empty, mark low confidence
    if not customer_msg and not agent_msg:
        confidence = 0.0
    
    return {
        "customer_message": customer_msg,
        "agent_message": agent_msg,
        "confidence": confidence
    }

# =====================================
# MODEL API CALLS
# =====================================
def call_telephony_model(
    prompt: str,
    max_tokens: int = MAX_TOKENS,
    retries: int = RETRIES,
    token: str = None,
    modelgateway_baseurl: str = None,
    api_timeout: int = 60
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Call the model with retry logic and error handling.
    Returns (analysis, metadata)
    """
    apiurl = f"{modelgateway_baseurl}secure-gpt-openai/openai/deployments/{MODEL}/chat/completions?api-version=2024-06-01"
    
    payload = {
        "messages": [
            {
                "role": "system",
                "content": "You are an expert AXA Health customer service analyst who separates call transcripts into customer and agent contributions."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": TELEPHONY_JSON_SCHEMA["name"],
                "schema": TELEPHONY_JSON_SCHEMA["schema"],
                "strict": True
            }
        }
    }
    
    last_exception = None
    
    for attempt in range(retries):
        start_api = time.time()
        try:
            # Import here to avoid hardcoding
            import requests
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            resp = requests.post(
                apiurl,
                headers=headers,
                json=payload,
                verify=True,
                timeout=api_timeout
            )
            resp.raise_for_status()
            
            api_latency = time.time() - start_api
            request_id = resp.headers.get("x-request-id")
            
            r = resp.json()
            choice = r["choices"][0]["message"]
            
            if "parsed" in choice:
                parsed = choice["parsed"]
            elif choice.get("content"):
                parsed = custom_json_loader(choice["content"])
            else:
                raise ValueError("No model content returned")
            
            cleaned = validate_and_repair_telephony_response(parsed)
            
            usage = r.get("usage", {})
            meta = {
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "api_latency": api_latency,
                "request_id": request_id,
                "attempt": attempt + 1,
                "error": None
            }
            
            return cleaned, meta
        
        except Exception as e:
            last_exception = e
            if attempt < retries - 1:
                wait_time = 2 ** attempt + np.random.rand()  # exponential backoff + jitter
                time.sleep(wait_time)
            else:
                meta = {
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "api_latency": time.time() - start_api,
                    "request_id": None,
                    "attempt": attempt + 1,
                    "error": f"{type(e).__name__}: {str(e)}"
                }
                return {
                    "customer_message": None,
                    "agent_message": None,
                    "confidence": 0.0
                }, meta
    
    raise RuntimeError(f"Model call failed after {retries} attempts: {last_exception}")

# =====================================
# ASYNC BATCH PROCESSING
# =====================================
class TokenBudgetManager:
    """Manages token rate limiting across concurrent tasks"""
    
    def __init__(self, token_limit: int, window_secs: int, safety_buffer: float = 0.90):
        self.token_limit = token_limit
        self.window_secs = window_secs
        self.safety_buffer = safety_buffer
        self.token_usage_window = deque()  # (timestamp, tokens)
        self.current_tokens = 0
        self.lock = asyncio.Lock()
    
    async def clean_old_tokens(self):
        """Remove tokens outside the window"""
        now = time.time()
        async with self.lock:
            while self.token_usage_window and now - self.token_usage_window[0][0] > self.window_secs:
                _, old_tokens = self.token_usage_window.popleft()
                self.current_tokens = max(0, self.current_tokens - old_tokens)
    
    async def throttle_if_needed(self, estimated_tokens: int = ESTIMATED_TOKENS_PER_CALL):
        """Wait if approaching token limit"""
        await self.clean_old_tokens()
        limit = self.token_limit * self.safety_buffer
        
        while self.current_tokens + estimated_tokens > limit:
            pressure = self.current_tokens / limit
            sleep_time = min(1 + pressure * 4, 8)
            print(f"[THROTTLE] Tokens: {self.current_tokens:,}/{int(limit):,} | Sleeping {sleep_time:.1f}s")
            await asyncio.sleep(sleep_time)
            await self.clean_old_tokens()
    
    async def record_tokens(self, token_count: int):
        """Record token usage"""
        async with self.lock:
            now = time.time()
            self.token_usage_window.append((now, token_count))
            self.current_tokens += token_count

# =====================================
# MAIN BATCH PROCESSOR
# =====================================
async def process_telephony_batch_async(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None
) -> pd.DataFrame:
    """
    Process telephony transcripts in batches with:
    - Async concurrency control
    - Token rate limiting
    - Retry logic
    - PII redaction
    - Checkpoint saving
    """
    
    # Validate input
    required_cols = ["ConversationId", "ConversationStartTimestamp", "ConversationTranscript", "ClaimNumber"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    df = df.reset_index(drop=True).copy()
    
    # Filter already-processed if checkpoint exists
    if checkpoint_table:
        try:
            checkpoint_df = spark.table(checkpoint_table).toPandas()
            processed_ids = set(checkpoint_df["ConversationId"].unique())
            df = df[~df["ConversationId"].isin(processed_ids)].copy()
            print(f"[CHECKPOINT] Resuming: {len(df)} unprocessed records remaining")
        except Exception as e:
            print(f"[CHECKPOINT] Could not load checkpoint (first run?): {e}")
    
    # Filter by minimum transcript length
    df = df[df["ConversationTranscript"].astype(str).str.len() >= MIN_TRANSCRIPT_CHARS].copy()
    print(f"[FILTER] Processing {len(df)} records with transcript >= {MIN_TRANSCRIPT_CHARS} chars")
    
    # Initialize output
    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    token_mgr = TokenBudgetManager(TOKEN_LIMIT, WINDOW_SECONDS, SAFETY_BUFFER)
    
    async def process_single_row(idx: int, row: pd.Series):
        """Process one transcript"""
        async with semaphore:
            task_start = time.time()
            
            try:
                # Throttle if needed
                await token_mgr.throttle_if_needed(ESTIMATED_TOKENS_PER_CALL)
                
                # Clean transcript
                clean_transcript = build_clean_transcript_for_llm(row["ConversationTranscript"])
                
                if len(clean_transcript) < MIN_TRANSCRIPT_CHARS:
                    print(f"[SKIP] {row['ConversationId']}: transcript too short after PII redaction")
                    return {
                        "ConversationId": row["ConversationId"],
                        "ClaimNumber": row["ClaimNumber"],
                        "ConversationStartTimestamp": row["ConversationStartTimestamp"],
                        "CustomerMessage": None,
                        "AgentMessage": None,
                        "Confidence": 0.0,
                        "prompt_tokens": None,
                        "completion_tokens": None,
                        "total_tokens": None,
                        "api_latency": None,
                        "task_latency": time.time() - task_start,
                        "request_id": None,
                        "error": "Transcript too short after redaction"
                    }
                
                # Build prompt
                agents_str = row.get("AgentsInvolved", "")
                prompt = build_telephony_prompt(clean_transcript, agents_str)
                
                # Call model
                analysis, meta = call_telephony_model(
                    prompt,
                    max_tokens=MAX_TOKENS,
                    retries=RETRIES,
                    token=token,
                    modelgateway_baseurl=modelgateway_baseurl
                )
                
                # Record tokens
                if meta.get("total_tokens"):
                    await token_mgr.record_tokens(meta["total_tokens"])
                
                # Build result
                result = {
                    "ConversationId": row["ConversationId"],
                    "ClaimNumber": row["ClaimNumber"],
                    "ConversationStartTimestamp": row["ConversationStartTimestamp"],
                    "CustomerMessage": analysis.get("customer_message"),
                    "AgentMessage": analysis.get("agent_message"),
                    "Confidence": analysis.get("confidence"),
                    "prompt_tokens": meta.get("prompt_tokens"),
                    "completion_tokens": meta.get("completion_tokens"),
                    "total_tokens": meta.get("total_tokens"),
                    "api_latency": meta.get("api_latency"),
                    "task_latency": time.time() - task_start,
                    "request_id": meta.get("request_id"),
                    "error": meta.get("error")
                }
                
                # Progress logging
                if (len(results) + 1) % 50 == 0:
                    print(f"[PROGRESS] Processed {len(results) + 1}/{len(df)} | Avg confidence: {np.mean([r.get('Confidence', 0) for r in results]):.2f}")
                
                return result
            
            except Exception as e:
                print(f"[ERROR] {row['ConversationId']}: {str(e)}")
                return {
                    "ConversationId": row["ConversationId"],
                    "ClaimNumber": row["ClaimNumber"],
                    "ConversationStartTimestamp": row["ConversationStartTimestamp"],
                    "CustomerMessage": None,
                    "AgentMessage": None,
                    "Confidence": 0.0,
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "api_latency": None,
                    "task_latency": time.time() - task_start,
                    "request_id": None,
                    "error": f"{type(e).__name__}: {str(e)}"
                }
    
    # Run all tasks concurrently
    tasks = [process_single_row(i, row) for i, (_, row) in enumerate(df.iterrows())]
    results = await asyncio.gather(*tasks)
    
    # Convert to dataframe
    df_out = pd.DataFrame(results)
    
    # Save checkpoint if specified
    if checkpoint_table:
        try:
            spark_df = spark.createDataFrame(df_out)
            spark_df.write.mode("append").format("delta").saveAsTable(checkpoint_table)
            print(f"[CHECKPOINT] Saved {len(df_out)} results")
        except Exception as e:
            print(f"[WARNING] Could not save checkpoint: {e}")
    
    return df_out

# =====================================
# USAGE WRAPPER (SYNC CALLER)
# =====================================
def run_telephony_llm_batch(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None,
    date_chunks: List[Tuple[str, str]] = None
) -> pd.DataFrame:
    """
    Wrapper to run async batch processor from sync context.
    If date_chunks provided, processes in chunks and appends to output table.
    """
    
    all_results = []
    
    if date_chunks:
        for start_date, end_date in date_chunks:
            print(f"\n[CHUNK] Processing {start_date} to {end_date}")
            df_chunk = df[
                (df["ConversationStartTimestamp"] >= start_date) &
                (df["ConversationStartTimestamp"] <= end_date)
            ].copy()
            
            if len(df_chunk) == 0:
                print(f"[CHUNK] No data in range {start_date} to {end_date}")
                continue
            
            # Run async processor
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                df_chunk_out = loop.run_until_complete(
                    process_telephony_batch_async(
                        df_chunk,
                        token,
                        modelgateway_baseurl,
                        checkpoint_table,
                        output_table
                    )
                )
                all_results.append(df_chunk_out)
            finally:
                loop.close()
    else:
        # Single run
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            df_out = loop.run_until_complete(
                process_telephony_batch_async(
                    df,
                    token,
                    modelgateway_baseurl,
                    checkpoint_table,
                    output_table
                )
            )
            all_results.append(df_out)
        finally:
            loop.close()
    
    if all_results:
        return pd.concat(all_results, ignore_index=True)
    else:
        return pd.DataFrame()
