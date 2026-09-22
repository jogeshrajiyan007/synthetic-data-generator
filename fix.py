# ================================================================
# TELEPHONY LLM BATCH PROCESSOR - PRODUCTION VERSION
# ================================================================
#
# Features:
#   - Async concurrent LLM calls
#   - Blocking requests moved to background threads
#   - Token rate limiting
#   - Retry with exponential backoff
#   - PII redaction
#   - Incremental checkpointing
#   - Resume from checkpoint
#   - Failed records can be retried on next run
#   - Live progress bar
#   - Processing rate
#   - ETA
#   - Success / failure counts
#   - Controlled logging
#   - Databricks notebook event-loop safe
#
# ================================================================

import asyncio
import json
import time
import re
import traceback
import threading

import pandas as pd
import numpy as np

from collections import deque
from typing import Dict, Any, Tuple, List
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import requests
from tqdm.auto import tqdm


# ================================================================
# CONFIGURATION
# ================================================================

MODEL = "gpt-4o-2024-11-20"

# Number of simultaneous LLM requests
MAX_CONCURRENCY = 15

# Minimum transcript length before LLM processing
MIN_TRANSCRIPT_CHARS = 100

# Maximum completion tokens
MAX_TOKENS = 1200

# API retries
RETRIES = 3

# API timeout
API_TIMEOUT = 90

# ================================================================
# CHECKPOINT CONFIGURATION
# ================================================================

# Save every N completed records
CHECKPOINT_EVERY = 250

# Retry checkpoint writes
CHECKPOINT_RETRIES = 3


# ================================================================
# TOKEN RATE LIMITING
# ================================================================

TOKEN_LIMIT = 15_000_000

# 30 minute rolling window
WINDOW_SECONDS = 60 * 30

# Use 90% of provider limit
SAFETY_BUFFER = 0.90

# Estimated tokens per request.
#
# This is used BEFORE the request to reserve capacity.
# Actual usage is recorded after the request.
#
# Increase this if your real requests regularly exceed this.
ESTIMATED_TOKENS_PER_CALL = 1800


# ================================================================
# PII REDACTION
# ================================================================

PII_PATTERNS = [

    # Phone: 123 456 7890
    re.compile(
        r"\b\d{3}\s?\d{3}\s?\d{4}\b"
    ),

    # Long numeric sequences
    re.compile(
        r"\b\d{10,}\b"
    ),

    # Various phone formats
    re.compile(
        r"\b(?:\+?1?[-.\s]?\(?)?"
        r"(\d{3})\)?[-.\s]?"
        r"(\d{3})[-.\s]?"
        r"(\d{4})\b"
    ),

    # Email
    re.compile(
        r"[A-Za-z0-9._%+-]+"
        r"@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}"
    ),

    # UK ID style
    re.compile(
        r"\b[A-Z]{2}\d{6}\b"
    ),

    # DOB
    re.compile(
        r"(?:DOB|dob|date of birth)"
        r"[:\s]*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}",
        re.IGNORECASE
    ),

    # Postcode
    re.compile(
        r"(?:postcode|postal code|zip)"
        r"[:\s]*[A-Z0-9]{2,4}\s?[A-Z0-9]{1,3}",
        re.IGNORECASE
    ),

    # Sort code / bank account
    re.compile(
        r"(?:sort code|account)"
        r"[:\s]*\d{2}[-\s]?\d{2}[-\s]?\d{2}",
        re.IGNORECASE
    ),
]


def redact_pii(text: str) -> str:

    if not isinstance(text, str):
        return ""

    redacted = text

    for pattern in PII_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)

    return redacted.strip()


# ================================================================
# TRANSCRIPT PARSING
# ================================================================

def parse_transcript_into_turns(
    transcript: str
) -> List[Dict[str, str]]:

    if not transcript:
        return []

    turns = []
    lines = transcript.split("\n")

    for line in lines:

        line = line.strip()

        if not line:
            continue

        # --------------------------------------------------------
        # Explicit Agent
        # --------------------------------------------------------

        if line.startswith("Agent:"):

            turns.append({
                "speaker": "agent",
                "text": line[6:].strip()
            })

        # --------------------------------------------------------
        # Explicit Customer
        # --------------------------------------------------------

        elif line.startswith("Customer:"):

            turns.append({
                "speaker": "customer",
                "text": line[9:].strip()
            })

        # --------------------------------------------------------
        # Other transcript formats
        # --------------------------------------------------------

        elif ":" in line:

            parts = line.split(":", 1)

            if len(parts) != 2:
                continue

            speaker_part = parts[0].strip()
            message = parts[1].strip()

            speaker_lower = speaker_part.lower()

            if any(
                x in speaker_lower
                for x in [
                    "agent",
                    "support",
                    "representative",
                    "tech",
                    "customer service"
                ]
            ):

                turns.append({
                    "speaker": "agent",
                    "text": message
                })

            elif any(
                x in speaker_lower
                for x in [
                    "customer",
                    "caller",
                    "client",
                    "member"
                ]
            ):

                turns.append({
                    "speaker": "customer",
                    "text": message
                })

    return turns


def build_clean_transcript_for_llm(
    transcript: str
) -> str:

    turns = parse_transcript_into_turns(transcript)

    if not turns:
        return ""

    clean_lines = []

    for turn in turns:

        text = redact_pii(turn["text"])

        speaker_label = (
            "Agent"
            if turn["speaker"] == "agent"
            else "Customer"
        )

        clean_lines.append(
            f"{speaker_label}: {text}"
        )

    return "\n".join(clean_lines)


# ================================================================
# LLM JSON SCHEMA
# ================================================================

TELEPHONY_JSON_SCHEMA = {

    "name": "telephony_message_split",

    "schema": {

        "type": "object",

        "properties": {

            "customer_message": {
                "type": "string",
                "description":
                    "Summary of what the customer asked for, "
                    "reported, or was concerned about"
            },

            "agent_message": {
                "type": "string",
                "description":
                    "Summary of what the agent explained, "
                    "did, or outcome communicated"
            },

            "confidence": {
                "type": "number",
                "description":
                    "Confidence score between 0 and 1"
            }
        },

        "required": [
            "customer_message",
            "agent_message",
            "confidence"
        ],

        "additionalProperties": False
    }
}


# ================================================================
# PROMPT
# ================================================================

def build_telephony_prompt(
    clean_transcript: str,
    agents_involved: str = None
) -> str:

    agent_context = ""

    if agents_involved:

        agent_context = f"""
Note:
This call involved {agents_involved}.

If multiple agents handled the call, combine their
responses into one coherent resolution summary.
"""

    return f"""
You are an expert AXA Health customer service analyst.

Analyse the telephone call transcript and extract TWO
separate summaries.

1. customer_message

Summarise what the customer asked for, reported,
or was concerned about.

Write this from the customer's perspective and intent.

2. agent_message

Summarise what the agent(s) explained, did,
or what outcomes were communicated.

If multiple agents handled the call, combine their
contributions into one coherent resolution summary.

Rules:

- Do NOT invent information.
- Do NOT infer information not present in the transcript.
- Do NOT include internal system jargon unless discussed
  with the customer.
- If the transcript is unclear, use an empty string.
- Keep each summary to 1-3 sentences.
- Make the summaries clear and actionable.
- Preserve important context.
- [REDACTED] may appear where PII was removed.

{agent_context}

TRANSCRIPT
--------------------------------------------------

{clean_transcript}

--------------------------------------------------

Return ONLY JSON containing:

customer_message
agent_message
confidence

Confidence must be between 0 and 1.
""".strip()


# ================================================================
# JSON PARSER
# ================================================================

def custom_json_loader(
    output_text: str
) -> Dict[str, Any]:

    try:

        return json.loads(output_text)

    except json.JSONDecodeError:

        start = output_text.find("{")
        end = output_text.rfind("}")

        if (
            start != -1
            and end != -1
            and end > start
        ):

            return json.loads(
                output_text[start:end + 1]
            )

        raise


# ================================================================
# RESPONSE VALIDATION
# ================================================================

def validate_and_repair_telephony_response(
    response: Dict[str, Any]
) -> Dict[str, Any]:

    if not isinstance(response, dict):
        response = {}

    customer_msg = (
        str(
            response.get(
                "customer_message",
                ""
            )
        ).strip()
        or None
    )

    agent_msg = (
        str(
            response.get(
                "agent_message",
                ""
            )
        ).strip()
        or None
    )

    confidence = response.get("confidence")

    try:

        confidence = float(
            confidence or 0
        )

        confidence = max(
            0.0,
            min(1.0, confidence)
        )

    except (
        ValueError,
        TypeError
    ):

        confidence = 0.5

    if not customer_msg and not agent_msg:

        confidence = 0.0

    return {

        "customer_message":
            customer_msg,

        "agent_message":
            agent_msg,

        "confidence":
            confidence
    }


# ================================================================
# SYNCHRONOUS MODEL CALL
# ================================================================
#
# IMPORTANT:
# requests.post is blocking.
#
# The async processor below calls this using:
#
# asyncio.to_thread(...)
#
# Therefore MAX_CONCURRENCY actually works.
# ================================================================

def call_telephony_model(

    prompt: str,

    max_tokens: int = MAX_TOKENS,

    retries: int = RETRIES,

    token: str = None,

    modelgateway_baseurl: str = None,

    api_timeout: int = API_TIMEOUT

) -> Tuple[Dict[str, Any], Dict[str, Any]]:

    if not modelgateway_baseurl:
        raise ValueError(
            "modelgateway_baseurl is required"
        )

    base_url = modelgateway_baseurl.rstrip("/")

    apiurl = (
        f"{base_url}"
        f"/secure-gpt-openai/openai/deployments/"
        f"{MODEL}"
        f"/chat/completions"
        f"?api-version=2024-06-01"
    )

    payload = {

        "messages": [

            {
                "role": "system",
                "content":
                    "You are an expert AXA Health "
                    "customer service analyst who "
                    "separates call transcripts into "
                    "customer and agent contributions."
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

                "name":
                    TELEPHONY_JSON_SCHEMA["name"],

                "schema":
                    TELEPHONY_JSON_SCHEMA["schema"],

                "strict": True
            }
        }
    }

    last_exception = None

    for attempt in range(1, retries + 1):

        start_api = time.time()

        try:

            headers = {

                "Authorization":
                    f"Bearer {token}",

                "Content-Type":
                    "application/json"
            }

            response = requests.post(

                apiurl,

                headers=headers,

                json=payload,

                verify=True,

                timeout=api_timeout
            )

            response.raise_for_status()

            api_latency = (
                time.time()
                - start_api
            )

            request_id = (
                response.headers.get(
                    "x-request-id"
                )
            )

            data = response.json()

            choice = (
                data["choices"][0]["message"]
            )

            if "parsed" in choice:

                parsed = choice["parsed"]

            elif choice.get("content"):

                parsed = custom_json_loader(
                    choice["content"]
                )

            else:

                raise ValueError(
                    "No model content returned"
                )

            cleaned = (
                validate_and_repair_telephony_response(
                    parsed
                )
            )

            usage = data.get(
                "usage",
                {}
            )

            metadata = {

                "prompt_tokens":
                    usage.get(
                        "prompt_tokens"
                    ),

                "completion_tokens":
                    usage.get(
                        "completion_tokens"
                    ),

                "total_tokens":
                    usage.get(
                        "total_tokens"
                    ),

                "api_latency":
                    api_latency,

                "request_id":
                    request_id,

                "attempt":
                    attempt,

                "error":
                    None
            }

            return cleaned, metadata

        except Exception as e:

            last_exception = e

            if attempt < retries:

                wait_time = (
                    (2 ** (attempt - 1))
                    + np.random.rand()
                )

                time.sleep(
                    wait_time
                )

            else:

                return (

                    {
                        "customer_message":
                            None,

                        "agent_message":
                            None,

                        "confidence":
                            0.0
                    },

                    {
                        "prompt_tokens":
                            None,

                        "completion_tokens":
                            None,

                        "total_tokens":
                            None,

                        "api_latency":
                            time.time()
                            - start_api,

                        "request_id":
                            None,

                        "attempt":
                            attempt,

                        "error":
                            f"{type(e).__name__}: {str(e)}"
                    }
                )

    raise RuntimeError(
        f"Model call failed: {last_exception}"
    )


# ================================================================
# TOKEN BUDGET MANAGER
# ================================================================

class TokenBudgetManager:

    def __init__(
        self,
        token_limit: int,
        window_secs: int,
        safety_buffer: float = 0.90
    ):

        self.token_limit = token_limit

        self.window_secs = window_secs

        self.safety_buffer = safety_buffer

        self.token_usage_window = deque()

        self.current_tokens = 0

        self.lock = asyncio.Lock()

    async def _clean_old_tokens_locked(
        self
    ):

        now = time.time()

        while (
            self.token_usage_window
            and
            now
            - self.token_usage_window[0][0]
            > self.window_secs
        ):

            _, old_tokens = (
                self.token_usage_window.popleft()
            )

            self.current_tokens = max(
                0,
                self.current_tokens
                - old_tokens
            )

    async def reserve_tokens(
        self,
        estimated_tokens:
            int = ESTIMATED_TOKENS_PER_CALL
    ):

        limit = int(
            self.token_limit
            * self.safety_buffer
        )

        while True:

            async with self.lock:

                await (
                    self._clean_old_tokens_locked()
                )

                if (
                    self.current_tokens
                    + estimated_tokens
                    <= limit
                ):

                    now = time.time()

                    self.token_usage_window.append(
                        (
                            now,
                            estimated_tokens
                        )
                    )

                    self.current_tokens += (
                        estimated_tokens
                    )

                    return

                current = (
                    self.current_tokens
                )

            # Don't print every 5 seconds.
            # This prevents notebook output explosion.

            await asyncio.sleep(5)

    async def adjust_tokens(
        self,
        estimated_tokens: int,
        actual_tokens: int
    ):

        async with self.lock:

            difference = (
                actual_tokens
                - estimated_tokens
            )

            self.current_tokens = max(
                0,
                self.current_tokens
                + difference
            )

            # Update the most recent reservation.
            #
            # This is approximate because multiple
            # requests can complete concurrently.
            #
            # The rolling limit is still protected
            # by the reservation mechanism.


# ================================================================
# RESULT BUILDER
# ================================================================

def build_result(

    row: pd.Series,

    analysis: Dict[str, Any],

    meta: Dict[str, Any],

    task_latency: float,

    error_override: str = None

) -> Dict[str, Any]:

    return {

        "ConversationId":
            row.get("ConversationId"),

        "ClaimNumber":
            row.get("ClaimNumber"),

        "ConversationStartTimestamp":
            row.get(
                "ConversationStartTimestamp"
            ),

        "MembershipNumber":
            row.get(
                "MembershipNumber"
            ),

        "CustomerMessage":
            analysis.get(
                "customer_message"
            ),

        "AgentMessage":
            analysis.get(
                "agent_message"
            ),

        "Confidence":
            analysis.get(
                "confidence"
            ),

        "prompt_tokens":
            meta.get(
                "prompt_tokens"
            ),

        "completion_tokens":
            meta.get(
                "completion_tokens"
            ),

        "total_tokens":
            meta.get(
                "total_tokens"
            ),

        "api_latency":
            meta.get(
                "api_latency"
            ),

        "task_latency":
            task_latency,

        "request_id":
            meta.get(
                "request_id"
            ),

        "error":
            (
                error_override
                if error_override is not None
                else meta.get("error")
            ),

        "processed_at":
            datetime.now()
    }


# ================================================================
# CHECKPOINT WRITER
# ================================================================

CHECKPOINT_COLUMNS = [

    "ConversationId",
    "ClaimNumber",
    "ConversationStartTimestamp",
    "MembershipNumber",
    "CustomerMessage",
    "AgentMessage",
    "Confidence",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "api_latency",
    "task_latency",
    "request_id",
    "error",
    "processed_at"
]


def save_checkpoint(
    results: List[Dict[str, Any]],
    checkpoint_table: str
):

    if not results:
        return

    checkpoint_df = pd.DataFrame(
        results
    )

    # Guarantee all columns exist
    for col in CHECKPOINT_COLUMNS:

        if col not in checkpoint_df.columns:

            checkpoint_df[col] = None

    checkpoint_df = checkpoint_df[
        CHECKPOINT_COLUMNS
    ]

    # Convert timestamp safely
    checkpoint_df[
        "ConversationStartTimestamp"
    ] = pd.to_datetime(
        checkpoint_df[
            "ConversationStartTimestamp"
        ],
        errors="coerce"
    )

    checkpoint_df[
        "processed_at"
    ] = pd.to_datetime(
        checkpoint_df[
            "processed_at"
        ],
        errors="coerce"
    )

    last_exception = None

    for attempt in range(
        1,
        CHECKPOINT_RETRIES + 1
    ):

        try:

            spark_df = spark.createDataFrame(
                checkpoint_df
            )

            # BIGINT
            for col_name in [
                "prompt_tokens",
                "completion_tokens",
                "total_tokens"
            ]:
                if col_name in spark_df.columns:
                    spark_df = spark_df.withColumn(
                        col_name,
                        F.col(col_name).cast("long")
                    )

            # DOUBLE
            for col_name in [
                "Confidence",
                "api_latency",
                "task_latency"
            ]:
                if col_name in spark_df.columns:
                    spark_df = spark_df.withColumn(
                        col_name,
                        F.col(col_name).cast("double")
                    )

            # STRING
            for col_name in [
                "ConversationId",
                "ClaimNumber",
                "MembershipNumber",
                "CustomerMessage",
                "AgentMessage",
                "request_id",
                "error"
            ]:
                if col_name in spark_df.columns:
                    spark_df = spark_df.withColumn(
                        col_name,
                        F.col(col_name).cast("string")
                    )

            # TIMESTAMP
            if "ConversationStartTimestamp" in spark_df.columns:
                spark_df = spark_df.withColumn(
                    "ConversationStartTimestamp",
                    F.to_timestamp(
                        F.col("ConversationStartTimestamp")
                    )
                )

            # processed_at, if your checkpoint dataframe contains it
            if "processed_at" in spark_df.columns:
                spark_df = spark_df.withColumn(
                    "processed_at",
                    F.to_timestamp(
                        F.col("processed_at")
                    )
                )

            # ============================================
            # DEBUG — VERY IMPORTANT
            # ============================================

            print("\n[CHECKPOINT] Schema being written:")
            spark_df.printSchema()

            spark_df.write \
                .mode("append") \
                .format("delta") \
                .option("mergeSchema", "false") \
                .saveAsTable(
                    checkpoint_table
                ) 
            
            print(f"[CHECKPOINT] Successfully Saved "
                  f"{len(checkpoint_df):,} rows"
                  )

            return

        except Exception as e:

            last_exception = e
            print(f"[CHECKPOINT] Attempt {attempt} Failed: "
                  f"{type(e).__name__}: {e}"
                  )

            if attempt < CHECKPOINT_RETRIES:

                time.sleep(
                    2 ** attempt
                )

            else:

                raise RuntimeError(
                    "Checkpoint write failed "
                    f"after {CHECKPOINT_RETRIES} attempts: "
                    f"{last_exception}"
                )


# ================================================================
# LOAD CHECKPOINT
# ================================================================

def load_processed_ids(
    checkpoint_table: str
):

    try:

        checkpoint_df = (
            spark.table(
                checkpoint_table
            )
            .select(
                "ConversationId",
                "error"
            )
            .toPandas()
        )

        if len(checkpoint_df) == 0:

            return set()

        # IMPORTANT:
        #
        # Only successful rows are treated as permanently
        # processed.
        #
        # Failed rows will be retried on a future run.

        successful_ids = set(
            checkpoint_df[
                checkpoint_df["error"].isna()
            ]["ConversationId"]
            .dropna()
            .astype(str)
            .unique()
        )

        return successful_ids

    except Exception as e:

        print(
            f"[CHECKPOINT] Could not load checkpoint: {e}"
        )

        return set()
    


# ================================================================
# MAIN ASYNC PROCESSOR
# ================================================================

async def process_telephony_batch_async(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None) -> pd.DataFrame:

    # ------------------------------------------------------------
    # Validate columns
    # ------------------------------------------------------------

    required_cols = [
        "ConversationId",
        "ConversationStartTimestamp",
        "ConversationTranscript",
        "ClaimNumber"
    ]

    missing = [
        c
        for c in required_cols
        if c not in df.columns
    ]

    if missing:

        raise ValueError(
            f"Missing required columns: {missing}"
        )

    # ------------------------------------------------------------
    # Prepare dataframe
    # ------------------------------------------------------------

    df = (
        df
        .reset_index(drop=True)
        .copy()
    )

    original_count = len(df)

    # ------------------------------------------------------------
    # Remove successfully checkpointed records
    # ------------------------------------------------------------

    if checkpoint_table:

        processed_ids = (
            load_processed_ids(
                checkpoint_table
            )
        )

        if processed_ids:

            before = len(df)

            df = df[
                ~df["ConversationId"]
                .astype(str)
                .isin(processed_ids)
            ].copy()

            print(
                f"[CHECKPOINT] Previously successful: "
                f"{before - len(df):,}"
            )

            print(
                f"[CHECKPOINT] Remaining: "
                f"{len(df):,}"
            )

    # ------------------------------------------------------------
    # Transcript length filter
    # ------------------------------------------------------------

    df = df[
        df["ConversationTranscript"]
        .fillna("")
        .astype(str)
        .str.len()
        >= MIN_TRANSCRIPT_CHARS
    ].copy()

    total_rows = len(df)

    print()
    print("=" * 80)
    print("TELEPHONY LLM BATCH")
    print("=" * 80)
    print(
        f"Input rows:              {original_count:,}"
    )
    print(
        f"Rows to process:         {total_rows:,}"
    )
    print(
        f"Concurrency:             {MAX_CONCURRENCY}"
    )
    print(
        f"Checkpoint interval:     {CHECKPOINT_EVERY}"
    )
    print(
        f"Token limit:             {TOKEN_LIMIT:,}"
    )
    print(
        f"Effective token limit:   "
        f"{int(TOKEN_LIMIT * SAFETY_BUFFER):,}"
    )
    print("=" * 80)
    print()

    if total_rows == 0:

        print(
            "[COMPLETE] Nothing to process."
        )

        return pd.DataFrame(
            columns=CHECKPOINT_COLUMNS
        )

    # ------------------------------------------------------------
    # Concurrency controls
    # ------------------------------------------------------------

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENCY
    )

    token_mgr = TokenBudgetManager(
        TOKEN_LIMIT,
        WINDOW_SECONDS,
        SAFETY_BUFFER
    )

    # ------------------------------------------------------------
    # Process single record
    # ------------------------------------------------------------

    async def process_single_row(
        row: pd.Series
    ):

        task_start = time.time()

        async with semaphore:

            try:

                # ------------------------------------------------
                # Build clean transcript
                # ------------------------------------------------

                clean_transcript = (
                    build_clean_transcript_for_llm(
                        row[
                            "ConversationTranscript"
                        ]
                    )
                )

                # ------------------------------------------------
                # Too short after PII redaction
                # ------------------------------------------------

                if len(clean_transcript) < MIN_TRANSCRIPT_CHARS:

                    return build_result(

                        row,

                        {
                            "customer_message":
                                None,

                            "agent_message":
                                None,

                            "confidence":
                                0.0
                        },

                        {
                            "prompt_tokens":
                                None,

                            "completion_tokens":
                                None,

                            "total_tokens":
                                None,

                            "api_latency":
                                None,

                            "request_id":
                                None,

                            "error":
                                "Transcript too short after redaction"
                        },

                        time.time()
                        - task_start
                    )

                # ------------------------------------------------
                # Build prompt
                # ------------------------------------------------

                agents_str = row.get(
                    "AgentsInvolved",
                    ""
                )

                prompt = (
                    build_telephony_prompt(
                        clean_transcript,
                        agents_str
                    )
                )

                # ------------------------------------------------
                # Reserve token capacity BEFORE request
                # ------------------------------------------------

                await token_mgr.reserve_tokens(
                    ESTIMATED_TOKENS_PER_CALL
                )

                # ------------------------------------------------
                # IMPORTANT:
                #
                # requests.post is blocking.
                #
                # Run it in a background thread.
                # ------------------------------------------------

                analysis, meta = await asyncio.to_thread(

                    call_telephony_model,

                    prompt,

                    MAX_TOKENS,

                    RETRIES,

                    token,

                    modelgateway_baseurl,

                    API_TIMEOUT
                )

                # ------------------------------------------------
                # Adjust token accounting
                # ------------------------------------------------

                actual_tokens = (
                    meta.get("total_tokens")
                )

                if actual_tokens:

                    await token_mgr.adjust_tokens(

                        ESTIMATED_TOKENS_PER_CALL,

                        int(actual_tokens)
                    )

                # ------------------------------------------------
                # Build result
                # ------------------------------------------------

                return build_result(

                    row,

                    analysis,

                    meta,

                    time.time()
                    - task_start
                )

            except Exception as e:

                return build_result(

                    row,

                    {
                        "customer_message":
                            None,

                        "agent_message":
                            None,

                        "confidence":
                            0.0
                    },

                    {
                        "prompt_tokens":
                            None,

                        "completion_tokens":
                            None,

                        "total_tokens":
                            None,

                        "api_latency":
                            None,

                        "request_id":
                            None,

                        "error":
                            None
                    },

                    time.time()
                    - task_start,

                    error_override=
                        f"{type(e).__name__}: {str(e)}"
                )

    # ------------------------------------------------------------
    # Create tasks
    # ------------------------------------------------------------

    tasks = [

        asyncio.create_task(
            process_single_row(row)
        )

        for _, row in df.iterrows()
    ]

    # ------------------------------------------------------------
    # Progress tracking
    # ------------------------------------------------------------

    all_results = []

    checkpoint_buffer = []

    successful = 0
    failed = 0

    start_time = time.time()

    # ------------------------------------------------------------
    # tqdm progress bar
    # ------------------------------------------------------------

    progress = tqdm(

        total=total_rows,

        desc="Telephony LLM",

        unit="rows",

        dynamic_ncols=True,

        mininterval=2,

        smoothing=0.1
    )

    # ------------------------------------------------------------
    # Process tasks as they finish
    # ------------------------------------------------------------

    try:

        for completed_task in asyncio.as_completed(
            tasks
        ):

            result = await completed_task

            all_results.append(
                result
            )

            checkpoint_buffer.append(
                result
            )

            # ----------------------------------------------------
            # Success / failure
            # ----------------------------------------------------

            if result.get("error"):

                failed += 1

            else:

                successful += 1

            # ----------------------------------------------------
            # Update progress bar
            # ----------------------------------------------------

            processed = (
                successful
                + failed
            )

            elapsed = (
                time.time()
                - start_time
            )

            rate = (
                processed / elapsed * 60
                if elapsed > 0
                else 0
            )

            remaining = (
                total_rows
                - processed
            )

            eta_seconds = (
                remaining / (processed / elapsed)
                if processed > 0
                else 0
            )

            progress.update(1)

            progress.set_postfix(

                {

                    "ok":
                        f"{successful:,}",

                    "failed":
                        f"{failed:,}",

                    "rate":
                        f"{rate:.1f}/min",

                    "ETA":
                        format_eta(
                            eta_seconds
                        )
                },

                refresh=False
            )

            # ----------------------------------------------------
            # Incremental checkpoint
            # ----------------------------------------------------

            if (
                len(checkpoint_buffer)
                >= CHECKPOINT_EVERY
            ):

                if checkpoint_table:

                    save_checkpoint(

                        checkpoint_buffer,

                        checkpoint_table
                    )

                checkpoint_buffer = []

    except asyncio.CancelledError:

        # Cancel outstanding tasks
        for task in tasks:

            if not task.done():

                task.cancel()

        raise

    finally:

        progress.close()

    # ------------------------------------------------------------
    # Save remaining checkpoint records
    # ------------------------------------------------------------

    if checkpoint_buffer:

        if checkpoint_table:

            save_checkpoint(

                checkpoint_buffer,

                checkpoint_table
            )

    # ------------------------------------------------------------
    # Final statistics
    # ------------------------------------------------------------

    elapsed = (
        time.time()
        - start_time
    )

    rate = (
        len(all_results)
        / elapsed
        * 60
        if elapsed > 0
        else 0
    )

    print()
    print("=" * 80)
    print("BATCH COMPLETE")
    print("=" * 80)
    print(
        f"Processed:       {len(all_results):,}"
    )
    print(
        f"Successful:      {successful:,}"
    )
    print(
        f"Failed:          {failed:,}"
    )
    print(
        f"Elapsed:         {format_duration(elapsed)}"
    )
    print(
        f"Rate:            {rate:.1f} records/min"
    )
    print("=" * 80)

    return pd.DataFrame(
        all_results
    )


# ================================================================
# HELPER: ETA
# ================================================================

def format_eta(
    seconds: float
) -> str:

    if not seconds or seconds <= 0:

        return "--"

    seconds = int(seconds)

    days, remainder = divmod(
        seconds,
        86400
    )

    hours, remainder = divmod(
        remainder,
        3600
    )

    minutes, secs = divmod(
        remainder,
        60
    )

    if days > 0:

        return (
            f"{days}d "
            f"{hours}h"
        )

    if hours > 0:

        return (
            f"{hours}h "
            f"{minutes}m"
        )

    if minutes > 0:

        return (
            f"{minutes}m "
            f"{secs}s"
        )

    return f"{secs}s"


def format_duration(
    seconds: float
) -> str:

    return format_eta(
        seconds
    )


# ================================================================
# ASYNC DATE-CHUNK RUNNER
# ================================================================

async def run_telephony_llm_batch_async(

    df: pd.DataFrame,

    token: str,

    modelgateway_baseurl: str,

    checkpoint_table: str = None,

    output_table: str = None,

    date_chunks: List[
        Tuple[str, str]
    ] = None

) -> pd.DataFrame:

    all_results = []

    # ============================================================
    # Date chunk processing
    # ============================================================

    if date_chunks:

        for start_date, end_date in date_chunks:

            print()
            print("=" * 80)
            print(
                f"DATE CHUNK: "
                f"{start_date} -> {end_date}"
            )
            print("=" * 80)

            # ----------------------------------------------------
            # IMPORTANT:
            #
            # Use >= start
            # and < day AFTER end.
            #
            # This prevents losing records from the end date
            # when ConversationStartTimestamp contains time.
            # ----------------------------------------------------

            start_ts = pd.Timestamp(
                start_date
            )

            end_ts = (
                pd.Timestamp(end_date)
                + pd.Timedelta(days=1)
            )

            timestamps = pd.to_datetime(
                df[
                    "ConversationStartTimestamp"
                ],
                errors="coerce"
            )

            df_chunk = df[
                (timestamps >= start_ts)
                &
                (timestamps < end_ts)
            ].copy()

            print(
                f"[CHUNK] Input rows: "
                f"{len(df_chunk):,}"
            )

            if len(df_chunk) == 0:

                print(
                    "[CHUNK] No records."
                )

                continue

            chunk_results = (
                await process_telephony_batch_async(

                    df_chunk,

                    token,

                    modelgateway_baseurl,

                    checkpoint_table,

                    output_table
                )
            )

            all_results.append(
                chunk_results
            )

    else:

        results = (
            await process_telephony_batch_async(

                df,

                token,

                modelgateway_baseurl,

                checkpoint_table,

                output_table
            )
        )

        all_results.append(
            results
        )

    # ============================================================
    # Combine
    # ============================================================

    if all_results:

        return pd.concat(
            all_results,
            ignore_index=True
        )

    return pd.DataFrame(
        columns=CHECKPOINT_COLUMNS
    )


# ================================================================
# NOTEBOOK-SAFE SYNC WRAPPER
# ================================================================
#
# This avoids:
#
# RuntimeError:
# Cannot run the event loop while another loop is running
#
# Databricks notebooks can already have an active event loop.
#
# We therefore execute asyncio.run() in a separate thread
# when necessary.
# ================================================================

def run_telephony_llm_batch(

    df: pd.DataFrame,

    token: str,

    modelgateway_baseurl: str,

    checkpoint_table: str = None,

    output_table: str = None,

    date_chunks: List[
        Tuple[str, str]
    ] = None

) -> pd.DataFrame:

    async def runner():

        return await (
            run_telephony_llm_batch_async(

                df,

                token,

                modelgateway_baseurl,

                checkpoint_table,

                output_table,

                date_chunks
            )
        )

    # ------------------------------------------------------------
    # Detect whether an event loop is already running
    # ------------------------------------------------------------

    try:

        asyncio.get_running_loop()

        loop_is_running = True

    except RuntimeError:

        loop_is_running = False

    # ------------------------------------------------------------
    # Normal Python environment
    # ------------------------------------------------------------

    if not loop_is_running:

        return asyncio.run(
            runner()
        )

    # ------------------------------------------------------------
    # Databricks / Jupyter environment
    #
    # Run asyncio in another thread.
    # ------------------------------------------------------------

    result_container = []

    exception_container = []

    def thread_target():

        try:

            result = asyncio.run(
                runner()
            )

            result_container.append(
                result
            )

        except Exception as e:

            exception_container.append(
                e
            )

    thread = threading.Thread(
        target=thread_target
    )

    thread.start()

    thread.join()

    if exception_container:

        raise exception_container[0]

    if result_container:

        return result_container[0]

    return pd.DataFrame(
        columns=CHECKPOINT_COLUMNS
    )
