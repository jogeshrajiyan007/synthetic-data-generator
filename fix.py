# ================================================================
# TELEPHONY LLM BATCH PROCESSOR - PRODUCTION VERSION (FIXED)
# ================================================================
#
# Changes vs previous version:
#   - Explicit 429 / 401 / 403 detection (previously swallowed into
#     a generic Exception with no special handling)
#   - GLOBAL coordinated backoff: when ANY worker hits a 429, ALL
#     concurrent workers pause together and resume together.
#     (Previously each of the 15 concurrent workers retried
#     independently, so 14 kept hammering the API while 1 backed
#     off -> immediate re-throttle -> the storm you saw.)
#   - Retry-After header is now read and honored
#   - Added a requests-per-minute limiter alongside the existing
#     token-per-window limiter (429s are usually RPM, not just TPM)
#   - Rows that hit rate limits are retried after the pause instead
#     of being recorded as permanently failed for the run
#   - Optional token_refresh_callback for 401/403 (token expiry on
#     long-running jobs)
#   - Fixed a bug in save_checkpoint(): the double-column dtype
#     loop was iterating over bigint_columns instead of
#     double_columns, so Confidence/api_latency/task_latency were
#     never cleaned and prompt/completion/total_tokens were
#     overwritten back to float right after being cast to int.
#
# ================================================================

import asyncio
import json
import time
import re
import math
import base64
import traceback
import threading

import pandas as pd
import numpy as np

from collections import deque
from typing import Dict, Any, Tuple, List, Optional, Callable
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)

import requests
from tqdm.auto import tqdm


# ================================================================
# CONFIGURATION
# ================================================================

MODEL = "gpt-4o-2024-11-20"

# Number of simultaneous LLM requests.
# If you keep seeing sustained 429s even with the coordinated
# backoff below, lower this first (try 8-10).
MAX_CONCURRENCY = 15

# Minimum transcript length before LLM processing
MIN_TRANSCRIPT_CHARS = 100

# Maximum completion tokens
MAX_TOKENS = 1200

# API retries for NON-rate-limit errors (timeouts, 5xx, etc).
# 429/401/403 are handled separately below - they are not
# retried locally, they bubble up so the async layer can
# coordinate a shared pause / token refresh.
RETRIES = 3

# API timeout
API_TIMEOUT = 90

# How many times a single row will re-attempt after being rate
# limited before it's finally recorded as failed (and retried on
# the NEXT run via checkpoint resume).
MAX_ROW_RATE_LIMIT_RETRIES = 6

# If the gateway doesn't send Retry-After, back off this many
# seconds as a baseline before scaling up.
RATE_LIMIT_BASE_WAIT = 10

# Hard cap on any single coordinated pause, however bad it gets.
RATE_LIMIT_MAX_WAIT = 120

# How many times a single row will retry after an auth failure
# (401/403) before giving up. Protects against a refresh_callback
# that returns a token that's still invalid (e.g. bad credentials) -
# without this, that would loop forever.
MAX_ROW_AUTH_RETRIES = 3

# Proactively refresh the token this many seconds before it's due
# to expire, instead of waiting for a 401/403 to happen first.
# Only takes effect if the token is a decodable JWT with an `exp`
# claim (Azure AD tokens are) - otherwise this is a no-op and
# refresh stays purely reactive (on 401/403).
TOKEN_EXPIRY_BUFFER_SECONDS = 120

# ================================================================
# CHECKPOINT CONFIGURATION
# ================================================================

CHECKPOINT_EVERY = 250
CHECKPOINT_RETRIES = 3


# ================================================================
# TOKEN RATE LIMITING (existing - limits total token VOLUME)
# ================================================================

TOKEN_LIMIT = 15_000_000
WINDOW_SECONDS = 60 * 30
SAFETY_BUFFER = 0.90
ESTIMATED_TOKENS_PER_CALL = 1800


# ================================================================
# REQUEST RATE LIMITING (NEW - limits requests PER MINUTE)
# ================================================================
#
# A 429 is very often an RPM limit, not a TPM limit. The token
# budget manager above does nothing to protect against 15
# concurrent requests firing in the same second. This does.
#
# Tune MAX_REQUESTS_PER_MINUTE to whatever your model gateway
# actually allows - check with your platform team if unsure.
# ================================================================

MAX_REQUESTS_PER_MINUTE = 60


# ================================================================
# PII REDACTION (unchanged)
# ================================================================

PII_PATTERNS = [
    re.compile(r"\b\d{3}\s?\d{3}\s?\d{4}\b"),
    re.compile(r"\b\d{10,}\b"),
    re.compile(
        r"\b(?:\+?1?[-.\s]?\(?)?"
        r"(\d{3})\)?[-.\s]?"
        r"(\d{3})[-.\s]?"
        r"(\d{4})\b"
    ),
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}"),
    re.compile(r"\b[A-Z]{2}\d{6}\b"),
    re.compile(
        r"(?:DOB|dob|date of birth)[:\s]*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}",
        re.IGNORECASE
    ),
    re.compile(
        r"(?:postcode|postal code|zip)[:\s]*[A-Z0-9]{2,4}\s?[A-Z0-9]{1,3}",
        re.IGNORECASE
    ),
    re.compile(
        r"(?:sort code|account)[:\s]*\d{2}[-\s]?\d{2}[-\s]?\d{2}",
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
# TRANSCRIPT PARSING (unchanged)
# ================================================================

def parse_transcript_into_turns(transcript: str) -> List[Dict[str, str]]:
    if not transcript:
        return []

    turns = []
    lines = transcript.split("\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("Agent:"):
            turns.append({"speaker": "agent", "text": line[6:].strip()})

        elif line.startswith("Customer:"):
            turns.append({"speaker": "customer", "text": line[9:].strip()})

        elif ":" in line:
            parts = line.split(":", 1)
            if len(parts) != 2:
                continue

            speaker_part = parts[0].strip()
            message = parts[1].strip()
            speaker_lower = speaker_part.lower()

            if any(x in speaker_lower for x in
                   ["agent", "support", "representative", "tech", "customer service"]):
                turns.append({"speaker": "agent", "text": message})

            elif any(x in speaker_lower for x in
                     ["customer", "caller", "client", "member"]):
                turns.append({"speaker": "customer", "text": message})

    return turns


def build_clean_transcript_for_llm(transcript: str) -> str:
    turns = parse_transcript_into_turns(transcript)
    if not turns:
        return ""

    clean_lines = []
    for turn in turns:
        text = redact_pii(turn["text"])
        speaker_label = "Agent" if turn["speaker"] == "agent" else "Customer"
        clean_lines.append(f"{speaker_label}: {text}")

    return "\n".join(clean_lines)


# ================================================================
# LLM JSON SCHEMA (unchanged)
# ================================================================

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
                "description": "Summary of what the agent explained, did, or outcome communicated"
            },
            "confidence": {
                "type": "number",
                "description": "Confidence score between 0 and 1"
            }
        },
        "required": ["customer_message", "agent_message", "confidence"],
        "additionalProperties": False
    }
}


# ================================================================
# PROMPT (unchanged)
# ================================================================

def build_telephony_prompt(clean_transcript: str, agents_involved: str = None) -> str:
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
# JSON PARSER (unchanged)
# ================================================================

def custom_json_loader(output_text: str) -> Dict[str, Any]:
    try:
        return json.loads(output_text)
    except json.JSONDecodeError:
        start = output_text.find("{")
        end = output_text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(output_text[start:end + 1])
        raise


# ================================================================
# RESPONSE VALIDATION (unchanged)
# ================================================================

def validate_and_repair_telephony_response(response: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(response, dict):
        response = {}

    customer_msg = str(response.get("customer_message", "")).strip() or None
    agent_msg = str(response.get("agent_message", "")).strip() or None

    confidence = response.get("confidence")
    try:
        confidence = float(confidence or 0)
        confidence = max(0.0, min(1.0, confidence))
    except (ValueError, TypeError):
        confidence = 0.5

    if not customer_msg and not agent_msg:
        confidence = 0.0

    return {
        "customer_message": customer_msg,
        "agent_message": agent_msg,
        "confidence": confidence
    }


# ================================================================
# NEW: SPECIFIC EXCEPTIONS FOR RATE LIMITING / AUTH
# ================================================================
#
# The old code caught EVERY failure (429, 401, timeouts, JSON
# errors, ...) as the same generic Exception, retried a couple
# of times with a tiny backoff, then gave up. That's why 429s
# were never really "handled" - they were just retried too fast
# and too locally to matter.
# ================================================================

class RateLimitError(Exception):
    """Raised on HTTP 429. Carries Retry-After if the server sent one."""

    def __init__(self, retry_after: Optional[float] = None, status_code: int = 429):
        self.retry_after = retry_after
        self.status_code = status_code
        super().__init__(f"Rate limited (HTTP {status_code}), retry_after={retry_after}")


class AuthError(Exception):
    """Raised on HTTP 401/403. Retrying the same token won't help - needs refresh."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"Auth error (HTTP {status_code})")


# ================================================================
# NEW: JWT EXPIRY DECODING (for proactive refresh)
# ================================================================
#
# Azure AD access tokens are JWTs with an `exp` claim (unix
# timestamp). We decode just the payload segment - no signature
# verification needed, we're not authenticating anything, just
# reading our own token's expiry so we can refresh BEFORE it
# lapses instead of waiting for a 401.
#
# If the token isn't a 3-part JWT (some gateways issue opaque
# tokens), this returns None and proactive refresh is skipped -
# refresh then stays purely reactive (on 401/403), which still
# works fine.
# ================================================================

def decode_jwt_exp(token: str) -> Optional[float]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None

        payload_b64 = parts[1]
        padding = "=" * (-len(payload_b64) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(payload_bytes)

        return float(payload["exp"]) if "exp" in payload else None

    except Exception:
        return None


# ================================================================
# NEW: MUTABLE TOKEN HOLDER (FIXED)
# ================================================================
#
# Lets all concurrent workers see a refreshed token without each
# holding a stale local copy of the string, and coordinates so
# that when N workers discover the token is bad/expiring at once,
# exactly ONE of them calls the refresh endpoint and the rest wait
# for it - instead of N redundant calls.
#
# IMPORTANT FIX vs the first version of this class: the actual
# network call to refresh_callback() used to happen INSIDE
# `async with self._lock`. That meant a second worker blocking on
# the lock would only get it back AFTER the first refresh had
# already finished and reset the flag - so it never actually saw
# "someone else is refreshing" and would immediately trigger a
# second, redundant refresh call. The lock here now only guards
# the flag check/set; the network call itself happens outside it.
# ================================================================

class TokenHolder:
    def __init__(self, token: str):
        self.token = token
        self._lock = asyncio.Lock()
        self._refreshing = False
        self._refreshed_event = asyncio.Event()
        self._refreshed_event.set()

    def seconds_until_expiry(self) -> Optional[float]:
        exp = decode_jwt_exp(self.token)
        if exp is None:
            return None
        return exp - time.time()

    async def ensure_fresh(
        self,
        refresh_callback: Optional[Callable[[], str]],
        buffer_seconds: float = TOKEN_EXPIRY_BUFFER_SECONDS
    ):
        """Proactive refresh - call before each request. Cheap no-op
        unless the token is within buffer_seconds of expiring."""

        if refresh_callback is None:
            return

        remaining = self.seconds_until_expiry()
        if remaining is not None and remaining <= buffer_seconds:
            await self.refresh(refresh_callback)

    async def refresh(self, refresh_callback: Optional[Callable[[], str]]):
        """Reactive refresh - call after a 401/403. Coordinates so
        only one concurrent worker actually hits the token
        endpoint; the rest wait on the event."""

        if refresh_callback is None:
            raise AuthError(status_code=401)

        should_refresh = False

        async with self._lock:
            if not self._refreshing:
                self._refreshing = True
                self._refreshed_event.clear()
                should_refresh = True

        if should_refresh:
            try:
                print("\n[AUTH] Token expired/invalid or expiring soon -> refreshing...")

                if asyncio.iscoroutinefunction(refresh_callback):
                    new_token = await refresh_callback()
                else:
                    # requests.post inside the callback is blocking -
                    # run it off the event loop like every other
                    # network call in this module.
                    new_token = await asyncio.to_thread(refresh_callback)

                if not new_token or not isinstance(new_token, str):
                    raise AuthError(status_code=401)

                self.token = new_token
                print("[AUTH] Token refreshed successfully.")

            except AuthError:
                raise
            except Exception as e:
                raise AuthError(status_code=401) from e
            finally:
                async with self._lock:
                    self._refreshing = False
                    self._refreshed_event.set()

        # Whether we refreshed it ourselves or another worker did -
        # wait until whichever refresh is in flight completes.
        await self._refreshed_event.wait()


# ================================================================
# NEW: AUTOMATIC OAUTH2 CLIENT-CREDENTIALS TOKEN FETCH
# ================================================================
#
# Concrete implementation for automatic refresh. If you pass
# tenant_id / client_id / client_secret / scopes into
# run_telephony_llm_batch(), this is wired up automatically as the
# refresh mechanism - no callback needs to be hand-built.
#
# If your token comes from somewhere else (a different IdP, a
# secrets-manager-backed helper, etc), just pass your own
# token_refresh_callback instead and this function is unused.
# ================================================================

def get_oauth_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scopes: str,
    timeout: int = 30
) -> str:
    """
    Client-credentials OAuth2 flow against Azure AD.
    scopes: space-separated scope string, e.g.
            "https://your-model-gateway/.default"
    """

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scopes
    }

    response = requests.post(token_url, data=payload, timeout=timeout)
    response.raise_for_status()

    data = response.json()

    if "access_token" not in data:
        raise AuthError(status_code=401)

    return data["access_token"]


def build_oauth_refresh_callback(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scopes: str
) -> Callable[[], str]:
    """Wraps get_oauth_token into a zero-arg callable for TokenHolder."""

    def _refresh() -> str:
        return get_oauth_token(tenant_id, client_id, client_secret, scopes)

    return _refresh


# ================================================================
# NEW: GLOBAL COORDINATED RATE-LIMIT BACKOFF
# ================================================================
#
# This is the core fix for the 429 storm.
#
# Without this, when worker A gets a 429, only worker A backs off.
# Workers B through O (14 of them) keep firing immediately,
# re-trip the limit, and the whole batch thrashes into a wall of
# 429s - exactly what the screenshot shows.
#
# With this, the FIRST worker to see a 429 pauses EVERY worker
# (via an asyncio.Event), waits out the backoff once, then
# releases everyone together. Consecutive hits increase the wait,
# and it decays back down on sustained success.
# ================================================================

class GlobalRateLimitCoordinator:

    def __init__(self):
        self._resume_event = asyncio.Event()
        self._resume_event.set()  # not paused initially
        self._lock = asyncio.Lock()
        self._consecutive_rate_limits = 0

    async def wait_if_paused(self):
        await self._resume_event.wait()

    async def trigger_pause(self, retry_after: Optional[float] = None):
        wait_time = 0
        should_sleep = False

        async with self._lock:
            already_paused = not self._resume_event.is_set()

            if not already_paused:
                self._consecutive_rate_limits += 1
                self._resume_event.clear()

                base_wait = retry_after if retry_after else RATE_LIMIT_BASE_WAIT
                backoff_multiplier = min(2 ** (self._consecutive_rate_limits - 1), 8)
                wait_time = min(base_wait * backoff_multiplier, RATE_LIMIT_MAX_WAIT)
                should_sleep = True

                print(
                    f"\n[RATE LIMIT] HTTP 429 received. Pausing ALL workers for "
                    f"{wait_time:.0f}s (consecutive hits: {self._consecutive_rate_limits})"
                )

        if should_sleep:
            await asyncio.sleep(wait_time)
            async with self._lock:
                self._resume_event.set()
            print("[RATE LIMIT] Resuming all workers.")

        await self._resume_event.wait()

    def note_success(self):
        if self._consecutive_rate_limits > 0:
            self._consecutive_rate_limits = max(0, self._consecutive_rate_limits - 1)


# ================================================================
# NEW: REQUESTS-PER-MINUTE LIMITER
# ================================================================
#
# The existing TokenBudgetManager only guards total token VOLUME
# over a 30-min window. It does nothing to stop 15 requests firing
# in the same second, which is usually what actually trips a 429
# on an APIM-style gateway. This adds a simple sliding-window RPM
# guard alongside it.
# ================================================================

class RequestRateLimiter:

    def __init__(self, max_requests_per_minute: int):
        self.max_requests_per_minute = max_requests_per_minute
        self.window = deque()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                while self.window and now - self.window[0] > 60:
                    self.window.popleft()

                if len(self.window) < self.max_requests_per_minute:
                    self.window.append(now)
                    return

            await asyncio.sleep(0.5)


# ================================================================
# SYNCHRONOUS MODEL CALL (FIXED)
# ================================================================
#
# requests.post is blocking - the async processor calls this via
# asyncio.to_thread(...).
#
# CHANGED: 429 and 401/403 are now detected explicitly by status
# code and raised as RateLimitError / AuthError instead of being
# retried locally and eventually flattened into a generic string.
# They are NOT retried inside this function - they bubble up so
# the async layer can coordinate a shared pause / token refresh
# across all concurrent workers.
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
        raise ValueError("modelgateway_baseurl is required")

    base_url = modelgateway_baseurl.rstrip("/")

    apiurl = (
        f"{base_url}/secure-gpt-openai/openai/deployments/"
        f"{MODEL}/chat/completions?api-version=2024-06-01"
    )

    payload = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an expert AXA Health customer service analyst who "
                    "separates call transcripts into customer and agent contributions."
                )
            },
            {"role": "user", "content": prompt}
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

    for attempt in range(1, retries + 1):
        start_api = time.time()

        try:
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            response = requests.post(
                apiurl, headers=headers, json=payload, verify=True, timeout=api_timeout
            )

            # ------------------------------------------------------
            # NEW: explicit status-code handling - this was missing
            # entirely before. Everything used to fall through to
            # response.raise_for_status() -> generic HTTPError ->
            # generic retry loop -> generic failure string.
            # ------------------------------------------------------

            if response.status_code == 429:
                retry_after_header = response.headers.get("Retry-After")
                try:
                    retry_after = float(retry_after_header) if retry_after_header else None
                except (TypeError, ValueError):
                    retry_after = None
                # Do not retry locally - bubble up for coordinated backoff.
                raise RateLimitError(retry_after=retry_after, status_code=429)

            if response.status_code in (401, 403):
                # Do not retry locally - bubble up for token refresh.
                raise AuthError(status_code=response.status_code)

            response.raise_for_status()

            api_latency = time.time() - start_api
            request_id = response.headers.get("x-request-id")
            data = response.json()
            choice = data["choices"][0]["message"]

            if "parsed" in choice:
                parsed = choice["parsed"]
            elif choice.get("content"):
                parsed = custom_json_loader(choice["content"])
            else:
                raise ValueError("No model content returned")

            cleaned = validate_and_repair_telephony_response(parsed)
            usage = data.get("usage", {})

            metadata = {
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "api_latency": api_latency,
                "request_id": request_id,
                "attempt": attempt,
                "error": None
            }

            return cleaned, metadata

        except (RateLimitError, AuthError):
            # These are handled one level up (coordinated pause /
            # token refresh). Retrying them here, locally, with a
            # 2-4 second backoff while 14 other workers keep firing
            # is exactly what caused the storm - so don't.
            raise

        except Exception as e:
            last_exception = e

            if attempt < retries:
                wait_time = (2 ** (attempt - 1)) + np.random.rand()
                time.sleep(wait_time)
            else:
                return (
                    {"customer_message": None, "agent_message": None, "confidence": 0.0},
                    {
                        "prompt_tokens": None,
                        "completion_tokens": None,
                        "total_tokens": None,
                        "api_latency": time.time() - start_api,
                        "request_id": None,
                        "attempt": attempt,
                        "error": f"{type(e).__name__}: {str(e)}"
                    }
                )

    raise RuntimeError(f"Model call failed: {last_exception}")


# ================================================================
# TOKEN BUDGET MANAGER (unchanged - still guards total TPM volume)
# ================================================================

class TokenBudgetManager:

    def __init__(self, token_limit: int, window_secs: int, safety_buffer: float = 0.90):
        self.token_limit = token_limit
        self.window_secs = window_secs
        self.safety_buffer = safety_buffer
        self.token_usage_window = deque()
        self.current_tokens = 0
        self.lock = asyncio.Lock()

    async def _clean_old_tokens_locked(self):
        now = time.time()
        while self.token_usage_window and now - self.token_usage_window[0][0] > self.window_secs:
            _, old_tokens = self.token_usage_window.popleft()
            self.current_tokens = max(0, self.current_tokens - old_tokens)

    async def reserve_tokens(self, estimated_tokens: int = ESTIMATED_TOKENS_PER_CALL):
        limit = int(self.token_limit * self.safety_buffer)

        while True:
            async with self.lock:
                await self._clean_old_tokens_locked()

                if self.current_tokens + estimated_tokens <= limit:
                    now = time.time()
                    self.token_usage_window.append((now, estimated_tokens))
                    self.current_tokens += estimated_tokens
                    return

            await asyncio.sleep(5)

    async def adjust_tokens(self, estimated_tokens: int, actual_tokens: int):
        async with self.lock:
            difference = actual_tokens - estimated_tokens
            self.current_tokens = max(0, self.current_tokens + difference)


# ================================================================
# RESULT BUILDER (unchanged)
# ================================================================

def build_result(
    row: pd.Series,
    analysis: Dict[str, Any],
    meta: Dict[str, Any],
    task_latency: float,
    error_override: str = None
) -> Dict[str, Any]:

    return {
        "ConversationId": row.get("ConversationId"),
        "ClaimNumber": row.get("ClaimNumber"),
        "ConversationStartTimestamp": row.get("ConversationStartTimestamp"),
        "MembershipNumber": row.get("MembershipNumber"),
        "CustomerMessage": analysis.get("customer_message"),
        "AgentMessage": analysis.get("agent_message"),
        "Confidence": analysis.get("confidence"),
        "prompt_tokens": meta.get("prompt_tokens"),
        "completion_tokens": meta.get("completion_tokens"),
        "total_tokens": meta.get("total_tokens"),
        "api_latency": meta.get("api_latency"),
        "task_latency": task_latency,
        "request_id": meta.get("request_id"),
        "error": error_override if error_override is not None else meta.get("error"),
        "processed_at": datetime.now()
    }


# ================================================================
# CHECKPOINT WRITER (dtype bug fixed)
# ================================================================

CHECKPOINT_COLUMNS = [
    "ConversationId", "ClaimNumber", "ConversationStartTimestamp",
    "MembershipNumber", "CustomerMessage", "AgentMessage", "Confidence",
    "prompt_tokens", "completion_tokens", "total_tokens",
    "api_latency", "task_latency", "request_id", "error", "processed_at"
]


def save_checkpoint(results: List[Dict[str, Any]], checkpoint_table: str):

    if not results:
        return

    checkpoint_df = pd.DataFrame(results)

    for col in CHECKPOINT_COLUMNS:
        if col not in checkpoint_df.columns:
            checkpoint_df[col] = None

    checkpoint_df = checkpoint_df[CHECKPOINT_COLUMNS].copy()

    # ------------------------------------------------------------
    # STRING COLUMNS
    # ------------------------------------------------------------

    string_columns = [
        "ConversationId", "ClaimNumber", "MembershipNumber",
        "CustomerMessage", "AgentMessage", "request_id", "error"
    ]

    for col in string_columns:
        checkpoint_df[col] = checkpoint_df[col].where(checkpoint_df[col].notna(), None)
        checkpoint_df[col] = checkpoint_df[col].apply(lambda x: str(x) if x is not None else None)

    # ------------------------------------------------------------
    # BIGINT COLUMNS
    # ------------------------------------------------------------

    bigint_columns = ["prompt_tokens", "completion_tokens", "total_tokens"]

    def to_python_int(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return int(value)

    for col in bigint_columns:
        checkpoint_df[col] = (
            pd.to_numeric(checkpoint_df[col], errors="coerce")
            .map(to_python_int)
            .astype(object)
        )

    # ------------------------------------------------------------
    # DOUBLE COLUMNS
    #
    # FIXED: this loop previously iterated over `bigint_columns`
    # again (copy-paste error), which meant:
    #   1. Confidence / api_latency / task_latency were NEVER
    #      cleaned of NaN/type issues here.
    #   2. prompt_tokens / completion_tokens / total_tokens got
    #      cast to int above, then immediately overwritten back
    #      to float by this loop running on the same columns -
    #      right before being written against an explicit
    #      LongType() schema below.
    # ------------------------------------------------------------

    double_columns = ["Confidence", "api_latency", "task_latency"]

    def to_python_float(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return float(value)

    for col in double_columns:
        checkpoint_df[col] = (
            pd.to_numeric(checkpoint_df[col], errors="coerce")
            .map(to_python_float)
            .astype(object)
        )

    # ------------------------------------------------------------
    # TIMESTAMP COLUMNS
    # ------------------------------------------------------------

    timestamp_columns = ["ConversationStartTimestamp", "processed_at"]

    def to_naive_python_datetime(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            if value.tzinfo is not None:
                value = value.tz_localize(None)
            return value.to_pydatetime().replace(tzinfo=None)

        if isinstance(value, datetime):
            return value.replace(tzinfo=None)

        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        if getattr(parsed, "tzinfo", None) is not None:
            parsed = parsed.replace(tzinfo=None)

        return parsed.to_pydatetime().replace(tzinfo=None)

    for col in timestamp_columns:
        checkpoint_df[col] = checkpoint_df[col].map(to_naive_python_datetime)

    # ------------------------------------------------------------
    # EXPLICIT SPARK SCHEMA
    # ------------------------------------------------------------

    checkpoint_schema = StructType([
        StructField("ConversationId", StringType(), True),
        StructField("ClaimNumber", StringType(), True),
        StructField("ConversationStartTimestamp", StringType(), True),
        StructField("MembershipNumber", StringType(), True),
        StructField("CustomerMessage", StringType(), True),
        StructField("AgentMessage", StringType(), True),
        StructField("Confidence", DoubleType(), True),
        StructField("prompt_tokens", LongType(), True),
        StructField("completion_tokens", LongType(), True),
        StructField("total_tokens", LongType(), True),
        StructField("api_latency", DoubleType(), True),
        StructField("task_latency", DoubleType(), True),
        StructField("request_id", StringType(), True),
        StructField("error", StringType(), True),
        StructField("processed_at", StringType(), True)
    ])

    def spark_safe(value):
        if value is None:
            return None
        if isinstance(value, (float, np.floating)) and (math.isnan(value) or math.isinf(value)):
            return None
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        return value

    records = checkpoint_df.to_dict(orient="records")
    records = [{k: spark_safe(v) for k, v in row.items()} for row in records]

    spark_df = spark.createDataFrame(records, schema=checkpoint_schema)

    spark_df = (
        spark_df
        .withColumn("ConversationStartTimestamp", F.to_timestamp("ConversationStartTimestamp"))
        .withColumn("processed_at", F.to_timestamp("processed_at"))
    )

    print("\n[CHECKPOINT] Schema being written:")
    spark_df.printSchema()
    print("[CHECKPOINT] Rows:", spark_df.count())

    last_exception = None

    for attempt in range(1, CHECKPOINT_RETRIES + 1):
        try:
            (
                spark_df.write
                .mode("append")
                .format("delta")
                .option("mergeSchema", "false")
                .saveAsTable(checkpoint_table)
            )
            print(f"[CHECKPOINT] Successfully saved {len(records):,} rows")
            return

        except Exception as e:
            last_exception = e
            print(f"[CHECKPOINT] Attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt < CHECKPOINT_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(
        f"Checkpoint write failed after {CHECKPOINT_RETRIES} attempts: {last_exception}"
    )


# ================================================================
# LOAD CHECKPOINT (unchanged)
# ================================================================

def load_processed_ids(checkpoint_table: str):
    try:
        checkpoint_df = (
            spark.table(checkpoint_table)
            .select("ConversationId", "error")
            .toPandas()
        )

        if len(checkpoint_df) == 0:
            return set()

        successful_ids = set(
            checkpoint_df[checkpoint_df["error"].isna()]["ConversationId"]
            .dropna().astype(str).unique()
        )

        return successful_ids

    except Exception as e:
        print(f"[CHECKPOINT] Could not load checkpoint: {e}")
        return set()


# ================================================================
# MAIN ASYNC PROCESSOR (FIXED)
# ================================================================

async def process_telephony_batch_async(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None,
    token_refresh_callback: Optional[Callable[[], str]] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scopes: Optional[str] = None
) -> pd.DataFrame:

    # --------------------------------------------------------------
    # AUTOMATIC token refresh: if OAuth client-credentials were
    # supplied and no custom callback was given, build one from
    # get_oauth_token() so 401/403 (and proactive near-expiry
    # refresh) just works with zero extra wiring.
    # --------------------------------------------------------------

    if token_refresh_callback is None and all([tenant_id, client_id, client_secret, scopes]):
        token_refresh_callback = build_oauth_refresh_callback(
            tenant_id, client_id, client_secret, scopes
        )
        print("[AUTH] Automatic token refresh enabled (OAuth2 client-credentials).")

    required_cols = [
        "ConversationId", "ConversationStartTimestamp",
        "ConversationTranscript", "ClaimNumber"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.reset_index(drop=True).copy()
    original_count = len(df)

    if checkpoint_table:
        processed_ids = load_processed_ids(checkpoint_table)
        if processed_ids:
            before = len(df)
            df = df[~df["ConversationId"].astype(str).isin(processed_ids)].copy()
            print(f"[CHECKPOINT] Previously successful: {before - len(df):,}")
            print(f"[CHECKPOINT] Remaining: {len(df):,}")

    df = df[
        df["ConversationTranscript"].fillna("").astype(str).str.len() >= MIN_TRANSCRIPT_CHARS
    ].copy()

    total_rows = len(df)

    print()
    print("=" * 80)
    print("TELEPHONY LLM BATCH")
    print("=" * 80)
    print(f"Input rows:              {original_count:,}")
    print(f"Rows to process:         {total_rows:,}")
    print(f"Concurrency:             {MAX_CONCURRENCY}")
    print(f"Requests/min limit:      {MAX_REQUESTS_PER_MINUTE}")
    print(f"Checkpoint interval:     {CHECKPOINT_EVERY}")
    print(f"Token limit:             {TOKEN_LIMIT:,}")
    print(f"Effective token limit:   {int(TOKEN_LIMIT * SAFETY_BUFFER):,}")
    print("=" * 80)
    print()

    if total_rows == 0:
        print("[COMPLETE] Nothing to process.")
        return pd.DataFrame(columns=CHECKPOINT_COLUMNS)

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    token_mgr = TokenBudgetManager(TOKEN_LIMIT, WINDOW_SECONDS, SAFETY_BUFFER)
    rate_coordinator = GlobalRateLimitCoordinator()
    request_limiter = RequestRateLimiter(MAX_REQUESTS_PER_MINUTE)
    token_holder = TokenHolder(token)

    async def process_single_row(row: pd.Series):

        task_start = time.time()

        async with semaphore:
            try:
                clean_transcript = build_clean_transcript_for_llm(row["ConversationTranscript"])

                if len(clean_transcript) < MIN_TRANSCRIPT_CHARS:
                    return build_result(
                        row,
                        {"customer_message": None, "agent_message": None, "confidence": 0.0},
                        {
                            "prompt_tokens": None, "completion_tokens": None, "total_tokens": None,
                            "api_latency": None, "request_id": None,
                            "error": "Transcript too short after redaction"
                        },
                        time.time() - task_start
                    )

                agents_str = row.get("AgentsInvolved", "")
                prompt = build_telephony_prompt(clean_transcript, agents_str)

                rate_limit_attempts = 0
                auth_attempts = 0
                analysis, meta = None, None

                # ------------------------------------------------
                # NEW: row-level retry loop for 429 / 401 / 403.
                # A rate-limited row is NOT immediately recorded
                # as failed - it waits for the coordinated pause
                # to clear and tries again, up to
                # MAX_ROW_RATE_LIMIT_RETRIES times. Same idea for
                # auth failures, capped at MAX_ROW_AUTH_RETRIES.
                # ------------------------------------------------

                while True:
                    await rate_coordinator.wait_if_paused()
                    await request_limiter.acquire()
                    await token_mgr.reserve_tokens(ESTIMATED_TOKENS_PER_CALL)

                    # Proactive refresh: if the token is close to
                    # expiring, refresh it BEFORE firing the request
                    # instead of waiting to be told via a 401. A
                    # no-op if the token isn't a decodable JWT or
                    # isn't close to expiry yet.
                    await token_holder.ensure_fresh(token_refresh_callback)

                    try:
                        analysis, meta = await asyncio.to_thread(
                            call_telephony_model,
                            prompt,
                            MAX_TOKENS,
                            RETRIES,
                            token_holder.token,
                            modelgateway_baseurl,
                            API_TIMEOUT
                        )
                        rate_coordinator.note_success()
                        break

                    except RateLimitError as rle:
                        rate_limit_attempts += 1

                        if rate_limit_attempts > MAX_ROW_RATE_LIMIT_RETRIES:
                            return build_result(
                                row,
                                {"customer_message": None, "agent_message": None, "confidence": 0.0},
                                {
                                    "prompt_tokens": None, "completion_tokens": None,
                                    "total_tokens": None, "api_latency": None, "request_id": None,
                                },
                                time.time() - task_start,
                                error_override=(
                                    f"RateLimitError: still throttled after "
                                    f"{MAX_ROW_RATE_LIMIT_RETRIES} coordinated retries"
                                )
                            )

                        await rate_coordinator.trigger_pause(rle.retry_after)
                        continue

                    except AuthError:
                        auth_attempts += 1

                        if auth_attempts > MAX_ROW_AUTH_RETRIES:
                            return build_result(
                                row,
                                {"customer_message": None, "agent_message": None, "confidence": 0.0},
                                {
                                    "prompt_tokens": None, "completion_tokens": None,
                                    "total_tokens": None, "api_latency": None, "request_id": None,
                                },
                                time.time() - task_start,
                                error_override=(
                                    f"AuthError: still unauthorized after "
                                    f"{MAX_ROW_AUTH_RETRIES} refresh attempts "
                                    f"(check client credentials / scopes)"
                                    if token_refresh_callback is not None else
                                    "AuthError: token expired/invalid and no "
                                    "token_refresh_callback (or OAuth credentials) "
                                    "was provided"
                                )
                            )

                        try:
                            await token_holder.refresh(token_refresh_callback)
                            continue
                        except AuthError:
                            return build_result(
                                row,
                                {"customer_message": None, "agent_message": None, "confidence": 0.0},
                                {
                                    "prompt_tokens": None, "completion_tokens": None,
                                    "total_tokens": None, "api_latency": None, "request_id": None,
                                },
                                time.time() - task_start,
                                error_override=(
                                    "AuthError: token expired/invalid and no "
                                    "token_refresh_callback (or OAuth credentials) "
                                    "was provided"
                                )
                            )

                actual_tokens = meta.get("total_tokens")
                if actual_tokens:
                    await token_mgr.adjust_tokens(ESTIMATED_TOKENS_PER_CALL, int(actual_tokens))

                return build_result(row, analysis, meta, time.time() - task_start)

            except Exception as e:
                return build_result(
                    row,
                    {"customer_message": None, "agent_message": None, "confidence": 0.0},
                    {
                        "prompt_tokens": None, "completion_tokens": None, "total_tokens": None,
                        "api_latency": None, "request_id": None, "error": None
                    },
                    time.time() - task_start,
                    error_override=f"{type(e).__name__}: {str(e)}"
                )

    tasks = [asyncio.create_task(process_single_row(row)) for _, row in df.iterrows()]

    all_results = []
    checkpoint_buffer = []
    successful = 0
    failed = 0
    start_time = time.time()

    progress = tqdm(
        total=total_rows, desc="Telephony LLM", unit="rows",
        dynamic_ncols=True, mininterval=2, smoothing=0.1
    )

    try:
        for completed_task in asyncio.as_completed(tasks):
            result = await completed_task
            all_results.append(result)
            checkpoint_buffer.append(result)

            if result.get("error"):
                failed += 1
            else:
                successful += 1

            processed = successful + failed
            elapsed = time.time() - start_time
            rate = processed / elapsed * 60 if elapsed > 0 else 0
            remaining = total_rows - processed
            eta_seconds = remaining / (processed / elapsed) if processed > 0 else 0

            progress.update(1)
            progress.set_postfix({
                "ok": f"{successful:,}",
                "failed": f"{failed:,}",
                "rate": f"{rate:.1f}/min",
                "ETA": format_eta(eta_seconds)
            }, refresh=False)

            if len(checkpoint_buffer) >= CHECKPOINT_EVERY:
                if checkpoint_table:
                    save_checkpoint(checkpoint_buffer, checkpoint_table)
                checkpoint_buffer = []

    except asyncio.CancelledError:
        for task in tasks:
            if not task.done():
                task.cancel()
        raise

    finally:
        progress.close()

    if checkpoint_buffer:
        if checkpoint_table:
            save_checkpoint(checkpoint_buffer, checkpoint_table)

    elapsed = time.time() - start_time
    rate = len(all_results) / elapsed * 60 if elapsed > 0 else 0

    print()
    print("=" * 80)
    print("BATCH COMPLETE")
    print("=" * 80)
    print(f"Processed:       {len(all_results):,}")
    print(f"Successful:      {successful:,}")
    print(f"Failed:          {failed:,}")
    print(f"Elapsed:         {format_duration(elapsed)}")
    print(f"Rate:            {rate:.1f} records/min")
    print("=" * 80)

    return pd.DataFrame(all_results)


# ================================================================
# HELPERS (unchanged)
# ================================================================

def format_eta(seconds: float) -> str:
    if not seconds or seconds <= 0:
        return "--"
    seconds = int(seconds)
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_duration(seconds: float) -> str:
    return format_eta(seconds)


# ================================================================
# ASYNC DATE-CHUNK RUNNER (passes through token_refresh_callback)
# ================================================================

async def run_telephony_llm_batch_async(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None,
    date_chunks: List[Tuple[str, str]] = None,
    token_refresh_callback: Optional[Callable[[], str]] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scopes: Optional[str] = None
) -> pd.DataFrame:

    all_results = []

    if date_chunks:
        for start_date, end_date in date_chunks:
            print()
            print("=" * 80)
            print(f"DATE CHUNK: {start_date} -> {end_date}")
            print("=" * 80)

            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
            timestamps = pd.to_datetime(df["ConversationStartTimestamp"], errors="coerce")

            df_chunk = df[(timestamps >= start_ts) & (timestamps < end_ts)].copy()
            print(f"[CHUNK] Input rows: {len(df_chunk):,}")

            if len(df_chunk) == 0:
                print("[CHUNK] No records.")
                continue

            chunk_results = await process_telephony_batch_async(
                df_chunk, token, modelgateway_baseurl,
                checkpoint_table, output_table, token_refresh_callback,
                tenant_id, client_id, client_secret, scopes
            )
            all_results.append(chunk_results)

    else:
        results = await process_telephony_batch_async(
            df, token, modelgateway_baseurl,
            checkpoint_table, output_table, token_refresh_callback,
            tenant_id, client_id, client_secret, scopes
        )
        all_results.append(results)

    if all_results:
        return pd.concat(all_results, ignore_index=True)

    return pd.DataFrame(columns=CHECKPOINT_COLUMNS)


# ================================================================
# NOTEBOOK-SAFE SYNC WRAPPER
# ================================================================

def run_telephony_llm_batch(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None,
    date_chunks: List[Tuple[str, str]] = None,
    token_refresh_callback: Optional[Callable[[], str]] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scopes: Optional[str] = None
) -> pd.DataFrame:
    """
    token (required):
        Your initial bearer token, exactly as before.

    Automatic token refresh - pick ONE of the following, both optional:

      Option A - built-in OAuth2 client-credentials refresh:
        Pass tenant_id, client_id, client_secret, scopes. The batch
        will call get_oauth_token(...) itself, both proactively
        (~2 min before the JWT's `exp` claim) and reactively (on
        any 401/403), and coordinate so only one concurrent worker
        ever hits the token endpoint at a time.

      Option B - your own refresh logic:
        Pass token_refresh_callback: a zero-arg function (sync or
        async) that returns a fresh bearer token string. Use this
        if your token doesn't come from Azure AD client-credentials
        (e.g. a secrets-manager-backed helper, a different IdP).

    If neither is provided, a 401/403 is recorded as a failed row
    (and retried on the next run via checkpoint resume) instead of
    crashing the whole batch - same as before.
    """

    async def runner():
        return await run_telephony_llm_batch_async(
            df, token, modelgateway_baseurl, checkpoint_table,
            output_table, date_chunks, token_refresh_callback,
            tenant_id, client_id, client_secret, scopes
        )

    try:
        asyncio.get_running_loop()
        loop_is_running = True
    except RuntimeError:
        loop_is_running = False

    if not loop_is_running:
        return asyncio.run(runner())

    result_container = []
    exception_container = []

    def thread_target():
        try:
            result = asyncio.run(runner())
            result_container.append(result)
        except Exception as e:
            exception_container.append(e)

    thread = threading.Thread(target=thread_target)
    thread.start()
    thread.join()

    if exception_container:
        raise exception_container[0]

    if result_container:
        return result_container[0]

    return pd.DataFrame(columns=CHECKPOINT_COLUMNS)
