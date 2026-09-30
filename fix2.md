Yes. At this point I would stop trying to solve this entirely with one large SQL regex.

You have three separate problems:

1. Section classification

REASON FOR CALL → customer

ACTIONS & AGREEMENTS → classify each statement

NEXT STEPS → agent, regardless of whether it says "Caller"



2. Speaker classification

Caller/Customer → CustomerMessage

Agent → AgentMessage



3. Deduplication

The same statement can appear multiple times.

We should normalize it before comparing, while preserving the original readable text.




For this kind of semi-structured generated text, a small PySpark Python UDF returning a struct is much easier to maintain than a giant regexp_extract.

Recommended architecture

Your existing extraction can remain:

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import re

Then create a parser:

message_schema = StructType([
    StructField("CustomerMessage", StringType(), True),
    StructField("AgentMessage", StringType(), True)
])


def parse_transcript_summary(text):
    if not text:
        return (None, None)

    # ---------------------------------------------------------
    # 1. Normalize the raw text
    # ---------------------------------------------------------
    text = text.replace("\\n", "\n")
    text = text.replace("\\r", "\r")
    text = text.replace("\\t", "\t")

    # Remove markdown emphasis around headings
    text = re.sub(r'\*{1,2}', '', text)

    # Normalize whitespace but preserve newlines
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)

    # ---------------------------------------------------------
    # 2. Extract sections
    # ---------------------------------------------------------
    reason_match = re.search(
        r'REASON\s+FOR\s+CALL\s*(.*?)'
        r'(?=ACTIONS\s*&\s*AGREEMENTS|NEXT\s+STEPS|Correlation\s+ID|$)',
        text,
        flags=re.I | re.S
    )

    actions_match = re.search(
        r'ACTIONS\s*&\s*AGREEMENTS\s*(.*?)'
        r'(?=NEXT\s+STEPS|Correlation\s+ID|$)',
        text,
        flags=re.I | re.S
    )

    next_steps_match = re.search(
        r'NEXT\s+STEPS\s*(.*?)'
        r'(?=Correlation\s+ID|$)',
        text,
        flags=re.I | re.S
    )

    reason = reason_match.group(1) if reason_match else ""
    actions = actions_match.group(1) if actions_match else ""
    next_steps = next_steps_match.group(1) if next_steps_match else ""

    # ---------------------------------------------------------
    # 3. Helpers
    # ---------------------------------------------------------

    def clean_message(msg):
        if not msg:
            return ""

        # Remove bullet
        msg = re.sub(r'^\s*[-•]\s*', '', msg)

        # Remove trailing whitespace
        msg = msg.strip()

        # Remove correlation ID if it accidentally gets included
        msg = re.sub(
            r'\s*Correlation\s+ID\s*:.*$',
            '',
            msg,
            flags=re.I | re.S
        )

        return msg.strip()

    def normalize_for_dedupe(msg):
        """
        Creates a comparison key without changing the displayed text.
        """

        key = msg.lower()

        # Normalize curly quotes
        key = key.replace("’", "'")
        key = key.replace("“", '"')
        key = key.replace("”", '"')

        # Remove markdown
        key = re.sub(r'\*+', '', key)

        # Normalize whitespace
        key = re.sub(r'\s+', ' ', key)

        # Remove punctuation around words
        key = re.sub(r'[^\w\s]', '', key)

        return key.strip()

    def dedupe(messages):
        """
        Deduplicate while preserving original order.
        """

        result = []
        seen = set()

        for msg in messages:
            msg = clean_message(msg)

            if not msg:
                continue

            key = normalize_for_dedupe(msg)

            if key and key not in seen:
                seen.add(key)
                result.append(msg)

        return result

    # ---------------------------------------------------------
    # 4. Extract bullet statements
    # ---------------------------------------------------------
    def extract_bullets(section):
        if not section:
            return []

        # Capture each '-' bullet, including wrapped lines
        bullets = re.findall(
            r'(?ms)^\s*[-•]\s*(.*?)(?=\n\s*[-•]\s*|\Z)',
            section
        )

        # If there are no bullets, treat the section as one message
        if not bullets:
            cleaned = clean_message(section)
            return [cleaned] if cleaned else []

        return [clean_message(x) for x in bullets if clean_message(x)]

    # ---------------------------------------------------------
    # 5. Customer messages
    # ---------------------------------------------------------

    customer_messages = []

    # REASON FOR CALL is always customer context
    reason_clean = clean_message(reason)

    if reason_clean:
        customer_messages.append(reason_clean)

    # ACTIONS & AGREEMENTS
    action_bullets = extract_bullets(actions)

    for msg in action_bullets:

        # Customer-side statements
        if re.match(
            r'^(caller|customer|member|patient|client)\b',
            msg,
            flags=re.I
        ):
            customer_messages.append(msg)

    # ---------------------------------------------------------
    # 6. Agent messages
    # ---------------------------------------------------------

    agent_messages = []

    for msg in action_bullets:

        # Agent-side statements
        if re.match(
            r'^(agent|representative|advisor|associate)\b',
            msg,
            flags=re.I
        ):
            agent_messages.append(msg)

    # NEXT STEPS ALWAYS belong to AgentMessage
    next_step_bullets = extract_bullets(next_steps)

    agent_messages.extend(next_step_bullets)

    # ---------------------------------------------------------
    # 7. Deduplicate
    # ---------------------------------------------------------

    customer_messages = dedupe(customer_messages)
    agent_messages = dedupe(agent_messages)

    # ---------------------------------------------------------
    # 8. Final output
    # ---------------------------------------------------------

    customer_output = "\n".join(customer_messages) or None
    agent_output = "\n".join(agent_messages) or None

    return customer_output, agent_output

Register it:

parse_summary_udf = F.udf(
    parse_transcript_summary,
    message_schema
)

Then apply it to your transcript_summary:

result = (
    cte_transcripts
    .withColumn(
        "parsed",
        parse_summary_udf(F.col("transcript_summary"))
    )
    .select(
        "*",
        F.col("parsed.CustomerMessage").alias("CustomerMessage"),
        F.col("parsed.AgentMessage").alias("AgentMessage")
    )
    .drop("parsed")
)


---

What this does with your screenshot

Suppose the summary contains:

REASON FOR CALL

Caller mentioned previous tests and a consultation with a specialist.

ACTIONS & AGREEMENTS

- Caller mentioned prior tests (blood tests and CT scan) and a consultation with a specialist.
- Agent confirmed the need to transfer the query to the pre-authorisation team.
- Caller mentioned prior tests (blood tests and CT scan) and a consultation with a specialist.
- Agent confirmed the need to transfer the query to the pre-authorisation team.

NEXT STEPS

- Caller to discuss coverage details with the pre-authorisation team.
- Agent transferred the call to the appropriate team.

The output becomes:

CustomerMessage

Caller mentioned previous tests and a consultation with a specialist.
Caller mentioned prior tests (blood tests and CT scan) and a consultation with a specialist.

The duplicate Caller statement is removed.

AgentMessage

Agent confirmed the need to transfer the query to the pre-authorisation team.
Caller to discuss coverage details with the pre-authorisation team.
Agent transferred the call to the appropriate team.

Notice the important behavior:

Caller to discuss coverage details...

is in AgentMessage, because it came from NEXT STEPS.

That's different from ACTIONS & AGREEMENTS, where we classify based on the speaker.


---

One improvement I'd strongly recommend

There is one difficult case we should handle explicitly.

Suppose the generated summary says:

- Caller mentioned the CT scan and Agent confirmed that it was covered.

Our bullet-level logic sees Caller first and puts the whole bullet into CustomerMessage.

If your data contains these combined statements, you should go one level deeper and split the bullet into speaker statements.

For example:

Caller mentioned the CT scan and Agent confirmed that it was covered.

should become:

CustomerMessage:
Caller mentioned the CT scan.

AgentMessage:
Agent confirmed that it was covered.

We can add that to the parser.


---

Also: deduplication should be smarter than DISTINCT

I would not simply do:

array_distinct(...)

because these can technically be different strings:

Caller mentioned prior tests.

caller mentioned prior tests

or:

Caller mentioned prior tests (blood tests and CT scan).

Caller mentioned prior tests (blood tests and CT scan)

Our normalize_for_dedupe() creates a comparison key:

caller mentioned prior tests blood tests and ct scan

while keeping the original text for the output.

So you get deduplication without destroying the readable message.


---

Your pipeline would then look like this

I'd structure your notebook as:

Raw table
    ↓
Extract conversationId / sessionStartTime / agentEmail
    ↓
Extract ChatSummary.text_output
    ↓
parse_transcript_summary()
    ↓
┌───────────────────────────┐
│ CustomerMessage           │
│ AgentMessage              │
└───────────────────────────┘
    ↓
Final Transcripts table/view

This is considerably easier to extend than the current SQL:

regexp_extract(...)
regexp_extract(...)
regexp_extract(...)

because the rules can evolve without turning one SQL expression into an unmaintainable regex.

One caveat

A regular Python UDF is convenient but can be slower on a very large Databricks table because data crosses the Python/JVM boundary. If you're processing millions of transcripts, I'd use the same parsing logic as a Pandas UDF / applyInPandas, or implement the simple section/bullet extraction using native Spark functions and reserve Python only for the speaker classification/deduplication.

If this is a large production dataset, I can give you a fully native PySpark version (no Python UDF) that performs the same classification + deduplication and will scale much better.