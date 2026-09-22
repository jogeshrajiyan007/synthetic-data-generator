from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)

def save_checkpoint(
    results: List[Dict[str, Any]],
    checkpoint_table: str
):

    if not results:
        return

    # ============================================================
    # BUILD PANDAS DATAFRAME
    # ============================================================

    checkpoint_df = pd.DataFrame(results)

    # Guarantee all columns exist
    for col in CHECKPOINT_COLUMNS:

        if col not in checkpoint_df.columns:
            checkpoint_df[col] = None

    checkpoint_df = checkpoint_df[
        CHECKPOINT_COLUMNS
    ].copy()

    # ============================================================
    # NORMALIZE NULLS / TYPES BEFORE SPARK
    # ============================================================

    # Strings
    string_columns = [
        "ConversationId",
        "ClaimNumber",
        "MembershipNumber",
        "CustomerMessage",
        "AgentMessage",
        "request_id",
        "error"
    ]

    for col in string_columns:

        checkpoint_df[col] = (
            checkpoint_df[col]
            .where(
                checkpoint_df[col].notna(),
                None
            )
        )

        checkpoint_df[col] = checkpoint_df[col].apply(
            lambda x: str(x) if x is not None else None
        )

    # ============================================================
    # BIGINT COLUMNS
    # ============================================================

    bigint_columns = [
        "prompt_tokens",
        "completion_tokens",
        "total_tokens"
    ]

    for col in bigint_columns:

        checkpoint_df[col] = pd.to_numeric(
            checkpoint_df[col],
            errors="coerce"
        )

        checkpoint_df[col] = checkpoint_df[col].apply(
            lambda x: int(x) if pd.notna(x) else None
        )

    # ============================================================
    # DOUBLE COLUMNS
    # ============================================================

    double_columns = [
        "Confidence",
        "api_latency",
        "task_latency"
    ]

    for col in double_columns:

        checkpoint_df[col] = pd.to_numeric(
            checkpoint_df[col],
            errors="coerce"
        )

        checkpoint_df[col] = checkpoint_df[col].apply(
            lambda x: float(x) if pd.notna(x) else None
        )

    # ============================================================
    # TIMESTAMP COLUMNS
    # ============================================================

    timestamp_columns = [
        "ConversationStartTimestamp",
        "processed_at"
    ]

    for col in timestamp_columns:

        checkpoint_df[col] = pd.to_datetime(
            checkpoint_df[col],
            errors="coerce"
        )

        checkpoint_df[col] = checkpoint_df[col].apply(
            lambda x: x.to_pydatetime()
            if pd.notna(x)
            else None
        )

    # ============================================================
    # EXPLICIT SPARK SCHEMA
    # ============================================================

    checkpoint_schema = StructType([

        StructField(
            "ConversationId",
            StringType(),
            True
        ),

        StructField(
            "ClaimNumber",
            StringType(),
            True
        ),

        StructField(
            "ConversationStartTimestamp",
            TimestampType(),
            True
        ),

        StructField(
            "MembershipNumber",
            StringType(),
            True
        ),

        StructField(
            "CustomerMessage",
            StringType(),
            True
        ),

        StructField(
            "AgentMessage",
            StringType(),
            True
        ),

        StructField(
            "Confidence",
            DoubleType(),
            True
        ),

        StructField(
            "prompt_tokens",
            LongType(),
            True
        ),

        StructField(
            "completion_tokens",
            LongType(),
            True
        ),

        StructField(
            "total_tokens",
            LongType(),
            True
        ),

        StructField(
            "api_latency",
            DoubleType(),
            True
        ),

        StructField(
            "task_latency",
            DoubleType(),
            True
        ),

        StructField(
            "request_id",
            StringType(),
            True
        ),

        StructField(
            "error",
            StringType(),
            True
        ),

        StructField(
            "processed_at",
            TimestampType(),
            True
        )
    ])

    # ============================================================
    # CREATE SPARK DATAFRAME USING EXPLICIT SCHEMA
    # ============================================================

    records = checkpoint_df.to_dict(
        orient="records"
    )

    spark_df = spark.createDataFrame(
        records,
        schema=checkpoint_schema
    )

    # ============================================================
    # DEBUG
    # ============================================================

    print("\n[CHECKPOINT] Schema being written:")
    spark_df.printSchema()

    # Optional: inspect actual rows with null token values
    print(
        "[CHECKPOINT] Rows:",
        spark_df.count()
    )

    # ============================================================
    # WRITE
    # ============================================================

    last_exception = None

    for attempt in range(
        1,
        CHECKPOINT_RETRIES + 1
    ):

        try:

            (
                spark_df.write
                .mode("append")
                .format("delta")
                .option("mergeSchema", "false")
                .saveAsTable(checkpoint_table)
            )

            print(
                f"[CHECKPOINT] Successfully Saved "
                f"{len(records):,} rows"
            )

            return

        except Exception as e:

            last_exception = e

            print(
                f"[CHECKPOINT] Attempt {attempt} Failed: "
                f"{type(e).__name__}: {e}"
            )

            if attempt < CHECKPOINT_RETRIES:

                time.sleep(
                    2 ** attempt
                )

    raise RuntimeError(
        "Checkpoint write failed "
        f"after {CHECKPOINT_RETRIES} attempts: "
        f"{last_exception}"
    )