# =====================================
# SAFE CHECKPOINT WRITER
# =====================================

def write_checkpoint_safely(
    df_out: pd.DataFrame,
    checkpoint_table: str
):
    """
    Write checkpoint results to Delta with an explicit stable schema.

    Prevents Pandas/Spark type inference from changing nullable
    BIGINT columns such as prompt_tokens into DOUBLE.
    """

    if df_out is None or len(df_out) == 0:
        print("[CHECKPOINT] Nothing to write")
        return

    print(f"\n[CHECKPOINT] Preparing {len(df_out):,} rows")

    # -----------------------------------------
    # Create Spark DataFrame
    # -----------------------------------------
    spark_df = spark.createDataFrame(df_out)

    # -----------------------------------------
    # Explicit BIGINT columns
    # -----------------------------------------
    bigint_columns = [
        "prompt_tokens",
        "completion_tokens",
        "total_tokens"
    ]

    for col_name in bigint_columns:
        if col_name in spark_df.columns:
            spark_df = spark_df.withColumn(
                col_name,
                F.col(col_name).cast("long")
            )

    # -----------------------------------------
    # Explicit DOUBLE columns
    # -----------------------------------------
    double_columns = [
        "Confidence",
        "api_latency",
        "task_latency"
    ]

    for col_name in double_columns:
        if col_name in spark_df.columns:
            spark_df = spark_df.withColumn(
                col_name,
                F.col(col_name).cast("double")
            )

    # -----------------------------------------
    # Explicit STRING columns
    # -----------------------------------------
    string_columns = [
        "ConversationId",
        "ClaimNumber",
        "MembershipNumber",
        "CustomerMessage",
        "AgentMessage",
        "request_id",
        "error"
    ]

    for col_name in string_columns:
        if col_name in spark_df.columns:
            spark_df = spark_df.withColumn(
                col_name,
                F.col(col_name).cast("string")
            )

    # -----------------------------------------
    # Explicit TIMESTAMP
    # -----------------------------------------
    if "ConversationStartTimestamp" in spark_df.columns:
        spark_df = spark_df.withColumn(
            "ConversationStartTimestamp",
            F.to_timestamp(
                F.col("ConversationStartTimestamp")
            )
        )

    # -----------------------------------------
    # DEBUG SCHEMA
    # -----------------------------------------
    print("[CHECKPOINT] Schema being written:")
    spark_df.printSchema()

    # -----------------------------------------
    # Append to Delta
    # -----------------------------------------
    (
        spark_df
        .write
        .mode("append")
        .format("delta")
        .option("mergeSchema", "false")
        .saveAsTable(checkpoint_table)
    )

    print(
        f"[CHECKPOINT] Successfully saved "
        f"{len(df_out):,} rows"
    )