# ============================================
# CREATE SPARK DATAFRAME
# ============================================

spark_df = spark.createDataFrame(checkpoint_df)

# ============================================
# FORCE CHECKPOINT SCHEMA
# ============================================

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

# ============================================
# WRITE
# ============================================

(
    spark_df
    .write
    .mode("append")
    .format("delta")
    .option("mergeSchema", "false")
    .saveAsTable(checkpoint_table)
)