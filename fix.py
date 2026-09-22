# =====================================
# DATabricks / NOTEBOOK ASYNC WRAPPER
# =====================================

async def run_telephony_llm_batch_async(
    df: pd.DataFrame,
    token: str,
    modelgateway_baseurl: str,
    checkpoint_table: str = None,
    output_table: str = None,
    date_chunks: List[Tuple[str, str]] = None
) -> pd.DataFrame:
    """
    Databricks-safe async wrapper.

    IMPORTANT:
    Do not use asyncio.run() or loop.run_until_complete()
    inside a Databricks/Jupyter notebook.
    """

    all_results = []

    if date_chunks:

        for start_date, end_date in date_chunks:

            print(f"\n{'=' * 80}")
            print(f"[CHUNK] Processing {start_date} to {end_date}")
            print(f"{'=' * 80}")

            # IMPORTANT:
            # Use < next day instead of <= end_date
            # so the entire end date is included.
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)

            df_chunk = df[
                (pd.to_datetime(df["ConversationStartTimestamp"]) >= start_ts) &
                (pd.to_datetime(df["ConversationStartTimestamp"]) < end_ts)
            ].copy()

            print(f"[CHUNK] Records found: {len(df_chunk):,}")

            if len(df_chunk) == 0:
                print(
                    f"[CHUNK] No data in range "
                    f"{start_date} to {end_date}"
                )
                continue

            df_chunk_out = await process_telephony_batch_async(
                df=df_chunk,
                token=token,
                modelgateway_baseurl=modelgateway_baseurl,
                checkpoint_table=checkpoint_table,
                output_table=output_table
            )

            all_results.append(df_chunk_out)

            print(
                f"[CHUNK] Completed {start_date} to {end_date}: "
                f"{len(df_chunk_out):,} results"
            )

    else:

        print(f"[BATCH] Processing {len(df):,} records")

        df_out = await process_telephony_batch_async(
            df=df,
            token=token,
            modelgateway_baseurl=modelgateway_baseurl,
            checkpoint_table=checkpoint_table,
            output_table=output_table
        )

        all_results.append(df_out)

    if all_results:
        return pd.concat(all_results, ignore_index=True)

    return pd.DataFrame()