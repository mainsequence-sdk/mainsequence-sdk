import csv
import datetime
import itertools
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

import numpy as np
import pandas as pd
import tqdm
from tqdm import tqdm  # Import tqdm for progress bar

from mainsequence.logconf import logger

from ..utils import DATE_FORMAT, make_request, set_types_in_table


def import_psycopg2():
    pass


def read_sql_tmpfile(query, time_series_orm_uri_db_connection: str):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
        # conn = db_engine.raw_connection()
        # cur = conn.cursor()
        with psycopg2.connect(time_series_orm_uri_db_connection) as conn:
            # TEMP FOR FUCKED UP BELOW
            # cur = session.connection().connection.cursor()
            cur = conn.cursor()
            cur.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile, header=0)

        return df


def filter_by_assets_ranges(table_name, asset_ranges_map, index_names, data_source, column_types):
    """
    Query time series data dynamically based on asset ranges.

    Args:
        table_name (str): The name of the table to query.
        asset_ranges_map (dict): A dictionary where keys are asset symbols and values are dictionaries containing:
                                 - 'start_date' (datetime): The start date of the range.
                                 - 'start_date_operand' (str): The SQL operand for the start date (e.g., '>=' or '>').
                                 - 'end_date' (datetime or None): The end date of the range.
        index_names (list): List of column names to set as the DataFrame index.
        data_source: A data source object with a method `get_connection_uri()` to get the database connection URI.

    Returns:
        pd.DataFrame: A Pandas DataFrame with the queried data, indexed by the specified columns.
    """
    # Base SQL query
    query_base = f"SELECT * FROM {table_name} WHERE"

    # Initialize a list to store query parts
    query_parts = []

    # Build query dynamically based on the asset_ranges_map dictionary
    for symbol, range_dict in asset_ranges_map.items():
        if range_dict["end_date"] is not None:
            tmp_query = (
                f" (asset_symbol = '{symbol}' AND "
                f"time_index BETWEEN '{range_dict['start_date']}' AND '{range_dict['end_date']}') "
            )
        else:
            tmp_query = (
                f" (asset_symbol = '{symbol}' AND "
                f"time_index {range_dict['start_date_operand']} '{range_dict['start_date']}') "
            )
        query_parts.append(tmp_query)

    # Combine all query parts using OR
    full_query = query_base + " OR ".join(query_parts)

    # Execute the query and load results into a Pandas DataFrame
    df = read_sql_tmpfile(
        full_query, time_series_orm_uri_db_connection=data_source.get_connection_uri()
    )

    # set correct types for values
    df = set_types_in_table(df, column_types)

    # Set the specified columns as the DataFrame index
    df = df.set_index(index_names)

    return df


def direct_data_from_db(
    data_node_update: "DataNodeUpdate",
    connection_uri: str,
    start_date: datetime.datetime | None = None,
    great_or_equal: bool = True,
    less_or_equal: bool = True,
    end_date: datetime.datetime | None = None,
    columns: list | None = None,
    unique_identifier_list: list | None = None,
    unique_identifier_range_map: dict | None = None,
):
    """
    Connects directly to the DB without passing through the ORM to speed up calculations.

    Parameters
    ----------
    metadata : dict
        Metadata containing table and column details.
    connection_config : dict
        Connection configuration for the database.
    start_date : datetime.datetime, optional
        The start date for filtering. If None, no lower bound is applied.
    great_or_equal : bool, optional
        Whether the start_date filter is inclusive (>=). Defaults to True.
    less_or_equal : bool, optional
        Whether the end_date filter is inclusive (<=). Defaults to True.
    end_date : datetime.datetime, optional
        The end date for filtering. If None, no upper bound is applied.
    columns : list, optional
        Specific columns to select. If None, all columns are selected.

    Returns
    -------
    pd.DataFrame
        Data from the table as a pandas DataFrame, optionally filtered by date range.
    """
    import_psycopg2()
    data_node_storage = data_node_update.data_node_storage

    def fast_table_dump(
        connection_config,
        table_name,
    ):
        query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"

        with psycopg2.connect(connection_config["connection_details"]) as connection:
            with connection.cursor() as cursor:
                import io

                buffer = io.StringIO()
                cursor.copy_expert(query, buffer)
                buffer.seek(0)
                df = pd.read_csv(buffer)
                return df

    # Build the SELECT clause
    select_clause = ", ".join(columns) if columns else "*"

    # Build the WHERE clause dynamically
    where_clauses = []
    time_index_name = data_node_storage.sourcetableconfiguration.time_index_name
    if start_date:
        operator = ">=" if great_or_equal else ">"
        where_clauses.append(f"{time_index_name} {operator} '{start_date}'")
    if end_date:
        operator = "<=" if less_or_equal else "<"
        where_clauses.append(f"{time_index_name} {operator} '{end_date}'")

    if unique_identifier_list:
        helper_symbol = "','"
        where_clauses.append(
            f"unique_identifier IN ('{helper_symbol.join(unique_identifier_list)}')"
        )

    # Combine WHERE clauses
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Construct the query
    query = f"SELECT {select_clause} FROM {data_node_storage.table_name} {where_clause}"
    # if where_clause=="":
    #     data=fast_table_dump(connection_config, metadata['table_name'])
    #     data[metadata["sourcetableconfiguration"]['time_index_name']]=pd.to_datetime(data[metadata["sourcetableconfiguration"]['time_index_name']])
    # else:
    with psycopg2.connect(connection_uri) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            column_names = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()

    # Convert to DataFrame
    data = pd.DataFrame(data=data, columns=column_names)

    data = data.set_index(data_node_storage.sourcetableconfiguration.index_names)

    return data


def direct_table_update(
    data_node_storage: "DataNodeStorage",
    serialized_data_frame: pd.DataFrame,
    overwrite: bool,
    grouped_dates,
    table_is_empty: bool,
    time_series_orm_db_connection: str | None = None,
    use_chunks: bool = True,
    num_threads: int = 4,
):
    """
    Updates the database table with the given DataFrame.

    Parameters:
    - table_name: Name of the database table.
    - serialized_data_frame: DataFrame containing the data to insert.
    - overwrite: If True, existing data in the date range will be deleted before insertion.
    - time_index_name: Name of the time index column.
    - index_names: List of index column names.
    - table_is_empty: If True, the table is empty.
    - time_series_orm_db_connection: Database connection string.
    - use_chunks: If True, data will be inserted in chunks using threads.
    - num_threads: Number of threads to use when use_chunks is True.
    """
    import_psycopg2()
    columns = serialized_data_frame.columns.tolist()

    index_names = data_node_storage.sourcetableconfiguration.index_names
    table_name = data_node_storage.table_name
    time_index_name = data_node_storage.sourcetableconfiguration.time_index_name

    def drop_indexes(table_name, table_index_names):
        # Use a separate connection for index management
        with psycopg2.connect(time_series_orm_db_connection) as conn:
            with conn.cursor() as cur:
                for index_name in index_names.keys():
                    drop_index_query = f'DROP INDEX IF EXISTS "{index_name}";'
                    print(f"Dropping index '{index_name}'...")
                    cur.execute(drop_index_query)
            # Commit changes after all indexes are processed
            conn.commit()
            print("All specified indexes dropped successfully.")

        # Drop indexes before insertion

    # do not drop indices this is only done on inception
    if data_node_storage._drop_indices == True:
        table_index_names = (
            data_node_storage.sourcetableconfiguration.get_time_scale_extra_table_indices()
        )
        drop_indexes(table_name, table_index_names)

    if overwrite and not table_is_empty:
        min_d = serialized_data_frame[time_index_name].min()
        max_d = serialized_data_frame[time_index_name].max()

        with psycopg2.connect(time_series_orm_db_connection) as conn:
            try:
                with conn.cursor() as cur:

                    if len(index_names) > 1:

                        grouped_dates = grouped_dates.rename(
                            columns={"min": "start_time", "max": "end_time"}
                        )
                        grouped_dates = grouped_dates.reset_index()
                        grouped_dates = grouped_dates.to_dict("records")

                        # Build the DELETE query
                        delete_conditions = []
                        for item in grouped_dates:
                            unique_identifier = item["unique_identifier"]
                            start_time = item["start_time"]
                            end_time = item["end_time"]

                            # Format timestamps as strings
                            start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S%z")
                            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S%z")

                            # Escape single quotes
                            unique_identifier = unique_identifier.replace("'", "''")

                            # Build the condition string
                            condition = (
                                f"({time_index_name} >= '{start_time_str}' AND {time_index_name} <= '{end_time_str}' "
                                f"AND unique_identifier = '{unique_identifier}')"
                            )
                            delete_conditions.append(condition)

                        # Combine all conditions using OR
                        where_clause = " OR ".join(delete_conditions)
                        delete_query = f"DELETE FROM public.{table_name} WHERE {where_clause};"

                        # Execute the DELETE query
                        cur.execute(delete_query)
                    else:
                        # Build a basic DELETE query using parameterized values
                        delete_query = f"DELETE FROM public.{table_name} WHERE {time_index_name} >= %s AND {time_index_name} <= %s;"
                        cur.execute(delete_query, (min_d, max_d))

                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"An error occurred during deletion: {e}")
                raise

    if use_chunks:
        total_rows = len(serialized_data_frame)
        num_threads = min(num_threads, total_rows)
        chunk_size = int(np.ceil(total_rows / num_threads))

        # Generator to yield chunks without copying data
        def get_dataframe_chunks(df, chunk_size):
            for start_row in range(0, df.shape[0], chunk_size):
                yield df.iloc[start_row : start_row + chunk_size]

        # Progress bar for chunks
        total_chunks = int(np.ceil(total_rows / chunk_size))

        def insert_chunk(chunk_df):
            try:
                with psycopg2.connect(time_series_orm_db_connection) as conn:
                    with conn.cursor() as cur:
                        buffer_size = 10000  # Adjust based on memory and performance requirements
                        data_generator = chunk_df.itertuples(index=False, name=None)

                        total_records = len(chunk_df)
                        with tqdm(
                            total=total_records, desc="Inserting records", leave=False
                        ) as pbar:
                            while True:
                                batch = list(itertools.islice(data_generator, buffer_size))
                                if not batch:
                                    break

                                # Convert batch to CSV formatted string
                                output = StringIO()
                                writer = csv.writer(output)
                                writer.writerows(batch)
                                output.seek(0)

                                copy_query = f"COPY public.{table_name} ({', '.join(columns)}) FROM STDIN WITH CSV"
                                cur.copy_expert(copy_query, output)

                                # Update progress bar
                                pbar.update(len(batch))

                    conn.commit()
            except Exception as e:
                print(f"An error occurred during insertion: {e}")
                raise

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(
                tqdm(
                    executor.map(
                        insert_chunk, get_dataframe_chunks(serialized_data_frame, chunk_size)
                    ),
                    total=total_chunks,
                    desc="Processing chunks",
                )
            )

    else:
        # Single insert using the same optimized method
        try:
            with psycopg2.connect(time_series_orm_db_connection) as conn:
                with conn.cursor() as cur:
                    buffer_size = 10000
                    data_generator = serialized_data_frame.itertuples(index=False, name=None)
                    total_records = len(serialized_data_frame)
                    with tqdm(total=total_records, desc="Inserting records") as pbar:
                        while True:
                            batch = list(itertools.islice(data_generator, buffer_size))
                            if not batch:
                                break
                            #
                            output = StringIO()
                            writer = csv.writer(output)
                            writer.writerows(batch)
                            output.seek(0)

                            copy_query = f"COPY public.{table_name} ({', '.join(columns)}) FROM STDIN WITH CSV"
                            cur.copy_expert(copy_query, output)

                            # Update progress bar
                            pbar.update(len(batch))

                conn.commit()
        except Exception as e:
            print(f"An error occurred during single insert: {e}")
            raise
    # do not rebuild  indices this is only done on inception
    if data_node_storage._rebuild_indices:
        logger.info("Rebuilding indices...")
        extra_indices = (
            data_node_storage.sourcetableconfiguration.get_time_scale_extra_table_indices()
        )

        with psycopg2.connect(time_series_orm_db_connection) as conn:
            with conn.cursor() as cur:
                # Create each index
                for index_name, index_details in extra_indices.items():
                    index_type, index_query = index_details["type"], index_details["query"]

                    if index_type not in ("INDEX", "UNIQUE INDEX"):
                        raise Exception(f"Unknown index type: {index_type}")

                    sql_create_index = (
                        f"CREATE {index_type} {index_name} ON public.{table_name} {index_query}"
                    )
                    logger.info(f"Executing SQL: {sql_create_index}")
                    cur.execute(sql_create_index)

                # After creating all indexes, run ANALYZE to update statistics
                sql_analyze = f"ANALYZE public.{table_name}"
                logger.info(f"Executing SQL: {sql_analyze}")
                cur.execute(sql_analyze)

            # Commit the transaction after creating indexes and analyzing
            conn.commit()

        logger.info("Index rebuilding and ANALYZE complete.")


def process_and_update_table(
    serialized_data_frame,
    data_node_update: "DataNodeUpdate",
    grouped_dates: list,
    data_source: object,
    index_names: list[str],
    time_index_name: str,
    overwrite: bool = False,
    JSON_COMPRESSED_PREFIX: list[str] = None,
):
    """
    Process a serialized DataFrame, handle overwriting, and update a database table.

    Args:
        serialized_data_frame (pd.DataFrame): The DataFrame to process and update.
        data_node_storage (DataNodeStorage): data_node_storage about the table, including table configuration.
        grouped_dates (list): List of grouped dates to assist with the update.
        data_source (object): A data source object with a `get_connection_uri` method.
        index_names (list): List of index column names.
        time_index_name (str): The name of the time index column.
        overwrite (bool): Whether to overwrite the table or not.
        JSON_COMPRESSED_PREFIX (list): List of prefixes to identify JSON-compressed columns.

    Returns:
        None
    """
    import_psycopg2()
    JSON_COMPRESSED_PREFIX = JSON_COMPRESSED_PREFIX or []
    data_node_storage = data_node_update.data_node_storage
    if "unique_identifier" in serialized_data_frame.columns:
        serialized_data_frame["unique_identifier"] = serialized_data_frame[
            "unique_identifier"
        ].astype(str)

    TDAG_ENDPOINT = f"{os.environ.get('TDAG_ENDPOINT')}"
    base_url = TDAG_ENDPOINT + "/orm/api/dynamic_table"  # metadata.get("root_url")
    serialized_data_frame = serialized_data_frame.replace({np.nan: None})

    # Validate JSON-compressed columns
    for c in serialized_data_frame.columns:
        if any([t in c for t in JSON_COMPRESSED_PREFIX]):
            assert isinstance(serialized_data_frame[c].iloc[0], dict)

    # Encode JSON-compressed columns
    for c in serialized_data_frame.columns:
        if any([t in c for t in JSON_COMPRESSED_PREFIX]):
            serialized_data_frame[c] = serialized_data_frame[c].apply(
                lambda x: json.dumps(x).encode()
            )

    # Handle overwrite and decompress chunks if required
    recompress = False
    if overwrite:
        url = f"{base_url}/{data_node_storage.id}/decompress_chunks/"
        from ..models_vam import BaseObject

        s = BaseObject.build_session()

        r = make_request(
            s=s,
            loaders=BaseObject.LOADERS,
            r_type="POST",
            url=url,
            payload={
                "json": {
                    "start_date": serialized_data_frame[time_index_name]
                    .min()
                    .strftime(DATE_FORMAT),
                    "end_date": serialized_data_frame[time_index_name].max().strftime(DATE_FORMAT),
                }
            },
            time_out=60 * 5,
        )

        if r.status_code not in [200, 204]:
            logger.error(r.text)
            raise Exception("Error trying to decompress table")
        elif r.status_code == 200:
            recompress = True

    # Check if the table is empty
    table_is_empty = data_node_storage.sourcetableconfiguration.last_time_index_value is None

    # Update the table
    direct_table_update(
        serialized_data_frame=serialized_data_frame,
        grouped_dates=grouped_dates,
        time_series_orm_db_connection=data_source.get_connection_uri(),
        data_node_storage=data_node_storage,
        overwrite=overwrite,
        table_is_empty=table_is_empty,
    )

    # Recompress if needed
    if recompress:
        # Logic to recompress if needed (currently a placeholder)
        pass
