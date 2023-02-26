from cmath import nan
import os
import pandas as pd
import numpy as np
from typing import List, Union
from datetime import datetime
from glob import glob
from tqdm import tqdm

from google.cloud import bigquery

from .table import Table

LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH")


class GCPError(Exception):
    pass


def gcp_push(
    table: Table, df: pd.DataFrame, overwrite: bool = False, primary_keys: list = None
) -> None:
    """
    Store a dataframe inside a Table.

    Parameters
    ----------
    - table: Table, the reference of the table to create or update.
    - df: pd.DataFrame, data to push
    - overwrite: bool, default `False`, writing mode.
        - If set to `False`, the data will be appended to the end of the table
        - If set to `True`, the table will be overwritten, by dropping duplicate of `primary_keys`
    - primary_keys: list, default `None`. Unused if overwrite set to `False`. Keys to overwrite.
    """
    check_params(table, df, overwrite, primary_keys)
    if not exist_table(table):
        create_table(table)
    df = cast_object_cols_to_str(df)
    load_table(df, table, overwrite, primary_keys)


def gcp_fetch(
    sql: str, as_df: bool = True
) -> Union[bigquery.job.query.QueryJob, pd.DataFrame]:
    """
    Fetch a dataframe from a Table.

    Parameters
    ----------
    - sql: str, SQL query
    - as_df: bool, default is `True`, if set to `True` the result is a pd.DataFrame
    """
    sql_display = sql
    if len(sql) > 100:
        sql_display += "[...]"

    print(f" # [GCP] Fetch: {sql_display}")
    client = bigquery.Client()
    query_job = client.query(sql)
    if as_df:
        return query_job.to_dataframe()
    else:
        return query_job


def gcp_query(sql: str, table_destination: Table) -> bigquery.job.query.QueryJob:
    """
    Send a query to GCP and transfer result into a destination table.

    Destination table is created automatically if not exists.
    Transfer overwrites the previous table.
    """
    sql_display = sql
    if len(sql) > 100:
        sql_display += "[...]"

    print(f" # [GCP] Query: {sql_display}")
    client = bigquery.Client()
    if not exist_table(table_destination):
        gcp_table_dest = create_table(table_destination)
    else:
        gcp_table_dest = client.dataset(table_destination.dataset_id).table(
            table_destination.table_id
        )
    config = bigquery.QueryJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        destination=gcp_table_dest,
    )
    query_job = client.query(sql, config)
    return query_job


def check_params(table: Table, df: pd.DataFrame, overwrite: bool, primary_keys: list):
    """
    Enforce typing and push conditions.
    """
    helper = (
        "=" * 20
        + "\nUsage: gcp_push(table: Table, df: pd.DataFrame, overwrite: bool = False, primary_keys: list = None)"
    )
    # check types
    primary_keys = primary_keys or []
    args_types = {
        "table": (table, Table),
        "df": (df, pd.DataFrame),
        "overwrite": (overwrite, bool),
        "primary_keys": (primary_keys, List),
    }
    for key, (arg, arg_type) in args_types.items():
        if not isinstance(arg, arg_type):
            raise GCPError(
                f" # [GCP] {key} type must be utils.gcp.Table, got '{type(table)}' instead"
                + helper
            )

    # check overwrite and primary_keys interactions
    if overwrite and len(primary_keys) == 0:
        raise GCPError(
            " # [GCP] You must define primary_keys to perform an overwrite update"
        )

    # check dataframe length
    if df.empty:
        raise GCPError(" # [GCP] The dataframe is empty")


def cast_object_cols_to_str(df):
    obj_cols = df.select_dtypes(include=["object"]).columns
    print(f" # [GCP] Cast cols: {obj_cols} from object to str")
    df[obj_cols] = df[obj_cols].astype(str)
    cols = set(df.columns) - set(obj_cols)
    N = df.shape[0]
    nan_cols = [col for col in cols if df[col].isna().sum() == N]
    print(f" # [GCP] Cast cols: {nan_cols} from Int (only NaN values) to str")
    df[nan_cols] = df[nan_cols].astype(str)
    return df


def load_table(df, table, overwrite, primary_keys):
    """
    Push a dataframe into a table by either appending or overwriting.
    If overwrite is False: batching is activate.
    """
    df["dedomena_update_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    table_id = str(table)
    print(f" # [GCP] Loading dataframe of shape {df.shape} into {table_id}")
    if overwrite:
        df = update_df(df, table_id, primary_keys)
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        batching = False
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        batching = True
    if batching:
        df_chunks = chunk_data(df)
        print(f" # [GCP] Load batching activate, batch size: {df_chunks[0].shape[0]}")
        for df_chunk in tqdm(df_chunks):
            _load_table(df_chunk, table_id, write_disposition)
    else:
        _load_table(df, table_id, write_disposition)


def chunk_data(df: pd.DataFrame) -> List[pd.DataFrame]:
    n_cells_ideal = int(24e6)
    n_cells = df.shape[0] * df.shape[1]
    n_chunks = n_cells // n_cells_ideal + 1
    df_chunks = np.array_split(df, n_chunks)
    return df_chunks


def _load_table(df, table_id, write_disposition):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition, source_format=bigquery.SourceFormat.CSV
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = job.result()
    print(f" # [GCP] Result state: {result.state}")


def update_df(df, table_id, primary_keys):
    """
    Merge current dataframe with the existing one in production.
    """
    client = bigquery.Client()
    df_existing = client.list_rows(table_id).to_dataframe()
    old_cols, current_cols = df_existing.columns, df.columns

    missing_cols = list(set(old_cols) - set(current_cols))
    if len(missing_cols) > 0:
        print(f" # [GCP] Warning - {len(missing_cols)} missing columns: {missing_cols}")

    new_cols = list(set(current_cols) - set(old_cols))
    if len(new_cols) > 0:
        print(f" # [GCP] Warning - {len(new_cols)} new columns: {new_cols}")

    for col in missing_cols:
        df[col] = None
    for col in new_cols:
        df_existing[col] = None
    if df.shape[1] != df_existing.shape[1]:
        raise GCPError(
            f" # [GCP] columns mismatch! current: {df.shape}, old: {df_existing.shape}"
        )

    df = pd.concat([df, df_existing])
    merge_size = df.shape[0]
    df.drop_duplicates(subset=primary_keys, keep="first", inplace=True)
    deduplicate_size = df.shape[0]
    print(f" # [GCP] {merge_size - deduplicate_size} rows overwritten")

    return df


def exist_table(table: Table) -> bool:
    """
    Check if the table already exists in our dataset.
    """
    client = bigquery.Client()
    tables = list(client.list_tables(table.dataset_id))
    table_ids = [t.table_id for t in tables]
    exist = table.table_id in table_ids
    if not exist:
        print(f" # [GCP] Table {str(table)} doesn't exist!")
    return exist


def create_table(table: Table) -> None:
    """
    Create a table inside a Dataset.
    Called when pushing a dataframe without existing table.
    """
    client = bigquery.Client()
    gcp_table = bigquery.Table(str(table))  # , schema=data.schema)
    gcp_table = client.create_table(gcp_table)
    print(f" # [GCP] create_table: {str(table)}")
    return gcp_table


def create_dataset(dataset_name: str, project_id: str = None) -> None:
    """
    Create a dataset (aka schemas) inside a project.
    If `project_id` is None, the `client.project` is used.
    Need to be called manually.
    """
    client = bigquery.Client()
    # check project_id exist
    if not project_id is None:
        existing_project_ids = [p.project_id for p in client.list_projects()]
        if not project_id in existing_project_ids:
            raise GCPError(
                f" # [GCP] project_id: {project_id} not in existing projects: {existing_project_ids}"
            )
    else:
        project_id = client.project
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset, timeout=30)
    print(f" # [GCP] Created dataset {client.project}.{dataset.dataset_id}")


def delete_table(table_id):
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)


def store_locally(df: pd.DataFrame, name: str) -> None:
    """
    Store a dataframe locally.
    """
    date_str = datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = f"{name}_{date_str}.csv"
    file_path = os.path.join(LOCAL_DATA_PATH, file_name)
    df.to_csv(file_path, index=False)
    print(f" # [GCP] {file_path} written!")


def load_locally(name: str) -> pd.DataFrame:
    """
    Load a dataframe locally.
    """
    file_path_ext = f"{name}_*.csv"
    file_paths = os.path.join(LOCAL_DATA_PATH, file_path_ext)
    # get the last file stored using the date in its name
    file_path = sorted(glob(file_paths))[0]
    return pd.read_csv(file_path)
