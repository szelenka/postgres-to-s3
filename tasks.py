from typing import List, Dict, Any, Union
from pathlib import Path
import os
from collections import namedtuple, defaultdict
from tempfile import NamedTemporaryFile, mkdtemp
from shutil import rmtree

from prefect import task
from prefect.triggers import always_run
from prefect.utilities.logging import get_logger

import requests
import numpy as np
import pandas as pd
import dask.dataframe as dd
from sqlalchemy import create_engine, engine
from s3fs import S3FileSystem


table_batch = namedtuple(typename='table_batch', field_names='table_name,data_types,offset,limit,folder')
table_data = namedtuple(typename='table_data', field_names='table_name,filename')


def download_rds_cert(url: str = 'https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem') -> os.PathLike:
    # download the most recent cert from AWS
    # ref: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html
    filename = Path('amazon-rds-ca-cert.pem')
    if filename.exists() and filename.stat().st_size > 0:
        return filename

    response = requests.get(url)
    try:
        with filename.open(mode='wb') as f:
            f.write(response.content)
        assert filename.exists() and filename.stat().st_size > 0, f'Unable to locate filename_list inside: {filename.absolute()}'
    except FileNotFoundError:
        print('FileNotFoundError: will attempt to create')
        os.makedirs(filename.absolute().parents[0])
        return download_rds_cert(url=url)

    return filename


def get_rds_engine(secrets: Dict[str, str]) -> engine:
    database_cert = download_rds_cert()
    eng = create_engine(f'postgres://{secrets["database_user"]}:{secrets["database_pass"]}@{secrets["database_host"]}:{secrets["database_port"]}/{secrets["database_name"]}?sslrootcert={database_cert}')
    return eng


def get_table_data_types(
        table: str,
        secrets: Dict[str, str],
) -> Dict[str, Any]:
    # ensure Pandas has the correct filename_list-type as SQL
    stmt = """SELECT 
    column_name,
    is_nullable,
    data_type,
    udt_name
FROM information_schema.columns
WHERE 
    table_catalog = %(database)s
    AND table_schema = %(schema)s
    AND table_name = %(table_name)s
ORDER BY ordinal_position ASC"""
    get_logger().debug(stmt)
    dt = pd.read_sql(sql=stmt, con=get_rds_engine(secrets=secrets), params=dict(
        database=secrets['database_name'],
        schema=secrets['database_schema'],
        table=table
    ))
    dt['is_nullable'] = dt['is_nullable'].apply(lambda _: _ == 'YES')
    get_logger().info(f"Discovered {len(dt)} columns for table_name: {table}")

    mapper = dict()
    # we need to convert datatype to something that can hold NaN (i.e. Floats)
    for idx, row in dt.iterrows():
        if row.data_type == 'bigint':
            mapper[row.column_name] = np.float64 if row.is_nullable else np.int64
        elif row.data_type == 'integer':
            mapper[row.column_name] = np.float32 if row.is_nullable else np.int32
        elif row.data_type == 'smallint':
            mapper[row.column_name] = np.float32 if row.is_nullable else np.int16
        elif row.data_type == 'boolean':
            mapper[row.column_name] = 'bool'
        elif row.data_type in ['double precision', 'numeric']:
            mapper[row.column_name] = np.float64
        elif row.udt_name in ['timestamp', 'date']:
            mapper[row.column_name] = 'datetime64[ms]'
        elif row.udt_name in ['varchar', 'text', 'json', 'jsonb']:
            mapper[row.column_name] = str
        else:
            raise RuntimeError(f'Unknown data_type: {row.data_type}')

    return mapper


@task
def create_data_partitions(
        table_name: str,
        first_index: int,
        last_index: int,
        secrets: Dict[str, str],
        num_of_records_in_batch: int
) -> List[table_batch]:
    """Create partition for table_name based on number of days in the date range"""
    dt = get_table_data_types(table=table_name, secrets=secrets)

    stmt = f"""SELECT reltuples::BIGINT AS estimate 
FROM pg_class 
WHERE relname=%(table_name)s 
ORDER BY reltuples DESC
LIMIT 1"""
    get_logger().debug(stmt)
    df = pd.read_sql(sql=stmt, con=get_rds_engine(secrets=secrets), params=dict(table=table_name))
    row_estimate = df.iloc[0]['estimate'] + num_of_records_in_batch
    get_logger().info(f"Row estimate for table_name: {table_name} {row_estimate}")
    rows_to_pull = last_index if 0 < last_index < row_estimate else row_estimate
    assert first_index < row_estimate, f'starting_index: {first_index} is greater-than rows_to_pull: {rows_to_pull}'
    batch_size = last_index - first_index if 0 < last_index < num_of_records_in_batch else num_of_records_in_batch
    # TODO: validate batch size

    directory = Path(mkdtemp(suffix=f"_{table_name}"))
    table_partitions = [
        table_batch(
            table_name,
            dt,
            i,
            batch_size,
            directory
        ) for i in range(first_index, rows_to_pull, batch_size)
    ]

    get_logger().info(f"Created {len(table_partitions)} partitions from table_name: {table_name}")

    return table_partitions


@task
def flatten_nested_list(
        nested_list: List[List[Union[table_batch, table_data]]]
) -> List[Union[table_batch, table_data]]:
    results = [item for sublist in nested_list for item in sublist]
    get_logger().info(f"Discovered: {len(results)} total records from nested_list: {len(nested_list)}")
    return results


@task
def get_data_from_sql(
        partition: table_batch,
        index: str,
        secrets: Dict[str, str]
) -> Union[table_data, None]:
    stmt = f"""SELECT *
FROM "{secrets['database_schema']}"."{partition.table_name}" 
ORDER BY "{index}" ASC
OFFSET %(offset)s
LIMIT %(limit)s"""
    get_logger().debug(stmt)
    df = pd.read_sql(sql=stmt, con=get_rds_engine(secrets=secrets), params=dict(
        offset=partition.offset,
        limit=partition.limit
    ))

    if len(df) == 0:
        get_logger().warning(f'Unable to locate any filename_list in table_name: {partition.table_name} OFFSET: {partition.offset} LIMIT: {partition.limit}')
        return None

    assert len(df.columns) == len(partition.data_types), f'Mismatched columns in table_name: {partition.table_name}'

    # ensure we're using the approprite date-types for parquet
    # this is especially important for columns which allow NULL in Postgres, and NaN in Pandas
    for c in df.columns:
        assert c in partition.data_types, f"Missing data_type for query: {c}"
        df[c] = df[c].astype(partition.data_types[c])

    # create the placeholders for partitioning the parquet file later on
    assert 'created_at' in df.columns, f'Missing column "created_at" in table_name: {partition.table_name}'
    df['year'] = df['created_at'].dt.year.astype(np.int16)
    df['month'] = df['created_at'].dt.month.astype(np.int8)
    df['day'] = df['created_at'].dt.day.astype(np.int8)

    ntf = NamedTemporaryFile(
        prefix=f"{partition.table_name}_",
        suffix='.parquet',
        delete=False
    )
    filename = Path(ntf.name)
    df.to_parquet(
        path=filename,
        index=False
    )

    return table_data(partition.table_name, filename)


@task
def group_data_partitions_by_table_name(
        data: List[table_data]
) -> List[List[table_data]]:
    obj = defaultdict(list)
    for _ in data:
        if _ is None:
            continue
        obj[_.table_name].append(_)

    return list(obj.values())


def get_s3_connection(secrets: Dict[str, str]) -> S3FileSystem:
    # ref: https://s3fs.readthedocs.io/en/latest/#credentials
    conn = S3FileSystem(
        anon=False,
        key=secrets['s3_access_key'],
        secret=secrets['s3_secret_key'],
        use_ssl=True,
        client_kwargs=dict(
            endpoint_url=secrets['s3_server'],
        )
    )

    # verify the bucket exists
    if not conn.exists(secrets['s3_bucket']):
        get_logger().warning(f"Unable to lcoate bucket, will attempt to create it now: {secrets['s3_bucket']}")
        conn.mkdirs(secrets['s3_bucket'])

    return conn


def pandas_to_local_parquet(
        directory: os.PathLike,
        data: table_data,
        num_of_records_in_batch: int,
        append: bool = True,
        idx: str = ''
) -> int:
    df = pd.read_parquet(path=data.filename)
    if len(df) == 0:
        get_logger().warning(f"Unable to locate any filename_list in file: {data.filename}")
        return 0

    partition_cols = ['year', 'month', 'day']
    for p in partition_cols:
        assert p in df.columns, f'Missing column "{p}", cannot continue with S3 upload'

    get_logger().info(f"[{idx}] Attempting to prepare {len(df)} records, {data.filename.stat().st_size/(1024*1024)} MB from file: {data.filename}")
    df.to_parquet(
        # path=f"s3://{path}",
        path=directory,
        engine='pyarrow',
        partition_cols=partition_cols,
        index=False,
        allow_truncated_timestamps=True,
        # flavor='spark',
        # filesystem=s3
    )

    # use Dask to write the _metadata and _common_metadata files
    # write to local disk first, then use aws cli to sync the filename to s3
    # TODO: ref https://github.com/dask/dask/issues/6867
    # dd.from_pandas(
    #     data=df,
    #     chunksize=num_of_records_in_batch
    # ).to_parquet(
    #     # path=f"s3://{path}",
    #     path=directory,
    #     append=append,
    #     engine='pyarrow',
    #     partition_on=partition_cols,
    #     ignore_divisions=True,
    #     # storage_options=dict(
    #     #     anon=False,
    #     #     key=secrets['s3_access_key'],
    #     #     secret=secrets['s3_secret_key'],
    #     #     use_ssl=True,
    #     #     client_kwargs=dict(
    #     #         endpoint_url=secrets['s3_server'],
    #     #     )
    #     # ),
    #     write_index=False
    # )
    # return len(df)


@task
def prepare_table_data_for_parquet_directory(
        grouped_table_data: List[table_data],
        first_index: int,
        num_of_records_in_batch: int,
        secrets: Dict[str, str]
) -> Union[List[table_data], None]:
    total_records = 0
    if len(grouped_table_data) == 0:
        get_logger().warning('Unable to locate any data to insert...')
        return None

    table_name = grouped_table_data[-1].table_name
    total_partitions = len(grouped_table_data)
    directory = Path(mkdtemp(suffix=f"_{table_name}"))

    # if this is an incremental load, we should download the _metadata files from s3 locally
    # such that the appends will update that correctly
    # if first_index != 0:
    #     s3 = get_s3_connection(secrets=secrets)
    #     for filename in ['_metadata', '_common_metadata']:
    #         # _metadata must always exist locallay
    #         path = Path(secrets['s3_bucket'] + s3.sep + table_name + s3.sep + filename)
    #         s3.get(rpath=path.as_posix(), lpath=(directory / filename).as_posix())

    for i, data in enumerate(grouped_table_data):
        if data is None:
            get_logger().debug("Unable to locate a filename_list object to attempt an insert")
            continue

        # the first record needs to re-create the parquet filename if starting_index == 0 and i == 0
        # everything else should append to that parquet filename one-at-a-time
        recreate_metadata = (first_index == 0) and (i == 0)
        size = pandas_to_local_parquet(
            directory=directory,
            data=data,
            num_of_records_in_batch=num_of_records_in_batch,
            append=not recreate_metadata,
            idx=f"{i+1:05}/{total_partitions:05}"
        )
        if size == 0:
            get_logger().warning(f"Unable to upload contents of file: {data.filename}")

        total_records += size

    get_logger().info(f"Prepared: {total_records} from table_name: {table_name}")

    # return list of all files under this directory
    results = [table_data(table_name, _) for _ in directory.glob('**/*') if _.is_file()]
    # assert sorted(results)[0].filename.as_posix().endswith('_metadata'), 'Missing _metadata file in directory!'
    get_logger().info(f"Discovered {len(results)} parquet files to upload for {table_name}")
    return results


@task(
    trigger=always_run
)
def purge_transient_files(filename_list: List[table_data]) -> None:
    for data in filename_list:
        if data is None:
            continue
        data.filename.unlink(missing_ok=True)


def get_parent_folder_name(data: table_data) -> Path:
    directory = None
    parts = data.filename.parts
    for idx, part in enumerate(parts):
        if part.endswith(f"_{data.table_name}"):
            directory = '/'.join(parts[:idx + 1])
            if directory.startswith('//'):
                directory = directory[1:]
            directory = Path(directory)

    assert directory is not None, f"Unable to locate directory with: {data.table_name}"
    return directory


@task(
    trigger=always_run
)
def purge_transient_folders(filename_list: List[List[table_data]]) -> bool:
    data = filename_list[-1]
    if data is None:
        get_logger().warning(f"Unable to locate any folders to purge")
        return False

    directory = get_parent_folder_name(data=data)
    get_logger().info(f"Purging directory: {directory}")
    rmtree(directory)
    return True


@task
def identify_s3_files_to_purge(
        table_name: str,
        first_index: int,
        secrets: Dict[str, str]
) -> Union[List[str], None]:
    s3 = get_s3_connection(secrets=secrets)
    path = Path(secrets['s3_bucket'] + s3.sep + table_name).as_posix()

    if first_index > 0:
        get_logger().warning(f"starting_index: {first_index} implies we don't want to purge any existing data from s3 for: {path}")
        return []

    # results = s3.glob(path=path + s3.sep + '**' + s3.sep + '*metadata')
    results = s3.glob(path=path + s3.sep + '**' + s3.sep + '*.parquet')
    get_logger().info(f"Discovered {len(results)} files to purge from {path}")
    return results


@task
def purge_s3_files(
    filename: str,
    secrets: Dict[str, str]
) -> bool:
    s3 = get_s3_connection(secrets=secrets)
    s3.rm(path=filename)
    return True


@task
def sync_with_s3(
        data: table_data,
        secrets: Dict[str, str]
) -> Path:
    s3 = get_s3_connection(secrets=secrets)
    directory = get_parent_folder_name(data=data)
    destination = data.filename.as_posix().split(directory.as_posix())[-1]
    assert len(destination) > 0, f'Unable to determine destination from directory: {directory} filename: {data.filename}'
    path = Path(secrets['s3_bucket'] + s3.sep + data.table_name + s3.sep + destination).as_posix()

    get_logger().debug(f"Upload {data.filename.stat().st_size/(1024*1024)} MB from file: {data.filename} to {path}")
    s3.put(lpath=data.filename.as_posix(), rpath=path)
    return data.filename
