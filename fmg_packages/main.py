import datetime
from google.cloud import bigquery, storage
from google.cloud.storage.constants import PUBLIC_ACCESS_PREVENTION_ENFORCED
from fmg_packages.queries.results_query import results_query
from fmg_packages.utils.constants import BUCKET_NAME, TARGET_PROJECT_ID, EXECUTION_TS_KEY, \
    DAG_RUN_KEY, TS_NODASH_FORMAT, CONF_REPORT_DATE_KEY, TARGET_SAMPLE_TABLE, TARGET_CLEANSED_TABLE, \
    TARGET_RESULTS_TABLE, DATASET_NAME, RESULTS_BUCKET, TS_DATE_FORMAT, TS_RESULTS_FORMAT, TS_STORAGE_FORMAT
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "../fmg_packages/utils/fortuneminegames-a5c7f46dd8d5.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
storage_client = storage.Client(credentials=credentials, project=TARGET_PROJECT_ID)
bigquery_client = bigquery.Client(credentials=credentials, project=TARGET_PROJECT_ID)

def is_exist_table(table_id):
    try:
        bigquery_client.get_table(table_id)
        return True
    except:
        return False

def create_bucket(bucket_name):

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "ARCHIVE"
    bucket.iam_configuration.public_access_prevention = (
        PUBLIC_ACCESS_PREVENTION_ENFORCED
    )
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket = storage_client.create_bucket(bucket, location="us-central1")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket


def upload_file(bucket_name, source_file_name, destination_blob_name):

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def get_files(bucket_name, file_extension=None, create_date_utc_filter=None):

    file_list = []
    for blob in storage_client.list_blobs(bucket_or_name=bucket_name):
        if file_extension:
            if str.endswith(blob.name, file_extension):
                if create_date_utc_filter is not None and (blob.time_created - create_date_utc_filter).total_seconds() > 0:
                    file_list.append(blob)
        else:
            if create_date_utc_filter is not None and (blob.time_created - create_date_utc_filter).total_seconds() > 0:
                file_list.append(blob)
    return file_list

def read_report_date_from_conf(context, report_date):
    conf_params = context.get(DAG_RUN_KEY)
    conf_params = conf_params.conf if conf_params else {}
    print(f'Configuration params={conf_params}')
    conf_local_date = conf_params.get(CONF_REPORT_DATE_KEY, None) if conf_params else None
    return conf_local_date if conf_local_date else report_date


def pull_data(**kwargs):
    report_ts = kwargs.get(EXECUTION_TS_KEY)
    report_date = report_ts.strftime(TS_NODASH_FORMAT) if report_ts is not  None else None
    start_date = read_report_date_from_conf(context=kwargs, report_date=report_date)
    start_date = datetime.strptime(start_date, TS_NODASH_FORMAT)
    pull_data_from_storage(start_date)


def pull_data_from_storage(start_date):
    start_date = start_date.astimezone(timezone.utc)
    data = get_files(bucket_name=BUCKET_NAME, create_date_utc_filter=start_date, file_extension='csv')
    for item in data:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        uri = 'gs://' + BUCKET_NAME + '/' + item.name
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("event_name", "STRING"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("user_pseudo_id", "STRING"),
                bigquery.SchemaField("operating_system", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("version", "STRING")
            ],
            write_disposition=write_disposition,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_timestamp",
                require_partition_filter=True,

            ),
            clustering_fields=["country", "version"]
        )
        load_job = bigquery_client.load_table_from_uri(
            uri, TARGET_SAMPLE_TABLE, job_config=job_config
        )
        load_job.result()


def cleanse_data(**kwargs):
    if is_exist_table(TARGET_CLEANSED_TABLE):
        report_ts = kwargs.get(EXECUTION_TS_KEY)
        report_date = report_ts.strftime(TS_NODASH_FORMAT) if report_ts is not None else None
        start_date = read_report_date_from_conf(context=kwargs, report_date=report_date)
        start_date = datetime.strptime(start_date, TS_NODASH_FORMAT).strftime(TS_DATE_FORMAT)
        cleanse_query = """Merge {TARGET_CLEANSED_TABLE} AS old_data
                USING (
                select
                * except(row_number)
              from (
                select
                  *, row_number() over (partition by event_name,  event_timestamp, user_pseudo_id) as row_number
                from {TARGET_SAMPLE_TABLE} e
                where DATE(event_timestamp) > "{CURRENT_DATE}"
                ) 
              where row_number = 1
              INTERSECT DISTINCT
              SELECT * FROM {TARGET_SAMPLE_TABLE} WHERE DATE(event_timestamp) = "{CURRENT_DATE}"
               and
              (user_pseudo_id is not Null and event_name is not Null and country is not Null and 
              event_timestamp is not Null and version is not Null and operating_system is not    Null)
                ) AS new_data
                ON    
               old_data.event_name = new_data.event_name
               AND old_data.event_timestamp = new_data.event_timestamp
               AND old_data.user_pseudo_id = new_data.user_pseudo_id
                WHEN NOT MATCHED THEN
                                INSERT (event_name, event_timestamp, user_pseudo_id, operating_system, country, version)
                                VALUES (event_name, event_timestamp, user_pseudo_id, operating_system, country, version)""" \
            .format(TARGET_CLEANSED_TABLE=TARGET_CLEANSED_TABLE, CURRENT_DATE=start_date, TARGET_SAMPLE_TABLE=TARGET_SAMPLE_TABLE)
        bigquery_client.query(cleanse_query)

def resulting_data(**kwargs):
    if is_exist_table(TARGET_CLEANSED_TABLE):
        report_ts = kwargs.get(EXECUTION_TS_KEY)
        report_date = report_ts.strftime(TS_NODASH_FORMAT) if report_ts is not None else None
        start_date = read_report_date_from_conf(context=kwargs, report_date=report_date)
        start_date = datetime.strptime(start_date, TS_NODASH_FORMAT).strftime(TS_RESULTS_FORMAT)
        job_config = bigquery.QueryJobConfig(
            destination=TARGET_RESULTS_TABLE + start_date,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        xx = results_query
        query_job = bigquery_client.query(
            results_query,
            job_config=job_config,
        )
        query_job.result()

def extract_results(**kwargs):
    report_ts = kwargs.get(EXECUTION_TS_KEY)
    report_date = report_ts.strftime(TS_NODASH_FORMAT) if report_ts is not None else None
    start_date = read_report_date_from_conf(context=kwargs, report_date=report_date)
    storage_date = datetime.strptime(start_date, TS_NODASH_FORMAT).strftime(TS_STORAGE_FORMAT)
    results_date = datetime.strptime(start_date, TS_NODASH_FORMAT).strftime(TS_RESULTS_FORMAT)
    destination_uri = "gs://{}/{}".format(RESULTS_BUCKET, storage_date + "/results" + ".csv")
    dataset_ref = bigquery.DatasetReference(TARGET_PROJECT_ID, DATASET_NAME)
    table_ref = dataset_ref.table("results_" + results_date)

    extract_job = bigquery_client.extract_table(
        table_ref,
        destination_uri,
        location="us-central1",
    )
    extract_job.result()
    print(
        "Exported {}:{} to {}".format(TARGET_PROJECT_ID, DATASET_NAME, destination_uri)
    )