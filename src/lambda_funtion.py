import boto3
import awswrangler as wr
import os
from urllib.parse import unquote_plus

def lambda_handler(event, context):

    CLEAN_ZONE_BUCKET_NAME = os.environ.get('CLEAN_ZONE_BUCKET_NAME', 'clean-zone-bucket')

    # get the source and object name
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

    
    # get the database and table name
    key_list = key.split("/")
    print(f'key_list: {key_list}')
    db_name = key_list[len(key_list)-3]
    table_name = key_list[len(key_list)-2]

    # debug
    print(f'Bucket: {bucket}')
    print(f'Key: {key}')
    print(f'Database Name: {db_name}')
    print(f'Table Name: {table_name}')

    # create clean-zone db and table path
    input_path = f"s3://{bucket}/{key}"
    print(f'Input Path: {input_path}')
    output_path = f"s3://{CLEAN_ZONE_BUCKET_NAME}/{db_name}/{table_name}"
    print(f"Output Path: {output_path}")

    input_df = wr.s3.read_csv([input_path])

    current_databases = wr.catalog.databases()

    if db_name not in current_databases.values:
        print(f'- Database {db_name} does not exist.')
        print('Creating Database..')
        wr.catalog.create_database(db_name)
    else:
        print(f'- Database {db_name} already exists!')

    result = wr.s3.to_parquet(
        df=input_df,
        path=output_path,
        dataset=True,
        database=db_name,
        table=table_name,
        mode="append"
    )

    print("Result: ")
    print(f"{result}")

    return result