import sys
import boto3
import datetime
import pyarrow
import pyarrow.parquet as pq
import s3fs
import json
from common.settings import *
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)
print("READ_PATH in pqmerge", READ_PATH)

def read_s3_data(s3_path):
    s3 = s3fs.S3FileSystem()

    s3fullpath= "%s/year=%d/month=%d/day=%d" % (s3_path, YEAR, MONTH, DAY)
    logging.info("S3 path with Partition : %s " % s3fullpath)
    if not(s3.exists(s3fullpath)) :
        logging.error("S3 partition does not exist %s " % s3fullpath)
        return None

    dataframe = pq.ParquetDataset(
        s3_path, 
        filters=[('year', '==', YEAR),
                ('month', '==', MONTH),
                ('day', '==', DAY)
                ],
        filesystem=s3
        ).read_pandas().to_pandas()
    return dataframe

def write_s3_data(dataframe, s3_path):
    logging.info('writing object: %s' % s3_path)
    table = pyarrow.Table.from_pandas(dataframe)
    s3 = s3fs.S3FileSystem()
    pcols = ['year', 'month', 'day']
    pq.write_to_dataset(
        table,
        s3_path,
        compression='snappy',
        filesystem=s3,
        partition_cols=pcols,
        # partition_filename_cb=make_filename
    )

def delete_s3_folder(s3_bucket, s3_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    for obj in bucket.objects.filter(Prefix=s3_path):
        s3.Object(bucket.name,obj.key).delete()


if __name__ == "__main__":
    
    source_df = read_s3_data(READ_FULL_PATH)

    if source_df is None :
        logging.error("Partition does not exist so exit")
        sys.exit(1)

    source_df_count = len(source_df)

    if source_df_count == 0:
        logging.error("No records available so exit")
        sys.exit(1)

    logging.debug("no of records in df3 : %d " % source_df_count)
    print(source_df)   

    #write to temp file
    write_s3_data(source_df, TEMP_FULL_PATH)
    logging.info("File written to temp folder %s" % TEMP_FULL_PATH)

    delete_path = "%s/%s/year=%d/month=%d/day=%d" % (READ_PATH, S3_FILE, YEAR, MONTH, DAY)
    # logging.info("Delete partition before writing %s" % delete_path)
    delete_s3_folder(S3_BUCKET, delete_path)

    write_s3_data(source_df, WRITE_FULL_PATH)
    logging.info("File written to %s" % WRITE_FULL_PATH)

    dest_df = read_s3_data(WRITE_FULL_PATH)
    logging.debug("No of records written after merge : %d" % len(dest_df))
    print(dest_df)

    #check records before and after merge
    if len(dest_df) == source_df_count:
        delete_path = "%s/%s/year=%d/month=%d/day=%d" % (TEMP_PATH, S3_FILE, YEAR, MONTH, DAY)
        logging.info("delete_path %s" % delete_path)
        # delete_s3_folder(S3_BUCKET, delete_path)
        logging.info("Parquet Files merge finished")
        sys.exit(0)
    else:
        #If count does not match, there is issue in coalesce. Exit without deleting temp file 
        logging.error("Parquet Files merge failed. Record count before and after merge do not match. %s, %s" % (source_df_count, len(dest_df)))
        sys.exit(1)
