import os
import argparse
from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

READ_PATH = os.getenv("READ_PATH")
WRITE_PATH = os.getenv("WRITE_PATH")
TEMP_PATH = os.getenv("TEMP_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL")

parser = argparse.ArgumentParser()
parser.add_argument("--s3file", required=True, help="s3file")
parser.add_argument("--year", help="year.", type=int)
parser.add_argument("--month", help="month.", type=int)
parser.add_argument("--day", help="day.", type=int)

args = parser.parse_args()

if ((args.year == None) or (args.month == None) or (args.day == None)):
  print("Use yesterday as default")
  today = date.today()
  dt = date.today() - timedelta(days = 1)
  YEAR = dt.year
  MONTH = dt.month
  DAY = dt.day 
  print("dt", dt)
else:
  print("Using custom date format provided")
  YEAR = args.year
  MONTH = args.month
  DAY = args.day

S3_FILE = args.s3file
# READ_FULL_PATH = 's3a://%s/%s/%s' % (S3_BUCKET, READ_PATH, S3_FILE)
# WRITE_FULL_PATH = 's3a://%s/%s/%s' % (S3_BUCKET, WRITE_PATH, S3_FILE)
# TEMP_FULL_PATH = 's3a://%s/%s/%s' % (S3_BUCKET, TEMP_PATH, S3_FILE)
READ_FULL_PATH = '%s/%s/%s' % (S3_BUCKET, READ_PATH, S3_FILE)
WRITE_FULL_PATH = '%s/%s/%s' % (S3_BUCKET, WRITE_PATH, S3_FILE)
TEMP_FULL_PATH = '%s/%s/%s' % (S3_BUCKET, TEMP_PATH, S3_FILE)

print("READ_FULL_PATH : ", READ_FULL_PATH)
