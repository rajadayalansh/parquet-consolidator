# parquet-consolidator
Parquet Consolidator batch job to coalesce parquet files at end of the day

install dependencies: pipenv install

#consolidate files for previous day
python src/parquet-consolidator.py --s3file [s3filename]

#consolidate files for custom date
python src/parquet-consolidator.py --s3file [s3filename] --year [year] --month [month] --day [day]

Example parquet-consolidator call
python src/parquet-consolidator.py --s3file ercot_nodal_system_parameters
python src/parquet-consolidator.py --s3file ercot_nodal_system_parameters --year 2020 --month 3 --day 23