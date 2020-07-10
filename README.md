# Flights DB

The scope of this repo is to construct a pipeline (in airflow) that creates a DB (on a redshift cluster) containing information about US flights and strikes in 2019, to perform monthly analyses on  correlation between flight delays and strikes.

Using airflow allows to automatise the pipeline. As it is, the pipeline is executed monthly. Simple changes in the configuration would allow to execute it on a daily basis.

## Dataset
Data on flights are obtained from the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time). Data on strikes are obtained from the [Bureau of Labor Statistics](https://beta.bls.gov/labs/). Both these websites allow to download data for a specific year and month, and to select the required information. 

Both websites provide a .csv file that is stored in the s3 bucket ```s3://demographics-udacity/```, in the folders ```airports/``` and ```strikes``` respectively. Each file in the ```airports``` folder correspond to a month. E.g. ```s3://demographics-udacity/airports/1.csv``` corresponds to January 2019.

## Files 
The following files are needed for execution:
1. ```airflow/dags/capstone.py``` defines the DAG and the pipeline. Relies on the operators defined in the ```operators``` folder
2. ```airflow/plugins/operators``` folder containing the operators used in the DAG:
-  ```stage_redshift.py``` stages the data from S3  into redshift
- ```data_quality.py``` runs the data check query passed as argument and compares its record with the expected one. If they differ, it raises an error
3. ```airflow/plugins/helpers/sql_queries.py``` contains the SQL queries loaded in ```dags/capstone.py``` and used for deleting, creating and filling the fact and dimension tables.


## Database Schema
The data on flights are stored in a Database organised in a star schema, in which however not all of the periferic tables are dimension tables. This schema is optimized on the queries of the analytics department, which are mostly focused on delays depending on airports and carriers. The database contains the following tables
#### Fact Tables 
1. **flights** 
- columns:     ID_KEY (PRIMARY KEY),FL_DATE, OP_UNIQUE_CARRIER,TAIL_NUM,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID,DEST_AIRPORT_ID,CANCELLED,CANCELLATION_CODE,DIVERTED,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY


#### Dimenson Tables
1. **airports** 
 -columns:AIRPORT_ID (PRIMARY KEY),AIRPORT_SEQ_ID,CITY_MARKET_ID,AIRPORT,CITY_NAME, STATE_ABR,STATE_FIPS,STATE_NM,WAC

2. **airlines** 
- columns: OP_UNIQUE_CARRIER (PRIMARY KEY), OP_CARRIER_AIRLINE_ID,OP_CARRIER               
3. **dates**
- columns: FL_DATE (PRIMARY KEY),FL_YEAR,FL_QUARTER,FL_MONTH,DAY_OF_MONTH,DAY_OF_WEEK

#### Other 
1. **dep_perfs**
- columns: ID_KEY (PRIMARY KEY),CRS_DEP_TIME,DEP_TIME,DEP_DELAY,DEP_DELAY_NEW,DEP_DEL15,DEP_DELAY_GROUP,DEP_TIME_BLK,TAXI_OUT,WHEELS_OFF
2. **arr_perfs**
- columns: ID_KEY (PRIMARY KEY),WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,ARR_DELAY_NEW,ARR_DEL15,ARR_DELAY_GROUP,ARR_TIME_BLK   
3. **summaries**
- columns: ID_KEY (PRIMARY KEY),CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,FLIGHTS,DISTANCE,DISTANCE_GROUP
4. **gate_info**
- columns: ID_KEY (PRIMARY KEY),FIRST_DEP_TIME,TOTAL_ADD_GTIME,LONGEST_ADD_GTIME
5.**diversions**
- columns: all the remaining columns in the .csv files

The Entity Relation Diagram is as follows
![alt text](./figures/star_schema.png)

The diagram is generated using [Visual Paradigm](https://online.visual-paradigm.com/diagrams/features/erd-tool/). Primary keys are in bold font. I did not manage to do-undo italics to distinguish numerical entries...

The data on strikes are stored in an additional table named **strikes** 
- columns: strike_year, strike_month, label (PRIMARY KEY), strike_value, month_net
Strike value represent the thousands of workers  off work for strike in that month, and month_net the same value with respect to the previous month.

## ETL Pipeline
The pipeline is sketched as follows:
![alt text](./figures/pipeline.png)


For both flights and strikes, data are loaded from the ```.csv``` files into a staging table containing all the information. For flights, an identifier key for each flight is created by appending the month to the number of the entry in the ```.csv``` file. E.g. the 42nd flight in May will be uniquely identified by the key ```5_42```. 

Data from the staging table is then divided into the respective tables of the schema.

Finally, a check is ran on each table of the DB, verifying that there are no entries with missing key value.

## DAG configuration
The DAG is configured such that
- it does not have dependencies on past runs
- on failure, the tasks are retried 3 times
- retries happen every 5 minutes
- catchup is turned off
- it does not email on retry
## Test query
To test that the DB was created correctly, and that we can simply merge data on flight and strikes, we can run the following query
```
select count(*), s.strike_year, s.strike_month, d.fl_quarter
from strikes s
join dates as d 
on s.strike_year = d.fl_year and s.strike_month = d.fl_month
group by strike_year, strike_month, fl_quarter
order by strike_month
```
which should return
| count         | strike_year       | strike_month | fl_quarter | 
| ------------- |:-------------:| :-------------:| :-------------:| 
| 583985| 2019| 1| 1|
| 533175| 2019| 2| 1|
| 632074| 2019| 3| 1| 
| 612023| 2019| 4| 2| 
| 636390| 2019| 5| 2| 
| 636691| 2019| 6| 2| 
| 659029| 2019| 7| 3| 
| 658461| 2019| 8| 3| 
| 605979| 2019| 9| 3| 
| 636014| 2019| 10| 4| 
| 602453| 2019| 11| 4| 
| 625763| 2019| 12| 4|
