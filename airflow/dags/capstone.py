from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'mmosc',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@monthly',
    max_active_runs=1,
    catchup=False
)

# start
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)


drop_staging_strikes = PostgresOperator(
    task_id="Drop_staging_strikes",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_strikes_drop
)

create_staging_strikes = PostgresOperator(
    task_id="Create_staging_strikes",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_strikes
)

stage_strikes = StageToRedshiftOperator(
    task_id='Stage_strikes',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_strikes',
    s3_bucket='demographics-udacity',
    s3_key='strikes',
    copy_options='CSV FILLRECORD IGNOREHEADER 1',
    dag=dag
)

drop_strike = PostgresOperator(
    task_id="Drop_strike",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.strike_table_drop
)

create_strike = PostgresOperator(
    task_id="Create_strike",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.strikes_create
)

insert_strike = PostgresOperator(
    task_id="Fill_strike",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.strike_table_insert
)

drop_staging = PostgresOperator(
    task_id="Drop_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_table_drop
)


create_staging = PostgresOperator(
    task_id="Create_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_table
)

stage_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging',
    s3_bucket='demographics-udacity',
    s3_key='airports',
    copy_options='CSV FILLRECORD IGNOREHEADER 1',
    dag=dag
)

drop_flight = PostgresOperator(
    task_id="Drop_flight",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.flight_table_drop
)

drop_airport = PostgresOperator(
    task_id="Drop_airport",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airport_table_drop
)

drop_airline = PostgresOperator(
    task_id="Drop_airline",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airline_table_drop
)

drop_date = PostgresOperator(
    task_id="Drop_date",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.date_table_drop
)

drop_dep_perf = PostgresOperator(
    task_id="Drop_dep_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.dep_perf_table_drop
)

drop_arr_perf = PostgresOperator(
    task_id="Drop_arr_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.arr_perf_table_drop
)

drop_summary = PostgresOperator(
    task_id="Drop_summary",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.summary_table_drop
)

drop_gate_info = PostgresOperator(
    task_id="Drop_gate_info",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.gate_info_table_drop
)

drop_diversion = PostgresOperator(
    task_id="Drop_diversion",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.diversion_table_drop
)


create_flight = PostgresOperator(
    task_id="Create_flight",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.flight_table_create
)

create_airport = PostgresOperator(
    task_id="Create_airport",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airport_table_create
)

create_airline = PostgresOperator(
    task_id="Create_airline",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airline_table_create
)

create_date = PostgresOperator(
    task_id="Create_date",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.date_table_create
)

create_dep_perf = PostgresOperator(
    task_id="Create_dep_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.dep_perf_table_create
)

create_arr_perf = PostgresOperator(
    task_id="Create_arr_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.arr_perf_table_create
)

create_summary = PostgresOperator(
    task_id="Create_summary",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.summary_table_create
)

create_gate_info = PostgresOperator(
    task_id="Create_gate_info",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.gate_info_table_create
)

create_diversion = PostgresOperator(
    task_id="Create_diversion",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.diversion_table_create
)

insert_flight = PostgresOperator(
    task_id="Fill_flight",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.flight_table_insert
)

insert_airport_orig = PostgresOperator(
    task_id="Insert_airport_orig",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airport_table_insert_orig
)

insert_airport_dest = PostgresOperator(
    task_id="Insert_airport_dest",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airport_table_insert_dest
)

insert_airline = PostgresOperator(
    task_id="Insert_airline",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.airline_table_insert
)

insert_date = PostgresOperator(
    task_id="Insert_date",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.date_table_insert
)

insert_dep_perf = PostgresOperator(
    task_id="Insert_dep_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.dep_perf_table_insert
)

insert_arr_perf = PostgresOperator(
    task_id="Insert_arr_perf",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.arr_perf_table_insert
)

insert_summary = PostgresOperator(
    task_id="Insert_summary",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.summary_table_insert
)

insert_gate_info = PostgresOperator(
    task_id="Insert_gate_info",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.gate_info_table_insert
)

insert_diversion = PostgresOperator(
    task_id="Insert_diversion",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.diversion_table_insert
)


strikes_check = DataQualityOperator(
    task_id='Strikes_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM strikes WHERE label IS NULL',
    expected_result=0    
)


flights_check = DataQualityOperator(
    task_id='Flights_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM flights WHERE ID_KEY IS NULL',
    expected_result=0    
)



airports_check = DataQualityOperator(
    task_id='Airports_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM airports WHERE AIRPORT_ID IS NULL',
    expected_result=0    
)


airlines_check = DataQualityOperator(
    task_id='Airlines_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM airlines WHERE OP_UNIQUE_CARRIER IS NULL',
    expected_result=0    
)

dates_check = DataQualityOperator(
    task_id='Dates_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM dates WHERE FL_DATE IS NULL',
    expected_result=0    
)

dep_perfs_check = DataQualityOperator(
    task_id='Dep_perfs_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM dep_perfs WHERE ID_KEY IS NULL',
    expected_result=0    
)

arr_perfs_check = DataQualityOperator(
    task_id='Arr_perfs_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM arr_perfs WHERE ID_KEY IS NULL',
    expected_result=0    
)

summary_check = DataQualityOperator(
    task_id='Summary_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM summaries WHERE ID_KEY IS NULL',
    expected_result=0    
)

gate_info_check = DataQualityOperator(
    task_id='Gate_info_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM gate_info WHERE ID_KEY IS NULL',
    expected_result=0    
)

diversions_check = DataQualityOperator(
    task_id='Diversion_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM diversions WHERE ID_KEY IS NULL',
    expected_result=0    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



#PIPELINE
start_operator >> drop_staging
drop_staging >> create_staging


start_operator >> drop_staging_strikes
drop_staging_strikes >> create_staging_strikes

create_staging >> stage_to_redshift
create_staging_strikes >> stage_strikes

stage_strikes >> drop_strike
stage_to_redshift >> drop_flight
stage_to_redshift >> drop_airport
stage_to_redshift >> drop_airline
stage_to_redshift >> drop_date
stage_to_redshift >> drop_dep_perf
stage_to_redshift >> drop_arr_perf
stage_to_redshift >> drop_summary
stage_to_redshift >> drop_gate_info
stage_to_redshift >> drop_diversion

drop_strike >> create_strike
drop_flight >> create_flight
drop_airport >> create_airport
drop_airline >> create_airline
drop_date >> create_date
drop_dep_perf >> create_dep_perf
drop_arr_perf >> create_arr_perf
drop_summary >> create_summary
drop_gate_info >> create_gate_info
drop_diversion >> create_diversion

create_strike >> insert_strike
create_flight >> insert_flight
insert_flight >> flights_check
flights_check >> end_operator

create_airport >>insert_airport_orig
create_airport >>insert_airport_dest
insert_airport_orig >> airports_check
insert_airport_dest >> airports_check
airports_check >> end_operator

create_airline >> insert_airline
insert_airline >> airlines_check
airlines_check >> end_operator

create_date >> insert_date
insert_date >> dates_check
dates_check >> end_operator

create_dep_perf >> insert_dep_perf
insert_dep_perf >> dep_perfs_check
dep_perfs_check >> end_operator

create_arr_perf >> insert_arr_perf
insert_arr_perf >> arr_perfs_check
arr_perfs_check >> end_operator

create_summary >> insert_summary
insert_summary >> summary_check
summary_check >> end_operator

create_gate_info >> insert_gate_info
insert_gate_info >> gate_info_check
gate_info_check >> end_operator

create_diversion >> insert_diversion
insert_diversion >> diversions_check
diversions_check >> end_operator

insert_strike >> strikes_check
strikes_check >> end_operator