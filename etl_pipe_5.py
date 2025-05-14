#импортируем библиотеки
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#настраиваем соединение
connection = {
    'host': 'HOST',
    'password': 'PASSWORD',
    'user': 'USER',
    'database': 'DATABASE'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n-karimov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 24)
}

#интервал выполнения дагов
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def nikitakarimov_dag_etl():
    
    @task()
    def extract_one():
        query = """select user_id,
                            toDate(time) event_date,
                            countIf(action == 'like') likes,
                            countIf(action == 'view') views,
                            os,
                            gender,
                            age
            from simulator_20250220.feed_actions
            WHERE toDate(time) = yesterday()
            group by user_id, os, gender, age, event_date """
        df_one = ph.read_clickhouse(query, connection=connection)
        return df_one
    
    @task()
    def extract_two():
        query_2 = """select user_id, messages_sent, users_sent, messages_received, users_received
                from
                (select user_id,
                    Count(receiver_id) messages_sent,
                    count(Distinct receiver_id) users_sent
                from simulator_20250220.message_actions
                WHERE toDate(time) = yesterday()
                group by user_id) t1
                full outer join
                (select receiver_id,
                    count(user_id) messages_received,
                    count(Distinct user_id) as users_received
                from simulator_20250220.message_actions
                WHERE toDate(time) = yesterday()
                group by receiver_id) t2 on t1.user_id = t2.receiver_id"""
        df_two = ph.read_clickhouse(query_2, connection=connection)
        return df_two
        
    @task
    def transfrom_merge_tables(df_one, df_two):
        df = df_one.merge(df_two, how='left', on = ['user_id']).fillna(0)
        return df

    @task
    def transfrom_os_metrics(df):
        df_os = df.groupby('os').agg({'likes': 'sum', 'views': 'sum', 'messages_sent': 'sum', 'users_sent': 'sum', 'messages_received': 'sum', 'users_received': 'sum'  }).reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        return df_os    
    
    @task
    def transfrom_gender_metrics(df):
        df_gender = df.groupby('gender').agg({'likes': 'sum', 'views': 'sum', 'messages_sent': 'sum', 'users_sent': 'sum', 'messages_received': 'sum', 'users_received': 'sum'  }).reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        return df_gender    

    @task
    def transfrom_age_metrics(df):
        df_age = df.groupby('age').agg({'likes':'sum','views':'sum','messages_sent':'sum','users_sent':'sum','messages_received': 'sum', 'users_received': 'sum'  }).reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        return df_age
    
    @task
    def transfrom_final_table(df_gender, df_age, df_os, df):
        df=df.copy()
        final_table = pd.concat([df_gender, df_age, df_os], ignore_index=True)
        final_table['event_date'] = df['event_date']
        final_table = final_table.astype({'dimension_value': 'str','dimension': 'str','likes': 'float64', 'views':'float64', 'messages_sent': 'float64', 'users_sent': 'float64', 'messages_received': 'float64', 'users_received': 'float64',  'event_date': 'datetime64[ns]'})
        return final_table

    @task
    def load_to_clickhouse(final_table, table_name):
        clickhouse_test_connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'database': 'test',
            'user': 'student-rw',
            'password': '656e2b0c9c'
        }
        create_table_query ='''CREATE TABLE IF NOT EXISTS test.karimov(
              event_date DateTime,
              dimension String,
              dimension_value String,
              views Float64,
              likes Float64,
              messages_received Float64,
              messages_sent Float64,
              users_received Float64,
              users_sent Float64
            ) ENGINE = MergeTree()
            ORDER BY (event_date, dimension, dimension_value);
            '''
        ph.execute(create_table_query, clickhouse_test_connection)
        ph.to_clickhouse(df=final_table, table="karimov", connection=clickhouse_test_connection, index=False)

    df_one = extract_one()
    df_two = extract_two()
    
    df = transfrom_merge_tables(df_one, df_two)
    
    df_os = transfrom_os_metrics(df)
    df_gender = transfrom_gender_metrics(df)
    df_age = transfrom_age_metrics(df)
    
    final_table = transfrom_final_table(df_os, df_gender, df_age, df)
    load_to_clickhouse(final_table, 'karimov')

nikitakarimov_dag_etl = nikitakarimov_dag_etl()