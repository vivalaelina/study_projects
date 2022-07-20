from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task

con_1 = {
    'host': 'host',
    'password': '12345',
    'user': 'user',
    'database': 'simulator'
}

con_2 = {
    'host': 'host_2',
    'password': '12345',
    'user': 'user_2',
    'database': 'test'
}

default_args = {
    'owner': 'e.popova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 5),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_table_epopova():
    
    @task()
    def extract_feed():
        q = '''SELECT toDate(time) as event_date, user_id, if(gender=1,'male','female') as gender, os, 
            CASE WHEN age < 18 THEN '<18'
            WHEN age >= 18 and age <= 30 THEN '18-30'
            WHEN age > 30 and age <= 45 THEN '31-45'
            WHEN age > 45 and age <= 65 THEN '46-65'
            ELSE '>=65' END as age, 
            countIf(action='like') as likes, countIf(action='view') as views
            FROM simulator_20220620.feed_actions 
            WHERE event_date = yesterday()
            GROUP BY event_date, user_id, gender, os, age'''

        df_feed = ph.read_clickhouse(q, connection=con_1)
        return df_feed

    @task()
    def extract_mes():
        q_2 = '''WITH t1 as 
            (SELECT toDate(time) as event_date, user_id, count(user_id) as messages_sent, count(DISTINCT reciever_id) as users_sent,
            if(gender=1,'male','female') as gender, os, 
            CASE WHEN age < 18 THEN '<18'
            WHEN age >= 18 and age <= 30 THEN '18-30'
            WHEN age > 30 and age <= 45 THEN '31-45'
            WHEN age > 45 and age <= 65 THEN '46-65'
            ELSE '>=65' END as age
            FROM simulator_20220620.message_actions  
            WHERE event_date = yesterday()
            GROUP BY event_date, user_id, gender,os,age),
            t2 as 
            (SELECT toDate(time) as event_date, reciever_id, count(reciever_id) as messages_received, count(DISTINCT user_id) as users_received
            FROM simulator_20220620.message_actions  
            WHERE event_date = yesterday()
            GROUP BY event_date, reciever_id)
            SELECT t1.event_date, t1.user_id, t1.gender, t1.os, t1.age, messages_sent, users_sent, messages_received, users_received
            FROM t1 INNER JOIN t2 ON t1.user_id = t2.reciever_id'''
        df_mes = ph.read_clickhouse(q_2, connection=con_1)
        return df_mes
    
    @task()
    def join_tables(df_feed,df_mes):
        df_all = df_feed.merge(df_mes,how='outer', on=['event_date','user_id','gender','os','age']).fillna(0)
        return df_all

    @task()
    def transform_gender(df_all):
        df_gender = df_all[['event_date','gender','likes','views','messages_sent','users_sent','messages_received','users_received']]\
        .groupby(['event_date','gender']).sum().reset_index()
        df_gender['feature'] = 'gender'
        df_gender = df_gender.iloc[:, [0,8,1,2,3,4,5,6,7]]
        df_gender = df_gender.rename(columns={'gender':'feature_value'})

        return df_gender
    
    @task()
    def transform_age(df_all):
        df_age = df_all[['event_date','age','likes','views','messages_sent','users_sent','messages_received','users_received']]\
        .groupby(['event_date','age']).sum().reset_index()
        df_age['feature'] = 'age'
        df_age = df_age.iloc[:, [0,8,1,2,3,4,5,6,7]]
        df_age = df_age.rename(columns={'age':'feature_value'})
        return df_age
    
    @task()
    def transform_os(df_all):
        df_os = df_all[['event_date','os','likes','views','messages_sent','users_sent','messages_received','users_received']]\
        .groupby(['event_date','os']).sum().reset_index()
        df_os['feature'] = 'os'
        df_os = df_os.iloc[:, [0,8,1,2,3,4,5,6,7]]
        df_os = df_os.rename(columns={'os':'feature_value'})
        return df_os
    
    @task
    def join_tables_2(df_gender,df_age,df_os):
        final_table = pd.concat([df_gender,df_age,df_os])
        final_table = final_table.astype({'likes':'int','views':'int','messages_sent': 'int','users_sent':'int',\
                                          'messages_received':'int','users_received':'int'})
        return final_table       
    
    @task
    def load(final_table):
        q_3 = '''CREATE TABLE IF NOT EXISTS test.table_epopova
                (event_date Date,
                feature String,
                feature_value String,
                likes UInt64,
                views UInt64,
                messages_sent UInt64,
                users_sent UInt64,
                messages_received UInt64,
                users_received UInt64
                ) ENGINE = Log() '''
        ph.execute(connection=con_2, query=q_3)
        ph.to_clickhouse(df=final_table, table = 'table_epopova', connection=con_2, index=False)

    df_feed = extract_feed()
    df_mes = extract_mes()
    df_all = join_tables(df_feed,df_mes)
    df_gender = transform_gender(df_all)
    df_age = transform_age(df_all)
    df_os = transform_os(df_all)
    final_table = join_tables_2(df_gender,df_age,df_os)
    load(final_table)
    
dag_table_epopova = dag_table_epopova()