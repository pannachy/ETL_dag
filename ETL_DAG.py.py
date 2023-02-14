from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# одключение к основной бд (выгрузка)
connection_db = {
    'host': host,
    'database': database,
    'user': user,
    'password': password
}

# подключение ко второй базе (загрузка)
connection_test = {
    'host': host',
    'database': database_load
    'user': user,
    'password': password
}

# запрос на выгрузку feed
query_get_feed = """ SELECT toDate(time) as event_date,
                            user_id, 
                            gender,
                            os,
                            multiIf(age < 20, '0-20', 
                                        age >= 20 AND age < 30, '20-29', 
                                        age >= 30 AND age < 40, '30-39', 
                                        age >= 40 AND age < 50, '40-49', 
                                        '50+') AS age_group,
                            sum(action = 'like') as likes,
                            sum(action = 'view') as views
                    FROM simulator_20230120.feed_actions
                    WHERE event_date = toDate(today()) - 1
                    GROUP BY event_date, user_id, gender, os, age_group
                    """

# запрос на выгрузку messages
query_get_messages = """WITH t1 AS (
                                SELECT toDate(time) as event_date,
                                        reciever_id,
                                        uniq(user_id) as users_sent,
                                        count(user_id) as messages_received
                                FROM simulator_20230120.message_actions
                                WHERE event_date = toDate(today()) - 1
                                GROUP BY event_date, reciever_id),

                             t2 AS (
                                SELECT event_date,
                                        reciever_id as user_id,
                                        GREATEST(tmp1.gender, tmp2.gender) as gender, 
                                        GREATEST(tmp1.age, tmp2.age) as age, 
                                        GREATEST(tmp1.os, tmp2.os) as os,
                                        users_sent,
                                        0 as users_received,
                                        0 as messages_sent,
                                        messages_received
                                FROM t1
                                LEFT JOIN 
                                        (SELECT distinct user_id, 
                                                gender, 
                                                age, 
                                                os
                                        FROM simulator_20230120.feed_actions
                                        ) as tmp1
                                ON reciever_id = tmp1.user_id         
                                LEFT JOIN 
                                        (SELECT distinct user_id, 
                                                gender, 
                                                age, 
                                                os
                                        FROM simulator_20230120.message_actions
                                        ) as tmp2
                                ON reciever_id = tmp2.user_id

                            UNION ALL

                                SELECT toDate(time) as event_date,
                                        user_id,
                                        gender,
                                        age,
                                        os,
                                        0  as users_sent,
                                        uniq(reciever_id) as users_received,
                                        count(reciever_id) as messages_sent,
                                        0 as messages_received
                                FROM simulator_20230120.message_actions
                                WHERE event_date = toDate(today()) - 1
                                GROUP BY event_date, user_id, gender, os, age
                                )

                        SELECT event_date,
                                user_id,
                                gender,
                                multiIf(age < 20, '0-20', 
                                    age >= 20 AND age < 30, '20-29', 
                                    age >= 30 AND age < 40, '30-39', 
                                    age >= 40 AND age < 50, '40-49', 
                                    '50+') AS age_group,
                                os,
                                SUM(users_sent) as users_sent,
                                SUM(users_received) as users_received,
                                SUM(messages_sent) as messages_sent,
                                SUM(messages_received) as messages_received
                        FROM  t2
                        GROUP BY event_date, user_id, gender, age, os
                        ORDER BY event_date                
                        """

# запрос на создание новой таблицы
query_create_table = """CREATE TABLE IF NOT EXISTS test.a_ponom6 (
                            event_date Date,
                            dimension String,
                            dimension_value String,
                            views Float64,
                            likes Float64,
                            messages_received Float64,
                            messages_sent Float64,
                            users_received Float64,
                            users_sent Float64                            
                            )
                        ENGINE = MergeTree()
                        ORDER BY event_date
                        """

# дефолтные параметры
default_args = {
    'owner': 'a-ponomareva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 2),
    'email': 'chann@bk.ru',
    'email_on_failure': True
}

# интервал для запуска, каждый день в 23-00
schedule_interval = '0 23 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, tags=['a-pono'], catchup=False)
def pono_dag():
    @task()  # загрузка feed
    def extract_feed():
        df_feed = ph.read_clickhouse(query=query_get_feed, connection=connection_db)

        return df_feed

    @task()  # загрузка messages
    def extract_messages():
        df_messages = ph.read_clickhouse(query=query_get_messages, connection=connection_db)

        return df_messages

    @task()  # объединение feed и messages
    def transfrom_merging(df_feed, df_messages):
        df_total_pn = pd.merge(df_feed, df_messages, on=['event_date', 'user_id', 'age_group', 'os', 'gender'], how='outer')
        df_total_pn.fillna(0, inplace=True)

        return df_total_pn

    @task()  # срез по полу
    def transform_gender(df_total_pn):
        df_gender = df_total_pn.groupby(['event_date', 'gender'], as_index=False) \
            [['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']] \
            .sum()  
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)

        return df_gender

    @task()  # срез по возрасту
    def transform_age(df_total_pn):
        df_age = df_total_pn.groupby(['event_date', 'age_group'], as_index=False) \
            [['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']] \
            .sum()
        df_age['dimension'] = 'age'
        df_age.rename(columns={'age_group': 'dimension_value'}, inplace=True)

        return df_age

    @task()  # срез по os
    def transform_os(df_total_pn):
        df_os = df_total_pn.groupby(['event_date', 'os'], as_index=False) \
            [['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']] \
            .sum()
        df_os['dimension'] = 'os'
        df_os.rename(columns={'os': 'dimension_value'}, inplace=True)

        return df_os
    
    @task()  # объединение срезов
    def concat_all(df_gender, df_age, df_os):
        df_final = pd.concat([df_gender, df_age, df_os], axis=0)
        
        return df_final

    @task()  # загрузка таблицы в БД
    def load(df_final):
        ph.execute(query=query_create_table, connection=connection_test)
        ph.to_clickhouse(df_final, 'a_ponom6', connection=connection_test, index=False)

    # последовательность тасков
    df_feed = extract_feed()
    df_messages = extract_messages()
    df_total_pn = transfrom_merging(df_feed, df_messages)
    df_gender = transform_gender(df_total_pn)
    df_age = transform_age(df_total_pn)
    df_os = transform_os(df_total_pn)
    df_final = concat_all(df_gender, df_age, df_os)
    load(df_final)

# запуск дага
pono_dag = pono_dag()