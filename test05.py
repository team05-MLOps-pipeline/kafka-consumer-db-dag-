from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import pymysql
import json
from confluent_kafka import Consumer, KafkaException

# MySQL 연결 설정
conn = pymysql.connect(host='ip',
                       user='eun',
                       port='port',
                       password='password',
                       db='eun_test',
                       charset='utf8')
cursor = conn.cursor()

def convert_date(kr_date_str):
    # "2023.10.13. 오후 1:09" --> "2023-10-13 13:09"
    date_str, am_pm, time_str = kr_date_str.split()
    hour, minute = map(int, time_str.split(':'))

    if am_pm == "오후" and hour < 12:
        hour += 12
    elif am_pm == '오전' and hour == 12:
        hour = 0

    return f"{date_str.replace('.', '-')} {hour:02d}:{minute:02d}:00"

def process_kafka_message(message):
    value = message.value().decode('utf-8')
    print('Received message: {}'.format(value))

    # MySQL에 데이터 적재하기
    sql = "INSERT INTO Article (category_id, news_url, company_name, title, author_info, create_date, modify_date, content, image_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data = json.loads(value)
    create_date_converted = convert_date(data['create_date'])

    cursor.execute(sql, (data['category'], data['url'], data['site_name'], data['title'], data['author_info'], create_date_converted, None, data['content'], data['image_url']))
    conn.commit()

def consume_kafka_messages():
    # Kafka consumer 설정
    from confluent_kafka import Consumer, KafkaException
    conf = {'bootstrap.servers':'ip:9094',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    
    # Kafka topic 구독
    consumer.subscribe(['euntest'])
    
    while True:
        msg = consumer.poll(1.0)  # timeout is set to 1 second
        
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            process_kafka_message(msg)

def close_mysql_connection():
    consumer.close()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'test03_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 17),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_to_mysql_dag',
    default_args=default_args,
    description='Consume Kafka messages and store in MySQL',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

consume_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_kafka_messages,
    provide_context=True,
    dag=dag,
)

close_connection_task = PythonOperator(
    task_id='close_mysql_connection',
    python_callable=close_mysql_connection,
    provide_context=True,
    dag=dag,
)

consume_task >> close_connection_task