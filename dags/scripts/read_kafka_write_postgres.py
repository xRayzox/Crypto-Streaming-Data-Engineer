import psycopg2
import logging
import json
import time
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("read_kafka_write_postgres")


try:
    conn = psycopg2.connect(
        host="postgres-2",
        database="metabase",
        user="metabase",
        password="metabase",
        port=5432
    )
    cur = conn.cursor()
    logger.info('PostgreSQL server connection is successful')
    logger.info(cur)
except Exception as e:
    logger.error(f"Couldn't create the PostgreSQL connection due to: {e}")

try:
    consumer = KafkaConsumer(
        'btc_prices',
        bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'],
        auto_offset_reset='latest',  # Start consuming from the latest offset
        enable_auto_commit=False  # Disable auto-commit to have manual control over offsets
    )
    logger.info("Kafka connection successful")
except Exception as e:
    logger.error(f"Kafka connection is not successful. {e}")


def create_new_tables_in_postgres():
    """
    Creates the btc_prices table in PostgreSQL server.
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS btc_prices 
        (timestamp VARCHAR(50), name VARCHAR(10), price FLOAT, volume_24h FLOAT, percent_change_24h FLOAT)""")
        conn.commit()
        logging.info("Table created successfully in PostgreSQL server")
    except Exception as e:
        logging.error(f'Table cannot be created due to: {e}')
        conn.rollback()


def insert_data_into_postgres():
    """
    Insert the latest messages coming to Kafka consumer to PostgreSQL table.
    """
    end_time = time.time() + 120 # the script will run for 2 minutes
    for msg in consumer: 
        if time.time() > end_time:
            break

        kafka_message = json.loads(msg.value.decode('utf-8'))
        try:
            # Create an SQL INSERT statement
            insert_query = """INSERT INTO btc_prices (timestamp, name, price, volume_24h, percent_change_24h) 
                              VALUES (%s, %s, %s, %s, %s)"""
            cur.execute(insert_query, (kafka_message['timestamp'], kafka_message['name'], kafka_message['price'], kafka_message['volume_24h'], kafka_message['percent_change_24h']))
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            conn.rollback()
    
    logger.info("Data coming from Kafka topic inserted successfully to PostgreSQL.")


if __name__ == "__main__":
    create_new_tables_in_postgres()
    insert_data_into_postgres()

    cur.close()
    conn.close()
