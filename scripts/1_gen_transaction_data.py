import sys
import os
import json
import logging
import datetime as dt
import pandas as pd
import numpy as np
from kafka import KafkaProducer 
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN_DATE_STR = sys.argv[1]

def generate_pseudo_data(RUN_DATE_STR, num_rows=20000, num_users=1000):
    """Generates a Pandas DataFrame with pseudo transaction data."""

    # User IDs
    user_ids = [f"user_{i:06d}" for i in np.random.choice(num_users, size=num_rows)]

    # Transaction IDs
    transaction_ids = np.random.randint(10**11, 10**12 - 1, size=num_rows)

    # Source
    sources = np.random.choice(["Current Account", "Credit Card", "Debit Card"], 
                               size=num_rows, p=[0.6, 0.3, 0.1])

    # Amounts
    amounts = np.random.uniform(1.0, 1000.0, size=num_rows)

    # Vendors
    vendors = ["Online Shopping", "Hospital", "Sport", "Grocery", "Restaurant",
               "Travel", "Entertainment", "Electronics", "Home Improvement",
               "Clothing", "Education", "Sending Out", "Utilities", "Other"]
    vendor_probabilities = np.random.dirichlet(np.ones(len(vendors)))
    vendor_choices = np.random.choice(vendors, size=num_rows, p=vendor_probabilities)

    # Times
    start_date = dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d")
    time_deltas = [dt.timedelta(seconds=np.random.randint(0, 31536000)) for _ in range(num_rows)]  # Up to 1 year
    times = [(start_date + delta).strftime('%Y-%m-%d %H:%M:%S') for delta in time_deltas]

    # Create DataFrame
    df = pd.DataFrame({
        "User ID": user_ids,
        "Transaction ID": transaction_ids,
        "Amount": amounts,
        "Vendor": vendor_choices,
        "Sources": sources,
        "Time": times
    })

    return df


def send_message_batch(producer, topic_name: str, messages: list, batch_size: int = 100):
    """Send a batch of messages to Kafka"""
    futures = []
    
    try:
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            
            # Send batch of messages
            for message in batch:
                future = producer.send(topic_name, message)
                futures.append(future)
            
            # Wait for current batch to complete
            producer.flush()
            
            # Check for any errors in the batch
            for future in futures:
                try:
                    record_metadata = future.get(timeout=10)
                    logger.debug(f"Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
            
            logger.info(f"Processed batch of {len(batch)} messages")
            futures = []  # Clear futures for next batch
            
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        raise

# Modified ingest function
def ingest_data_to_kafka(df, topic_name: str, bootstrap_servers='kafka:29092', batch_size: int = 100):
    """
    Ingest DataFrame to Kafka with batching and error handling
    Args:
        df: DataFrame to ingest
        topic_name: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
        batch_size: Number of messages per batch
    """
    # Create a Kafka Admin Client to manage topics
    admin_client = KafkaAdminClient(
        bootstrap_servers=[bootstrap_servers],  # Adjust as needed
        client_id='admin'
    )

    # Define retention period for 2 days in milliseconds (2 days = 2 * 24 * 60 * 60 * 1000)
    retention_ms = str(2 * 24 * 60 * 60 * 1000)  # "172800000"

    # Create a new topic with the specified retention policy
    new_topic = NewTopic(
        name=topic_name,
        num_partitions=1, # Modify this if you have multiple nodes
        replication_factor=1, # Modify this if you have multiple nodes
        topic_configs={"retention.ms": retention_ms}
    )

    # Try to create the topic (if it already exists, you'll get an exception)
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created with retention.ms set to {retention_ms} (2 days)")
    except Exception as e:
        print(f"Topic creation issue (may already exist): {e}")

    # Ingest data to Kafka
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=100,
        compression_type='gzip'  # Add compression
    )

    try:
        # Convert DataFrame to list of dictionaries more efficiently
        messages = df.to_dict('records')
        
        # Send messages in batches
        send_message_batch(producer, topic_name, messages, batch_size)
        
        logger.info(f"Successfully sent {len(messages)} messages to topic {topic_name}")
        
    except Exception as e:
        logger.error(f"Failed to ingest data: {e}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    # Retrieve exchange info to get a list of all trading symbols
    df = generate_pseudo_data(RUN_DATE_STR)
    print("Data retrieved!", df.shape)

    df.to_parquet(f"/opt/airflow/results/transaction_{RUN_DATE_STR}.parquet", index=False)
    print("Data saved!", os.getcwd())    

    ingest_data_to_kafka(df, "transaction_data", batch_size=100)  
    print("Data Ingested to Kafka!")
