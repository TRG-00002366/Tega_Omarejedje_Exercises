"""
Kafka Connection Verification Script
=====================================
Complete the TODO sections to verify your Kafka cluster is running.

Prerequisites:
    pip install kafka-python
"""

from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import sys


def verify_connection(bootstrap_servers: str = "localhost:9092"):
    """
    Verify connection to the Kafka cluster.
    
    TODO: Complete this function to:
    1. Create a KafkaAdminClient
    2. List all topics
    3. Print the results
    """
    print("Attempting to connect to Kafka...")
    print("-" * 40)
    
    try:
        # TODO: Create a KafkaAdminClient
        # Hint: Use KafkaAdminClient(bootstrap_servers=..., client_id=...)
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='topic-creator' 
        )
        
        if admin_client is None:
            print("[ERROR] You need to create the admin_client!")
            return False
        
        # TODO: List all topics using admin_client.list_topics()
        topics = admin_client.list_topics()
        
        print(f"Connected to Kafka cluster!")
        print(f"Topics found: {len(topics)}")
        
        for topic in topics:
            print(f"  - {topic}")
        
        admin_client.close()
        return True
        
    except NoBrokersAvailable:
        print("[ERROR] No brokers available. Is Kafka running?")
        print("Try: docker-compose up -d")
        return False
    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def get_broker_info(bootstrap_servers: str = "localhost:9092"):
    """
    Get information about the Kafka brokers.
    
    TODO: Complete this function to:
    1. Create a KafkaConsumer (just for metadata access)
    2. Get broker information from the cluster
    3. Print the broker details
    """
    print("\n" + "-" * 40)
    print("Broker Information")
    print("-" * 40)
    
    try:
        # TODO: Create a KafkaConsumer to access cluster metadata
        # Hint: consumer = KafkaConsumer(bootstrap_servers=..., client_id=...)
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            client_id='broker-info-reader'

        )
        
        if consumer is None:
            print("[ERROR] You need to create the consumer!")
            return False
        
        # TODO: Get brokers from consumer._client.cluster.brokers()
        brokers = consumer._client.cluster.brokers()
        
        print(f"Brokers ({len(brokers)}):")
        for broker in brokers:
            print(f"  - Broker {broker.nodeId}: {broker.host}:{broker.port}")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def main():
    """Main function to run all verification checks."""
    print("=" * 50)
    print("KAFKA CONNECTION VERIFICATION")
    print("=" * 50)
    print()
    
    # Step 1: Verify basic connection
    connection_ok = verify_connection()
    
    if connection_ok:
        # Step 2: Get broker information
        get_broker_info()
        
        print("\n" + "=" * 50)
        print("VERIFICATION COMPLETE - Kafka is ready!")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("VERIFICATION FAILED")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()