"""
Create Kafka Topics with Python
================================
Complete the TODO sections to create topics programmatically.

Prerequisites:
    pip install kafka-python
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_admin_client(bootstrap_servers: str = "localhost:9092"):
    """
    Create and return a KafkaAdminClient.

    TODO: Create the admin client with appropriate settings.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic-creator"
        )
        return admin_client
    except Exception as e:
        print(f"[ERROR] {e}")
        return None


def create_single_topic(admin_client, topic_name: str, partitions: int, retention_days: int):
    """
    Create a single topic with custom configuration.

    TODO: Create a NewTopic with the specified settings and create it.
    """
    print(f"Creating topic '{topic_name}'...")

    # Convert retention days to milliseconds
    retention_ms = retention_days * 24 * 60 * 60 * 1000

    # TODO: Create a NewTopic object
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=1,
        topic_configs={"retention.ms": str(retention_ms)}
    )

    if topic is None:
        print("  [ERROR] Topic not created - complete the TODO!")
        return False

    # TODO: Create the topic using admin_client.create_topics([topic])
    # Handle TopicAlreadyExistsError gracefully
    try:
        admin_client.create_topics([topic])
        print(f"  [SUCCESS] Created topic '{topic_name}'")
    except TopicAlreadyExistsError:
        print(f"  [INFO] Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"  [ERROR] {e}")
        return False

    return True


def create_multiple_topics(admin_client, topics_config: list):
    """
    Create multiple topics in a batch.

    TODO: Create multiple NewTopic objects and create them all at once.

    topics_config format:
    [
        {"name": "topic1", "partitions": 3, "retention_days": 7},
        {"name": "topic2", "partitions": 2, "retention_days": 1},
    ]
    """
    print(f"\nCreating {len(topics_config)} topics in batch...")

    # TODO: Create a list of NewTopic objects from topics_config
    topics = []

    for config in topics_config:
        retention_ms = config["retention_days"] * 24 * 60 * 60 * 1000

        # TODO: Create NewTopic and append to topics list
        topic = NewTopic(
            name=config["name"],
            num_partitions=config["partitions"],
            replication_factor=1,
            topic_configs={"retention.ms": str(retention_ms)}
        )
        topics.append(topic)

    if not topics:
        print("  [ERROR] No topics created - complete the TODO!")
        return False

    # TODO: Create all topics at once using admin_client.create_topics(topics)
    try:
        admin_client.create_topics(topics)
        print(f"  [SUCCESS] Created {len(topics)} topics")
        return True
    except TopicAlreadyExistsError:
        print("  [INFO] One or more topics already exist")
        return True
    except Exception as e:
        print(f"  [ERROR] {e}")
        return False


def main():
    """Main function to create all required topics."""
    print("=" * 50)
    print("KAFKA TOPIC CREATION EXERCISE")
    print("=" * 50)

    # Create admin client
    admin_client = create_admin_client()

    if admin_client is None:
        print("\n[ERROR] Failed to create admin client. Complete the TODO!")
        return

    # Define topics to create
    topics = [
        {"name": "inventory-updates", "partitions": 4, "retention_days": 3},
        {"name": "price-changes", "partitions": 2, "retention_days": 7},
        {"name": "notifications", "partitions": 3, "retention_days": 1},
    ]

    # Create topics
    create_multiple_topics(admin_client, topics)

    # Cleanup
    admin_client.close()

    print("\n" + "=" * 50)
    print("TOPIC CREATION COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()