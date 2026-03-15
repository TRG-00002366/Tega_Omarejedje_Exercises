# Kafka Architecture Concepts Answers

## 1) What is the difference between a topic and a partition?
A **topic** is the logical stream or category of messages, such as `orders` or `payments`. A **partition** is a physical subdivision of that topic that stores an ordered sequence of records and lets Kafka spread load across brokers and consumers. :contentReference[oaicite:0]{index=0}

## 2) Why might you choose to have multiple partitions for a single topic?
Multiple partitions let Kafka scale reads and writes by distributing data and request load across brokers. They also allow multiple consumers in the same consumer group to read in parallel, which increases throughput. :contentReference[oaicite:1]{index=1}

## 3) What is the role of ZooKeeper in a Kafka cluster? What is KRaft and how is it different?
In older Kafka deployments, ZooKeeper handled cluster metadata and coordination tasks. In **KRaft** mode, Kafka removes ZooKeeper and uses a built-in metadata quorum of Kafka controllers instead; as of Kafka 4.0, ZooKeeper mode has been removed and Kafka supports KRaft only. :contentReference[oaicite:2]{index=2}

## 4) Explain the difference between a leader replica and a follower replica.
The **leader replica** for a partition is the broker replica that serves client reads and writes for that partition. **Follower replicas** copy data from the leader and stay caught up so one of them can take over if the leader fails. :contentReference[oaicite:3]{index=3}

## 5) What does ISR stand for, and why is it important for fault tolerance?
**ISR** stands for **In-Sync Replicas**. It matters because Kafka prefers to elect a new leader from the ISR, which helps ensure the new leader is up to date and reduces the risk of data loss after failures. :contentReference[oaicite:4]{index=4}

## 6) How does Kafka differ from traditional message queues like RabbitMQ?
Kafka is a **distributed event streaming platform** built around durable, replicated logs and replayable offsets, while traditional queues are usually centered on broker-managed delivery and message removal after consumption. Kafka is designed for high-throughput streaming, partitioned scalability, and retaining events for reprocessing. :contentReference[oaicite:5]{index=5}

## 7) What is the publish-subscribe pattern, and how does Kafka implement it?
The **publish-subscribe** pattern lets producers publish messages without knowing which consumers will read them. Kafka implements this through topics: producers write records to topics, and consumers subscribe to topics directly or as part of consumer groups. :contentReference[oaicite:6]{index=6}

## 8) What happens when a Kafka broker fails? How does the cluster recover?
If a broker fails, partitions it led lose that leader temporarily and the ISR for affected partitions shrinks. Kafka’s controller elects new leaders from available replicas, and when the failed broker returns and catches up, it can rejoin the ISR. :contentReference[oaicite:7]{index=7}