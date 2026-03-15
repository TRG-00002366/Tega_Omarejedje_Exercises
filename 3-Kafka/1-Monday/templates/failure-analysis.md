# Failure Analysis

## Scenario
Cluster has 3 brokers and topic `orders` with 3 partitions, replication factor 2:

- Partition 0: Leader on Broker 1, Follower on Broker 2
- Partition 1: Leader on Broker 2, Follower on Broker 3
- Partition 2: Leader on Broker 3, Follower on Broker 1

Broker 2 crashes.

## 1) Which partitions are affected?
Broker 2 affects **Partition 0** and **Partition 1**. For Partition 0, Broker 2 was a follower, so replication is reduced but the partition still has its leader on Broker 1. For Partition 1, Broker 2 was the leader, so leader election is required. :contentReference[oaicite:8]{index=8}

## 2) What happens to Partition 0? (Where is its leader now?)
Partition 0 keeps running with **Broker 1** as leader. Its ISR shrinks because Broker 2 is down, so the partition is still available but has reduced redundancy until Broker 2 returns and catches up. :contentReference[oaicite:9]{index=9}

## 3) What happens to Partition 1? (Who becomes the new leader?)
Partition 1 needs a new leader because Broker 2 failed. Assuming Broker 3 was in the ISR and fully caught up, **Broker 3** becomes the new leader. :contentReference[oaicite:10]{index=10}

## 4) Can producers still send messages to all partitions? Why or why not?
Yes, producers can still send messages to all three partitions in this scenario. Partition 0 still has Broker 1 as leader, Partition 1 fails over to Broker 3, and Partition 2 already has Broker 3 as leader, so each partition still has an available leader. :contentReference[oaicite:11]{index=11}

## 5) What is the cluster's replication status after the failure?
Replication is degraded because every affected partition has lost one replica from its ISR while Broker 2 is down. The cluster remains available, but it is less fault tolerant until Broker 2 comes back, catches up, and rejoins the ISR. :contentReference[oaicite:12]{index=12}