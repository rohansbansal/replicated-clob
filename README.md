# Replicated CLOB

## Overview
Central limit orderbooks are sequential state machines, in which matching outcomes depend on the exact order in which commands are applied. To ensure that replicas stay consistent operations must be applied in the exact same order on each replica. 

I started by implementing the orderbook, which needs to support several oprations:
1. Posting an order
2. Cancelling an order
3. Querying historical fills
4. Querying live orders

When posting an order if it can be matched to a resting order we match it with the resting order and track it in fills.
I left it quite simple for the sake of the take-home. We store a heap of bids and asks ordered by priceLevel. We keep track of orders in Price Time Priority in a map of price level to queue, and keep a ref of orderId to orderRef for quick cancels (a lot of orderbook workloads are cancels). Lastly we track fills by user in memory on the orderbook, to make durable can write to a db but left it out of scope for the take home.

Imagine that there is only 1 market for this use case, in the case of multiple markets we can store a map of Markets and map each operation to the corresponding market. We can horizontally scale this by paritioning by markets but will leave out of scope.

After implementing and testing the orderbook, we can move on to the data replication. When thinking about CAP theorem, we need this state machine to have the strongest consistency guarantees (linearizability). If this system was not linearizable it could result in incorrect matches on replicas. Along with this we need to be able to handle network paritions as they are bound to happen in a distributed system. As a result we must give on availability (ideally as little downtime as possible).

Given these constraints I've designed replication as follows.

1. Write path: All writes flow through a primary node that assigns 
   sequence numbers
2. Synchronous replication: Primary replicates to a majority quorum 
   before acknowledging
3. Read path: Quorum reads from majority of replicas, selecting state 
   with highest sequence number
4. Failure recovery: If secondary fails, we can bring up another secondary and request log entries from the primary to get caught up. If primary fails, we can bring up a new primary that catches up via durable WAL

