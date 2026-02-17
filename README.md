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

There is only 1 market for this use case, in the case of multiple markets we can store a map of Markets and map each operation to the corresponding market. We can horizontally scale this by paritioning by markets but will leave out of scope.

After implementing and testing the orderbook, we can move on to replicating state transitions across nodes. Because an orderbook is a sequential state machine, correctness depends on every replica applying operations in exactly the same order — any divergence could result in inconsistent matches. This demands the strongest consistency guarantee: linearizability.

Under CAP theorem, linearizability and partition tolerance are non-negotiable here, which means we must sacrifice some availability during network partitions. Rather than risk a split-brain scenario where replicas diverge, the system will reject operations until quorum can be reached — prioritizing correctness over uptime.

Given these constraints I've designed replication as follows:

1. Write path: All writes flow through a primary node that assigns sequence numbers
2. Synchronous replication: Primary replicates to a majority quorum before acknowledging
3. Read path: Quorum reads from majority of replicas, selecting state with highest sequence number
4. Failure recovery: If secondary fails, we can bring up another secondary and request log entries from other replicas to get caught up.

One issue with this is that if there is a bug where a sequence of events brings the system into a bad state such as a seg fault, then each replica will execute the exact same sequence and system will no longer be live. It is important that we have thouroughly tested against production workloads before putting a service like this into production.

Stuff I would like to do if I had more time:
1. Durable WAL for the primary to catch up to the live state after a crash on primary
2. Leader Election, if the primary goes down ideally we elect a new leader without manual intervention. We can implement Raft leader leases to manage leader state.
3. Support of multiple markets and partition service by market
4. More unit testing specifically for the replicating logic
5. Some more chaos testing where we are testing against a production workload and introduce server crashes (have pretty simple chaos testing set up)


## How to run
run with:
scripts/run-replica-cluster.sh

test with:
scripts/replica-cluster-failover-test.sh

This testing script spins up a core with 3 replicas, posts some orders, confirms valid state of replicas. Brings down a replica and confirms that the service doesnt break, then brings it back up and queries that replica to confirm that the state is consistent with the rest of the system.

Core and replicas are built as the same binary with different command line flags.

## AI & Tools
I set up the project structure without the use of AI (stuff like observability, handlers, api, ...)

Most of the business logic was written with the assistance of Codex. My codex flow looks as follows:
1. Write a plan for a specific function/object
2. Have codex write some code to implement function
3. Review the code in the codex app (like a git diff)
4. Repeat till this feature is gtg and move on to the next

When not prompted clearly for replication logic, there would be quite a bit of holes in code the AI writes. An example of this is it would blindly replicate a log entry to replicas and when it does not get enough votes to commit it on the primary the replicas would still apply the operation to their orderbook in which the followers would get into an incorrect state. So I prompted the AI to do a sort of 2 phase commit with prepare + commit.

Another area where I had to take over from the AI is it would try to set up its own logging for each individual piece instead of the observability package that I set up.
