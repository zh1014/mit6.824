#!/bin/bash
cd kvraft/ || exit
for i in {1..50}
do
   go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B -race || exit
done
