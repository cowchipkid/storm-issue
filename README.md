# storm-issue
Simple storm topology demonstrates tuple anchoring issue prevents parallelization across nodes. When tuples are anchored, the topology will not be able to utilize nodes in the cluster other than the one using the spout, so even if sufficient compute resources could be saturated across multiple nodes, if anchoring is enabled, only one node is saturated.

This project can be built using gradle using "gradle clean build jar". The TestParallelismTopology has some finals that can be used to adjust the data volumed passed around, the amount of computation to perform on each tuple(measured in milliseconds) and if tuples are to be anchored or not.
