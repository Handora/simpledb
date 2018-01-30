# TODO

## lab 3
1. Add code to perform more advanced join cardinality estimation. Rather than using simple heuristics to estimate join cardinality, devise a more sophisticated algorithm. One option is to use joint histograms between every pair of attributes a and b in every pair of tables t1 and t2. The idea is to create buckets of a, and for each bucket A of a, create a histogram of b values that co-occur with a values in A.
2. Bushy plans. Improve the provided orderJoins() and other helper methods to generate bushy joins. Our query plan generation and visualization algorithms are perfectly capable of handling bushy plans; for example, if orderJoins() returns the vector (t1 join t2 ; t3 join t4 ; t2 join t3), this will correspond to a bushy plan with the (t2 join t3) node at the top.
3. Do Grace hash joins!

## lab 4
1. Locking granularity: page-level versus tuple-level
2. Deadlock detection: timeouts versus dependency graphs
3. Deadlock resolution: aborting yourself versus aborting others
