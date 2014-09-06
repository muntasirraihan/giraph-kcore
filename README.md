## K-Core Giraph

This is a different way of computing the K-Cores of vertices in a graph with Giraph than the repository I've shared.

## How it works.

This is a bottom up approach, in that it first computes the 0-kcore, then the 1-core, 2-core, etc.
If you have large sparse graphs, computing the lower core values can take awhile.
However, this approach doesn't generate nearly as much messages as the other implementation I've shared, so depending on your environment this might be preferable.
On multi-node systems, I've found both to be comparable in terms of performance; it really boils down to your graph structure.

## Quick and Dirty install

Drop this under the example folder included with giraph and recompile with maven.

## Graph Structure

The expected format of the graph is [vertex_id, vertex_kcore_initial, [[neighbor_vertex_id, vertex_neighbor_kcore],[......]].
Here, verteix_id is the identifier of the vertex, vertex_kcore_initial is the starting kcore value (just leave this to be 0), and the list after that is the neighboring vertices (identifier and k-core value initially).
I would just set the kcore values to be 0; I added that in to experiment with (seeing if there would be a faster way to compute kcores, etc).

## Example Usage

GRAPH=example_graph.txt 
GIRAPH_HOME=/opt/bin/giraph

$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar \
     org.apache.giraph.GiraphRunner org.apache.giraph.examples.kcore.KCore \
     -mc org.apache.giraph.examples.kcore.KCoreMasterCompute \
     -wc org.apache.giraph.examples.kcore.KCoreWorkerContext \
     -vif org.apache.giraph.examples.kcore.KCoreInputFormat \
     -vip /user/hduser/input/$GRAPH \
     -vof org.apache.giraph.examples.kcore.KCoreOutputFormat \
     -op /user/hduser/output/kcore \
     -w 1 \
     /

