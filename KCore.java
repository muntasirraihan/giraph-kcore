package org.apache.giraph.examples.kcore;
import org.apache.giraph.examples.kcore.KCoreMessage;
import org.apache.giraph.examples.kcore.KCoreMasterCompute;

/* Giraph dependencies */
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

/* Hadoop dependencies */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import java.io.IOException;

/**
 * Calculates the maximal k-cores for a given graph
 */
public class KCore extends BasicComputation<
    LongWritable, KCoreVertexValue, LongWritable, KCoreMessage> {

  /** 
  * Logger (levels should be set in log4j.properies)
  */
  private static final Logger logger;
  static { 
    logger = Logger.getLogger(KCore.class);
    logger.setLevel(Level.OFF);
  }

  @Override
  public void compute(
      Vertex<LongWritable, KCoreVertexValue, LongWritable> vertex, 
      Iterable<KCoreMessage> messages) 
    throws IOException {
    
    if (0 == getSuperstep()) {
      /** 
      * Initialize the vertex value to be our degree and current k-core. 
      */
      KCoreVertexValue value = new KCoreVertexValue(vertex.getNumEdges());
      vertex.setValue(value);
    } else if (1 == getSuperstep()) {

    }
    
    for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
      logger.info("Vertex " + vertex.getId() + " Edge: " + edge);
    }

    /** 
    * Get K Core iteration (1,2,3, etc) from aggregator 
    */
    double deleted = 0;
    for (KCoreMessage message: messages) {
      vertex.getValue().logMessage(vertex.getId(), message);
      deleted += message.getDeleted();
      vertex.removeEdges(new LongWritable(message.getNeighbor()));
    }

    DoubleWritable kcore_current = getAggregatedValue(KCoreMasterCompute.KCORE_CUR);

    /**
    * Means we actually have to perform some computation 
    */
    if ((vertex.getNumEdges() <= kcore_current.get())  &&
        (vertex.getNumEdges() != 0) && 
        (!vertex.isHalted())){
      
      vertex.getValue().setCore(kcore_current.get());

      /**
      * If degree <= current_k_core, send a "1" to the 
      * aggregator to indicate this vertex has been processed.
      */
      int count = 0;
      aggregate(KCoreMasterCompute.KCORE_SUM, new DoubleWritable(1));
      long[] edge_array = new long[vertex.getNumEdges()];

      for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {

        if (count == 0) {
          /**
          * First edge- want to propogate deleted value to this vertex.
          */
          sendMessage(edge.getTargetVertexId(), 
            new KCoreMessage(vertex.getId().get(), deleted+1)
          );

        } else {
          sendMessage(edge.getTargetVertexId(), 
            new KCoreMessage(vertex.getId().get(), 0)
          );
        }
        edge_array[count] = edge.getTargetVertexId().get();
        count++;
      }

      /** 
      * Need to delete edges (deleting an edge while iterating it
      * does not seem to be possible with current version of giraph,
      * even using mutable edges).
      */
      for(int i=0; i < edge_array.length; i++) {
        vertex.removeEdges(new LongWritable(edge_array[i]));
      }
      vertex.voteToHalt();
    
    } else {
      /** 
      * Means vertex is restricted by this core, so send
      * value to aggregator and have it subtract.
      */
      vertex.getValue().logDecAgg(vertex.getId(), deleted);
      aggregate(KCoreMasterCompute.KCORE_DEC, new DoubleWritable(deleted));
    }
    
  }
}
