package org.apache.giraph.examples.kcore;

/* Giraph dependencies */
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

/* Hadoop dependencies */
import org.apache.log4j.Logger;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;

public class KCoreMasterCompute extends DefaultMasterCompute {
  /** 
  * Class logger 
  */
  private final static Logger logger = Logger.getLogger(KCore.class.getName());

  /**
  * Aggregator values.
  */
  public final static String KCORE_CUR = "KCORE_CUR";
  public final static String KCORE_SUM = "KCORE_SUM";
  public final static String KCORE_DEC = "KCORE_DEC";

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    
    registerPersistentAggregator(KCORE_CUR, DoubleSumAggregator.class);
    registerPersistentAggregator(KCORE_SUM, DoubleSumAggregator.class);
    registerPersistentAggregator(KCORE_DEC, DoubleSumAggregator.class);
    setAggregatedValue(KCORE_CUR, new DoubleWritable(0));
    setAggregatedValue(KCORE_SUM, new DoubleWritable(0));
    setAggregatedValue(KCORE_DEC, new DoubleWritable(0));
  }

  @Override
  public void compute() {
      DoubleWritable unprocessed_nodes = getAggregatedValue(KCORE_SUM);
      DoubleWritable processed_nodes = getAggregatedValue(KCORE_DEC);
      
      /**
      * Log supersteps.
      */
      logger.info(
        "\n Master Compute: unprocessed nodes: " + unprocessed_nodes + 
        "\n processed nodes: " + processed_nodes + 
        "\n kcore value: " + getAggregatedValue(KCORE_CUR) + 
        "\n superstep: " + getSuperstep() + 
        "\n edges remaining: " + getTotalNumEdges()
      );


      if (0 == (unprocessed_nodes.get() - processed_nodes.get())) {
        
        logger.info(
          "\n Master Compute: " + processed_nodes.get() + " nodes in" +
          getAggregatedValue(KCORE_CUR) + " core"
        );
        
        /**
        * Advance current k-core iteration.
        */
        DoubleWritable kcore_current = getAggregatedValue(KCORE_CUR);
        setAggregatedValue(KCORE_CUR, new DoubleWritable(
          kcore_current.get() + 1
        ));
        setAggregatedValue(KCORE_SUM, new DoubleWritable(0));
        setAggregatedValue(KCORE_DEC, new DoubleWritable(0));
      }
      
      if (getSuperstep() > 1 && getTotalNumEdges() == 0) {
        /**
        * Ends the computation.
        */
        logger.info(
          "\n\n Master Compute: highest core is " +
          getAggregatedValue(KCORE_CUR)
        );
        haltComputation();
      }
  }
}
