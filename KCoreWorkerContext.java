package org.apache.giraph.examples.kcore;
import org.apache.giraph.examples.kcore.KCoreMasterCompute;

/* Giraph dependencies */
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

/* Hadoop dependencies */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;

/**
* Worker context used with {@link KCore}.
*/
public class KCoreWorkerContext extends WorkerContext {

  /** 
  * Class logger 
  */
  private final static Logger logger = Logger.getLogger(KCore.class.getName());

  /**
  * Final max kcore value 
  */
  private static double FINAL_KCORE_MAX;

  /**
  * Returns the final maximum value.
  * @return kcore_max the maximum k-core value.
  */
  public static double getFinalMax() {
    return FINAL_KCORE_MAX;
  }

  @Override
  public void preApplication()
    throws InstantiationException, IllegalAccessException {
  }

  @Override
  public void postApplication() {
    FINAL_KCORE_MAX = this.<DoubleWritable>getAggregatedValue(
      KCoreMasterCompute.KCORE_CUR).get();
    logger.info(
      "\n K-Core Max : " + FINAL_KCORE_MAX
    );
  }

  @Override
  public void preSuperstep() { 
  }

  @Override
  public void postSuperstep() { }
}
