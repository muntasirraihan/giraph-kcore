package org.apache.giraph.examples.kcore;
import org.apache.giraph.examples.kcore.KCoreMessage;
import org.apache.giraph.examples.kcore.KCoreMasterCompute;

import org.apache.log4j.Logger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
* Vertex value used for the Core implementation 
*/

public class KCoreVertexValue implements Writable {

  private final static Logger logger = Logger.getLogger(KCore.class.getName());

  /** 
  * Core is an integer that represents the exact estimate of the 
  * corness of u, initialized with the local degree.
  */
  private double core;

  /**
  * Default constructor
  */
  public KCoreVertexValue() {
    this.core = 0;
  }
  
  /**
  * Paramaterized constructor
  *
  *@param core the core value we initialize the vertex to.
  */
  public KCoreVertexValue(double core) {
    this.core = core;
  }

  // Serialization functions -----------------------------------------------

  @Override
  public void readFields(DataInput input) throws IOException {
    this.core = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeDouble(this.core);
  }

  // Accessors -------------------------------------------------------------

  /**
  * Gets the node's core value.
  * @return coreness
  */
  public double getCore() {
    return this.core;
  }

  /**
  * Sets the node's core value.
  * @param core new core value
  */
  public void setCore(double core) {
    this.core = core;
  }

  /**
  * @param deleted count totaled from the messages
  * @param id of current vertex
  */
  public void logDecAgg(LongWritable id, double deleted) {
    if (deleted > 0) {
      logger.info(
        "Vertex " + id + 
        " have to decrement by " + deleted
      );
    }
  }
  
  /**
  * @param id of current vertex
  * @param current_k_core aggregated k core
  */
  public void logFinalCore(LongWritable id, double current_kcore) {
    logger.info(
      "Vertex " + id + " has final kcore of " + current_kcore
    );
  }

  /**
  * @param id of current vertex
  * @param message receieved message
  */
  public void logMessage(LongWritable id, KCoreMessage message) {
    logger.info(
      "Vertex id " + id + 
      " Processing message" + message
    );
  }

}
