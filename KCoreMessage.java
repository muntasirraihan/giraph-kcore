package org.apache.giraph.examples.kcore;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class KCoreMessage implements Writable, Configurable {

  private Configuration conf;
  private long neighbor;
  private double deleted;

  /**
  * Default Constructor.
  */
  public KCoreMessage() {}

  /**
  * Paramaterized Constructor.
  * @param neighbor id of the orginating vertex
  * @param deleted  deleted count
  */
  public KCoreMessage(long neighbor, double deleted) {
    this.neighbor = neighbor;
    this.deleted = deleted;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    neighbor = input.readLong();
    this.deleted = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(neighbor);
    output.writeDouble(this.deleted);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return long the id
   */
  public long getNeighbor() {
    return neighbor;
  }

  /**
   * @return double the deleted
   */
  public double getDeleted() {
    return deleted;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(
      "{ sender: " +  this.neighbor + "; deleted: " + this.deleted + "}"
    );
    return buffer.toString();
  }
}
