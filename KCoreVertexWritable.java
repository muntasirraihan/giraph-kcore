package org.apache.giraph.examples.kcore;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class KCoreVertexWritable implements Writable, Configurable {

  private Configuration conf;
  private long vertex_org;
  private double deleted;

  public KCoreVertexWritable() {}

  /**
   * @param vertex_org        id of the orginating vertex
   * @param deleted           deleted count
   */
  public KCoreVertexWritable(long vertex_org, double deleted) {
    this.vertex_org = vertex_org;
    this.deleted = deleted;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    vertex_org = input.readLong();
    this.deleted = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(vertex_org);
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
  public long get_vertex_org() {
    return vertex_org;
  }

  /**
   * @return double the deleted
   */
  public double get_deleted() {
    return deleted;
  }


  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("{ sender: " +  this.vertex_org + "; deleted: ");
    buffer.append(this.deleted + " }");
    return buffer.toString();
  }
}
