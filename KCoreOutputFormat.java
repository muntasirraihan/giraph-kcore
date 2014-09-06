package org.apache.giraph.examples.kcore;
import org.apache.giraph.examples.kcore.KCoreVertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import java.io.IOException;


public class KCoreOutputFormat extends TextVertexOutputFormat
  <LongWritable, KCoreVertexValue, LongWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new KCoreOutputFormatVertexWriter();
  }

  /**
  * Vertex Writer that outputs the following JSON:
  * [vertex id, vertex k-core value]
  * An example of this is:
  * [1,2]  (vertex id of 1 and k-core value of 2)
  */
  private class KCoreOutputFormatVertexWriter extends
    TextVertexWriterToEachLine {
    @Override

    public Text convertVertexToLine(
      Vertex<LongWritable, KCoreVertexValue, LongWritable> vertex
    ) throws IOException {

      JSONArray jsonVertex = new JSONArray();

      try {
        jsonVertex.put(vertex.getId().get());
        jsonVertex.put(vertex.getValue().getCore());

      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "KCoreOutputFormatVertexWriter: Unable to write" + vertex);
      }
      return new Text(jsonVertex.toString());
    }
  }
}
