package matrixbuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
//Fancy stuff
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.GenericWritable;

public class MatrixBuilderReducer
     extends Reducer<VertexWritable,VertexOrCountWritable,PositionPairWritable,FloatWritable> {

  private ArrayList<VertexWritable> iVertices = new ArrayList<VertexWritable>();
  private int edgeCount;
  private boolean edgeCountEncountered;

  private void flushBuffer(VertexWritable iVertex,Context context){
    for(VertexWritable jVertex: iVertices)
      context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
  }
  //Input a Interable of either destination vertex or edgecount (for the given source vertex)
  public void reduce(VertexWritable iVertex, Iterable<VertexOrCountWritable> values, Context context) throws IOException, InterruptedException {
    for(VertexOrCountWritable v:values){
      if(v instanceof VertexWritable){
        VertexWritable jVertex = (VertexWritable)(v);
        if(edgeCountEncountered){
          context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
        }
        else{
          iVertices.add(x);
        }
      }
      else{ //type CountWritable
        this.edgeCountEncountered = true;
        CountWritable temp = (CountWritable)(x);
        this.edgeCount = temp.get();
        this.flushBuffer(iVertex,context);
      }
    }
  }
}
