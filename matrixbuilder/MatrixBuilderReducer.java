package matrixbuilder;
//import mutipleInput.Join;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixBuilderReducer
     extends Reducer<VertexWritable,VertexOrCountWritable,PositionPairWritable,FloatWritable> {

  private ArrayList<VertexWritable> buffer = new ArrayList<VertexWritable>();
  private int edgeCount;
  private boolean edgeCountEncountered;

  private void flushBuffer(VertexWritable iVertex,Context context){
    for(VertexWritable jVertex: buffer)
      context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
  }
  //Input a Interable of either destination vertex or edgecount (for the given source vertex)
  public void reduce(VertexWritable iVertex, Iterable<VertexOrCountWritable> values, Context context) throws IOException, InterruptedException {
    for(VertexOrCountWritable v:values){
      if(v.get() instanceof VertexWritable){
        VertexWritable jVertex = (VertexWritable)(v.get());
        if(edgeCountEncountered){
          context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
        }
        else{
          buffer.add(jVertex);
        }
      }
      else{ //type CountWritable
        this.edgeCountEncountered = true;
        CountWritable temp = (CountWritable)(v.get());
        this.edgeCount = temp.get();
        this.flushBuffer(iVertex,context);
      }
    }
  }
}
