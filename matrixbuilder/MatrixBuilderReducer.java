
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
     extends Reducer<VertexWritable,VertexOrCountWritable,Text,FloatWritable> {

  private ArrayList<VertexWritable> buffer = new ArrayList<VertexWritable>();

  private void flushBuffer(VertexWritable iVertex,FloatWritable weightage,Context context)throws IOException,InterruptedException{
    //context.write(new Text("Starting output buffer flush: "+Long.toString(iVertex.get())),weightage);
    
    for(VertexWritable jVertex: buffer)
      context.write(new Text(Long.toString(jVertex.get())+" "+Long.toString(iVertex.get())),weightage);
    
    //context.write(new Text("Output buffer flushed: "+Long.toString(iVertex.get())),weightage);
  }
  //Input a Interable of either destination vertex or edgecount (for the given source vertex)
  public void reduce(VertexWritable iVertex, Iterable<VertexOrCountWritable> values, Context context) throws IOException, InterruptedException {
    FloatWritable weightage = new FloatWritable();
    boolean edgeCountEncountered = false;
    int numInputs = 0;
    
    context.write(new Text("begin vertex:"),new FloatWritable((float)iVertex.get()));
    
    for(VertexOrCountWritable v : values){
    	if(v.get() instanceof VertexWritable)
    	    context.write(new Text("Vertex input:"),new FloatWritable( (float)( ((VertexWritable)v.get()) .get())) ) ;
	else
    	    context.write(new Text("Count input:"),new FloatWritable( (float)(((CountWritable)v.get()).get())) ) ;

    	
    	
    	
      numInputs++;
      if(v.get() instanceof VertexWritable){
        VertexWritable jVertex = (VertexWritable)(v.get());
       if(edgeCountEncountered){ //normal operation, write out a pair
          context.write(new Text(Long.toString(jVertex.get()) + " " + Long.toString(iVertex.get())),weightage);
        }
        else{ // We don't know the weightage yet, add to a buffer
          buffer.add(jVertex);
        }
      }
      else{ //type CountWritable, we've found the count, flush the buffer
        edgeCountEncountered = true;
        CountWritable temp = (CountWritable)(v.get());
        weightage.set(1.0f/temp.get());
        
          context.write(new Text("weightage "+Long.toString(iVertex.get())),weightage);
        
        this.flushBuffer(iVertex,weightage,context);
      }
    }
    
	context.write(new Text("Inputs received: "+Long.toString(numInputs)),new FloatWritable((float)iVertex.get()));

  }
}
