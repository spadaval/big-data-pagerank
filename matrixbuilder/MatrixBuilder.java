
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

public class MatrixBuilder extends Configured implements Tool{

  public int run(String[] args) throws Exception{
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(MatrixBuilder.class);
    job.setReducerClass(MatrixBuilderReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setMapOutputKeyClass(VertexWritable.class);
    job.setMapOutputValueClass(VertexOrCountWritable.class);
    
    conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
    FileInputFormat.setInputDirRecursive(job, true);

    //FileInputFormat.addInputPath(job, new Path());
    MultipleInputs.addInputPath(job,new Path("/pagerank/Files/EdgeCount"),TextInputFormat.class,EdgeCountMapper.class);
    MultipleInputs.addInputPath(job,new Path("/pagerank/Input/Edges"),TextInputFormat.class,EdgeMapper.class);
    FileOutputFormat.setOutputPath(job, new Path("/pagerank/Output/A"));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int ecode = ToolRunner.run(new MatrixBuilder(),args);
		System.exit(ecode);
  }
}
