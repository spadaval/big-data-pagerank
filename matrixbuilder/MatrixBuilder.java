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

public class MatrixBuilder extends Configured implements Tool{

  public int run(String[] args) throws Exception{
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MatrixBuilder.class);
    job.setReducerClass(MatrixBuilderReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    //FileInputFormat.addInputPath(job, new Path());
    MultipleInputs.addInputPath(job,new Path("/Files/EdgeCount",EdgeCountFileMapper.class));
    MultipleInputs.addInputPath(job,new Path("/Input/edges",EdgeMapper.class));
    FileOutputFormat.setOutputPath(job, new Path("/Output/A"));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int ecode = ToolRunner.run(new multiply(),args);
		System.exit(ecode);
  }
}
