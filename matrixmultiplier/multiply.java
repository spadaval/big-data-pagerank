//import mutipleInput.Join;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
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

public class multiply extends Configured implements Tool
{
	public static class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lines = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(lines,"\n");
			while (tokenizer.hasMoreTokens())
			{
				int i,itr;
				StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken().toString());
				int a = Integer.parseInt(tokenizer2.nextToken());
				int b = Integer.parseInt(tokenizer2.nextToken());
				int v = Integer.parseInt(tokenizer2.nextToken());
				for(i=1;i<=2;i++)
				{
					itr = i;
					context.write(new Text((new IntWritable(a)).toString()+","+Integer.toString(itr)+","+(new IntWritable(b)).toString()), new IntWritable(v));
				}
			}
		}
	}

	public static class CountertwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lines = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(lines,"\n");
			while (tokenizer.hasMoreTokens())
			{
				int i,itr;
				StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken().toString());
				int a = Integer.parseInt(tokenizer2.nextToken());
				int b = 1;
				int v = Integer.parseInt(tokenizer2.nextToken());
				for(i=1;i<=2;i++)
				{
					itr = i;
					context.write(new Text(Integer.toString(itr)+","+(new IntWritable(b)).toString()+","+(new IntWritable(a)).toString()), new IntWritable(v));
				}
			}
		}
	}

	public static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		String line=null;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 1;
			int count = 0;
			for (IntWritable val : values)
			{
				sum *= val.get();
				count += 1;
			}
			System.out.print("\n\n"+count+"\n\n");
			if(count>1)
			{
				List<String> k = Arrays.asList(key.toString().split(","));
				context.write(new Text(k.get(0)+","+k.get(1)), new IntWritable(sum));
			}
		}
	}


	 public int run(String[] args) throws Exception
	 {
		 Configuration conf = new Configuration();
		 //JobConf job = new JobConf(multiply.class);
		 Job job = Job.getInstance();
		 job.setJarByClass(multiply.class);
		 MultipleInputs.addInputPath(job,new Path("/Input1"),TextInputFormat.class,CounterMapper.class);
		 MultipleInputs.addInputPath(job,new Path("/Input2"),TextInputFormat.class,CountertwoMapper.class);

		 FileOutputFormat.setOutputPath(job, new Path("Output"));
		 job.setReducerClass(CounterReducer.class);
		 job.setNumReduceTasks(1);
		 job.setMapOutputKeyClass(Text.class);
		 //job.setMapOutputValueClass(IntWritable.class);
		 //job.setOutputKeyClass(Text.class);
		 job.setOutputKeyClass(LongWritable.class);
		 job.setOutputValueClass(IntWritable.class);

		 return (job.waitForCompletion(true) ? 0 : 1);

	 }

 	public static void main(String[] args) throws Exception
 	{
 		
		int ecode = ToolRunner.run(new multiply(),args);
		System.exit(ecode);

		/*
		JobConf conf = new JobConf(multiply.class);
		MultipleInputs.addInputPath(job,new Path("/Input1"),TextInputFormat.class,CounterMapper.class);
		 MultipleInputs.addInputPath(job,new Path("/Input2"),TextInputFormat.class,CountertwoMapper.class);
		conf.setJobName("web");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/Input"));
		FileOutputFormat.setOutputPath(conf, new Path("Output"));
		JobClient.runJob(conf);
		*/
	 }
}
