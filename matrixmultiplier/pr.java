import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class pr
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		 //private final static IntWritable one = new IntWritable(1);
		 private Text un = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			String lines = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(lines,"\n");
			while (tokenizer.hasMoreTokens())
			{
				StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken().toString());
				un.set(tokenizer2.nextToken());
				output.collect(un, new IntWritable(tokenizer2.nextToken()));
			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			int sum = 0;
			while (values.hasNext())
			{
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
 	
	public static void main(String[] args) throws Exception
	{

		JobConf conf = new JobConf(pr.class);
		conf.setJobName("pr");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("Output"));
		FileOutputFormat.setOutputPath(conf, new Path("Output2"));
		
		
		JobClient.runJob(conf);
		
	}
}
