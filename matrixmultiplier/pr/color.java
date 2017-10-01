import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class color
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
	{
		 //private final static LongWritable one = new LongWritable(1);
		 private Text un = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException
		{
			String lines = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(lines,"\n");
			while (tokenizer.hasMoreTokens())
			{
				StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken().toString(),"\t");
				un.set(tokenizer2.nextToken());
				output.collect(un, new LongWritable(Long.parseLong(tokenizer2.nextToken())));
			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException
		{
			long sum = 0;
			while (values.hasNext())
			{
				sum += values.next().get();
			}
			List<String> k = Arrays.asList(key.toString().split(","));
			output.collect(new Text(k.get(0)), new LongWritable(sum));
		}
	}
 	
	public static void main(String[] args) throws Exception
	{

		JobConf conf = new JobConf(color.class);
		//conf.setJarByClass(color.class);
		conf.setJobName("color");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("/Output1"));
		FileOutputFormat.setOutputPath(conf, new Path("Output2"));
		
		
		JobClient.runJob(conf);
		
	}
}
