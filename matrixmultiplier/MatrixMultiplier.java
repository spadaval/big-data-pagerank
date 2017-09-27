//INCOMPLETE
import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MatrixMultiplier
{
	public static String keyword = "input";
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		 private Text un = new Text();
		 private Text url = new Text();
		 private Text b = new Text();
		int rep = 2;
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{

			String lines = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(lines,"\n");
			while (tokenizer.hasMoreTokens())
			{
				int i,itr;
				StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken().toString());
				int a = Integer.parseInt(tokenizer2.nextToken());
				int b = Integer.parseInt(tokenizer2.nextToken());
				if(tokenizer2.hasMoreTokens())
				{
					//int b = Integer.parseInt(tokenizer2.nextToken());
					int v = Integer.parseInt(tokenizer2.nextToken());
					for(i=1;i<=rep;i++)
					{
						itr = i;
						output.collect(new Text((new IntWritable(a)).toString()+" "+Integer.toString(itr)+" "+(new IntWritable(b)).toString()), new IntWritable(v));
					}
				}
				else
				{
					for(i=1;i<=rep;i++)
					{
						itr = i;
						int v = b;
						output.collect(new Text((Integer.toString(itr)+" "+new IntWritable(1)).toString()+" "+(new IntWritable(a)).toString()), new IntWritable(v));
					}
				}
			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			int sum = 1;
			int count = 0;
			while (values.hasNext())
			{
				sum *= values.next().get();
				count = count+1;
			}

			Text c = new Text(key+" : "+Integer.toString(count));
			if(count>1)
			{
				System.out.print("\n\n"+Integer.toString(count)+" : "+sum+"\n\n");
			}
				output.collect(new Text(key+" "+Integer.toString(count)), new IntWritable(sum));
				count = 0;
		}
	}
	public static void main(String[] args) throws Exception
	{

		JobConf conf = new JobConf(MatrixMultiplier.class);
		conf.setJobName("web");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/Input"));
		FileOutputFormat.setOutputPath(conf, new Path("/Output"));
		JobClient.runJob(conf);

	}
}
