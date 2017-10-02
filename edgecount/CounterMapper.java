//package edgecount;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterMapper
     extends Mapper<LongWritable, Text, LongWritable, LongWritable>{

  private final static LongWritable one = new LongWritable(1);
  private static LongWritable v = new LongWritable();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        v.set(((new Scanner(value.toString())).nextLong()));
        context.write(v,one);
  }
}
