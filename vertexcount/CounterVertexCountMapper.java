import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterVertexCountMapper
     extends Mapper<LongWritable, Text, LongWritable, LongWritable>{

  private final static LongWritable one = new LongWritable(1);

  public void map(LongWritable lineno, Text value, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(1),new LongWritable(1));
  }
}
