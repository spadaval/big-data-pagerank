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

public class MatrixBuilder {

  public static class VertexWritable extends IntWritable{
    VertexWritable(){
      super();
    }
  }

  public static class CountWritable extends IntWritable{
    CountWritable(){
      super();
    }
  }
  //wrapper class for inputs to the reducer
  public static class VertexOrCountWritable extends GenericWritable{
    private static Class[] CLASSES = {VertexWritable.class,CountWritable.class};
    protected static Class[] getTypes(){
      return CLASSES;
    }
  }

  public static class PositionPairWritable implements WritableComparable{
    // Some data
       private int i;
       private long j;

       PositionPairWritable(int i, int j){
         this.i = i;
         this.j = j;
       }

       public void write(DataOutput out) throws IOException {
         out.writeInt(i);
         out.writeInt(j);
       }

       public void readFields(DataInput in) throws IOException {
         i = in.readInt();
         j = in.readInt();
       }

       public int compareTo(PositionPairWritable o) {
        if(this.i<o.i)
          return -1;
        else if(this.i>o. i)
            return 1;
          else if(this.j<o.j)
              return -1;
            else if(this.j>o.j)
                return 1;
              else if(this.i==o.i && this.j==o.j)return 0;
       }

       public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + j;
         result = prime * result + (int) (i ^ (i >>> 32));
         return result;
       }
  }


  public static class EdgeMapper
       extends Mapper<VertexWritable, VertexWritable, VertexWritable, VertexWritable>{

    private Configuration conf;

    @Override
    public void map(VertexWritable key, VertexWritable value, Context context
                    ) throws IOException, InterruptedException {
                      context.write(key,value); //echo
    }
  }

  public static class EdgeCountMapper
       extends Mapper<VertexWritable, CountWritable, VertexWritable, CountWritable>{

    private Configuration conf;

    @Override
    public void map(VertexWritable key, VertexWritable value, Context context
                    ) throws IOException, InterruptedException {
                      context.write(key,value); //echo
    }
  }

  public static class MatrixBuilderReducer
       extends Reducer<VertexWritable,VertexOrCountWritable,PositionPairWritable,FloatWritable> {

    private ArrayList<VertexWritable> iVertices = new ArrayList<VertexWritable>();
    private int edgeCount;
    private boolean edgeCountEncountered;

    private void flushBuffer(VertexWritable iVertex,Context context){
      for(VertexWritable jVertex: iVertices)
        context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
    }
    //Input a Interable of either destination vertex or edgecount (for the given source vertex)
    public void reduce(VertexWritable iVertex, Iterable<VertexOrCountWritable> values, Context context) throws IOException, InterruptedException {
      for(VertexOrCountWritable v:values){
        if(v instanceof VertexWritable){
          VertexWritable jVertex = (VertexWritable)(v);
          if(edgeCountEncountered){
            context.write(new PositionPairWritable(jVertex.get(),iVertex.get()),new FloatWritable(1/this.edgeCount));
          }
          else{
            iVertices.add(x);
          }
        }
        else{ //type CountWritable
          this.edgeCountEncountered = true;
          CountWritable temp = (CountWritable)(x);
          this.edgeCount = temp.get();
          this.flushBuffer(iVertex,context);
        }
      }
    }

  }

  public static void main(String[] args) throws Exception {
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

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
