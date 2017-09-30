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

public class PositionPairWritable implements WritableComparable{
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
