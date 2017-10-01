package matrixbuilder;
//import mutipleInput.Join;
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

public class PositionPairWritable implements WritableComparable{
  // Some data
     private long i;
     private long j;

     PositionPairWritable(long i, long j){
       this.i = i;
       this.j = j;
     }

     public void write(DataOutput out) throws IOException {
       out.writeLong(i);
       out.writeLong(j);
     }

     public void readFields(DataInput in) throws IOException {
       i = in.readLong();
       j = in.readLong();
     }
     @Override
     public int compareTo(Object object) {
      if(!(object instanceof PositionPairWritable))
        return -1;

      PositionPairWritable o = (PositionPairWritable)object;

      if(this.i<o.i)
        return -1;
      if(this.i>o.i)
        return 1;
      if(this.i==o.i && this.j==o.j){
        if(this.j>o.j)
          return 1;
        if(this.j<o.j)
          return -1;
        if(this.j==o.j)
          return 0;
      }
     }

     public int hashCode() {
       final long prime = 31;
       long result = 1;
       result = prime * result + j;
       result = prime * result + (int) (i ^ (i >>> 32));
       return (int)result;
     }
}
