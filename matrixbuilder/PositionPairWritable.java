package matrixbuilder;

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
