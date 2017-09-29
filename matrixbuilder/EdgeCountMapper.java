package matrixbuilder;

public class EdgeCountMapper
     extends Mapper<VertexWritable, CountWritable, VertexWritable, CountWritable>{

  private Configuration conf;

  @Override
  public void map(VertexWritable key, VertexWritable value, Context context
                  ) throws IOException, InterruptedException {
                    context.write(key,value); //echo
  }
}
