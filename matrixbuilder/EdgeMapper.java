package matrixbuilder;

public class EdgeMapper
     extends Mapper<VertexWritable, VertexWritable, VertexWritable, VertexWritable>{

  private Configuration conf;

  @Override
  public void map(VertexWritable key, VertexWritable value, Context context
                  ) throws IOException, InterruptedException {
                    context.write(key,value); //echo
  }
}
