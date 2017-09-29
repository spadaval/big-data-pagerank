package matrixbuilder;

public class MatrixBuilderReducer
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
