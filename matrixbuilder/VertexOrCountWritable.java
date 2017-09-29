package matrixbuilder;

public class VertexOrCountWritable extends GenericWritable{
  private static Class[] CLASSES = {VertexWritable.class,CountWritable.class};
  protected static Class[] getTypes(){
    return CLASSES;
  }
}
