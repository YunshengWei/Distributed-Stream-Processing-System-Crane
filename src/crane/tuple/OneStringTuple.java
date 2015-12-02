package crane.tuple;

public class OneStringTuple extends AbstractTuple {

    private static final long serialVersionUID = 1L;

    public OneStringTuple(int tupleID, String content) {
        this.tupleID = tupleID;
        this.content = new Object[1];
        this.content[0] = content;
    }
    
    @Override
    public int hashCode() {
        return this.content[0].hashCode();
    }
}
