package crane.tuple;

import java.util.concurrent.ThreadLocalRandom;

public abstract class BasicTuple implements ITuple {

    private static final long serialVersionUID = 1L;

    protected int tupleID;
    protected Object[] content;
    protected long salt = 0;

    @Override
    public int getID() {
        return tupleID;
    }

    @Override
    public long getSalt() {
        return salt;
    }

    @Override
    public Object[] getContent() {
        return this.content;
    }

    @Override
    public void setSalt() {
        salt = ThreadLocalRandom.current().nextLong();
    }

    @Override
    public void setID(int id) {
        tupleID = id;
    }
}
