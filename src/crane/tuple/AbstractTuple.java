package crane.tuple;

import java.util.concurrent.ThreadLocalRandom;

import system.Catalog;

public abstract class AbstractTuple implements ITuple {

    private static final long serialVersionUID = 1L;

    protected int tupleID;
    protected Object[] content;
    private final byte[] salt = new byte[Catalog.CHECKSUM_LENGTH];

    @Override
    public int getID() {
        return tupleID;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public Object[] getContent() {
        return this.content;
    }

    @Override
    public void setSalt() {
        ThreadLocalRandom.current().nextBytes(this.salt);
    }
}
