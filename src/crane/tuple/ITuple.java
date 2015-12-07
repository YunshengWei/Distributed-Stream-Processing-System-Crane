package crane.tuple;

import java.io.Serializable;

public interface ITuple extends Serializable {
    int getID();

    void setID(int id);

    long getSalt();

    Object[] getContent();

    void setSalt();

    /**
     * Use this instead of Java's default Serialization mechanism will give huge
     * performance improvement
     */
    byte[] toBytes();
}
