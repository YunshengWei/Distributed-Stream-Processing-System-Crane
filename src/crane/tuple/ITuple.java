package crane.tuple;

import java.io.Serializable;

public interface ITuple extends Serializable {
    int getID();

    long getSalt();

    Object[] getContent();
    
    void setSalt();
}
