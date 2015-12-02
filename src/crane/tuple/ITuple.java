package crane.tuple;

import java.io.Serializable;

public interface ITuple extends Serializable {
    int getID();

    byte[] getSalt();

    Object[] getContent();
    
    void setSalt();
}
