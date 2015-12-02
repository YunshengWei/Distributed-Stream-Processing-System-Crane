package crane;

import java.io.Serializable;

public class AckMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * tupleID represents which tuple the ack message belongs to.
     */
    public int tupleID;

    public byte[] checksum;

    public AckMessage(int tupleID, byte[] checksum) {
        this.tupleID = tupleID;
        this.checksum = checksum;
    }
}
