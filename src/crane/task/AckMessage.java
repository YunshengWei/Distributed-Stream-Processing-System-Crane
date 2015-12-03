package crane.task;

import java.io.Serializable;

public class AckMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * tupleID represents which tuple the ack message belongs to.
     */
    public final int tupleID;

    public final long checksum;

    public AckMessage(int tupleID, long checksum) {
        this.tupleID = tupleID;
        this.checksum = checksum;
    }
}
