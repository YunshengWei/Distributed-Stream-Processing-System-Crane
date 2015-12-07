package crane.tuple;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import system.Catalog;

public class OneStringTuple extends BasicTuple {

    private static final long serialVersionUID = 1L;

    public OneStringTuple(int tupleID, String content) {
        this.tupleID = tupleID;
        this.content = new Object[1];
        this.content[0] = content;
    }

    public OneStringTuple(byte[] bytes) throws UnsupportedEncodingException {
        this.tupleID = ByteBuffer.wrap(bytes, 0, 4).getInt();
        this.salt = ByteBuffer.wrap(bytes, 4, 8).getLong();
        this.content = new Object[1];
        this.content[0] = new String(Arrays.copyOfRange(bytes, 12, bytes.length), "UTF-8");
    }

    @Override
    public int hashCode() {
        return this.content[0].hashCode();
    }

    public String getString() {
        return (String) this.content[0];
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = null;
        try {
            byte[] stringBytes = getString().getBytes(Catalog.ENCODING);
            bb = ByteBuffer.allocate(12 + stringBytes.length);
            bb.putInt(tupleID);
            bb.putLong(salt);
            bb.put(stringBytes, 0, stringBytes.length);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return bb.array();
    }
}
