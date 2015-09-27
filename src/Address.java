import java.io.Serializable;
import java.net.InetAddress;

public final class Address implements Serializable, Comparable<Address> {
    private static final long serialVersionUID = 1L;
    
    public final InetAddress IP;
    public final int port;
    private transient final String stringRepresentation;

    Address(InetAddress IP, int port) {
        this.IP = IP;
        this.port = port;
        this.stringRepresentation = IP.toString() + ":" + port;
    }

    @Override
    public boolean equals(Object that) {
        if (that == this) {
            return true;
        } else if (!(that instanceof Address)) {
            return false;
        } else {
            return this.toString().equals(((Address) that).toString());
        }
    }

    @Override
    public String toString() {
        return stringRepresentation;
    }

    @Override
    public int compareTo(Address that) {
        return this.toString().compareTo(that.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
