import java.io.Serializable;
import java.net.InetAddress;

public final class Address implements Serializable, Comparable<Address> {
    private static final long serialVersionUID = 1L;
    
    public final InetAddress IP;
    public final int port;
    private transient Integer hashCache = null;

    Address(InetAddress IP, int port) {
        this.IP = IP;
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof Address)) {
            return false;
        } else {
            Address that = (Address) obj;
            return this.IP.equals(that.IP) && this.port == that.port;
        }
    }

    @Override
    public String toString() {
        return IP.toString() + ":" + port;
    }

    @Override
    public int compareTo(Address that) {
        return this.toString().compareTo(that.toString());
    }

    @Override
    public int hashCode() {
        if (hashCache == null) {
            hashCache = toString().hashCode();
        }
        return hashCache;
    }
}
