package system;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Identity is an immutable class that uniquely identifies a member's identity.
 * It is composed of an IP address field, a port field, and a timestamp field.
 */
public final class Identity implements Serializable, Comparable<Identity> {
    private static final long serialVersionUID = 1L;

    public final InetAddress IPAddress;
    public final long timestamp;
    public final int port;
    private transient Integer hashCache = null;

    public Identity(InetAddress IPAddress, long timestamp, int port) {
        this.IPAddress = IPAddress;
        this.timestamp = timestamp;
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof Identity)) {
            return false;
        } else {
            Identity that = (Identity) obj;
            return this.IPAddress.equals(that.IPAddress) && this.timestamp == that.timestamp
                    && this.port == that.port;
        }
    }

    @Override
    public String toString() {
        return String.format("%s:%s<%s>", IPAddress.toString(), port, timestamp);
    }

    @Override
    public int compareTo(Identity that) {
        return this.toString().compareTo(that.toString());
    }

    public int compareToByIP(Identity that) {
        return this.IPAddress.toString().compareTo(that.IPAddress.toString());
    }

    @Override
    public int hashCode() {
        if (hashCache == null) {
            hashCache = toString().hashCode();
        }
        return hashCache;
    }
}
