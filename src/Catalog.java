import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog stores system information.
 */
public class Catalog {
    public static class Host {
        private final String IP;
        private final int portNumber;
        private final String hostName;

        public Host(String IP, int portNumber, String hostName) {
            this.IP = IP;
            this.portNumber = portNumber;
            this.hostName = hostName;
        }

        public String getIP() {
            return this.IP;
        }

        public int getPortNumber() {
            return this.portNumber;
        }

        public String getHostName() {
            return this.hostName;
        }

        @Override
        public String toString() {
            return String.format("%s(%s:%s)", hostName, IP, portNumber);
        }
    }

    private static final String LOGDIR = "log/iamlogfile";
    private static final List<Host> hostList;
    private static final Map<String, Host> hostNameMap;

    // Modify here to hard code hosts information
    static {
        Host host1 = new Host("127.0.0.1", 60001, "machine1");
        Host host2 = new Host("127.0.0.1", 60002, "machine2");
        //Host host3 = new Host("127.0.0.1", 40000, "machine3");
        //Host host4 = new Host("127.0.0.1", 40000, "machine4");
        //Host host5 = new Host("127.0.0.1", 40000, "machine5");
        hostList = Arrays.asList(host1, host2);//, host3, host4, host5);

        hostNameMap = new HashMap<>();
        for (Host host : hostList) {
            hostNameMap.put(host.hostName, host);
        }
    }

    public static List<Host> getHosts() {
        return hostList;
    }

    public static int getNumHosts() {
        return hostList.size();
    }

    public static Host getHostByName(String hostName) {
        return hostNameMap.get(hostName);
    }

    public static String getLogDirectory() {
        return LOGDIR;
    }
}
