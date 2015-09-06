import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog stores information about the whole system.
 */
public class Catalog {

    /**
     * Host stores information about a host, including host name, IP address,
     * and port number.
     */
    public static class Host {
        private final String IP;
        private final int portNumber;
        private final String hostName;

        public Host(String hostName, String IP, int portNumber) {
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
            return String.format("<%s[%s:%s]>", hostName, IP, portNumber);
        }
    }

    /** the directory where log files are stored */
    private static final String LOGDIR = "log";
    /** the path of the file which keeps host information */
    private static final String HOST_FILE_PATH = "conf/host_list";

    private static final List<Host> hostList = buildHostList();
    private static final Map<String, Host> hostNameMap = buildHostNameMap();

    /** Build host list from file */
    private static List<Host> buildHostList() {
        try (BufferedReader br = new BufferedReader(new FileReader(Catalog.HOST_FILE_PATH))) {

            List<Host> hostList = new ArrayList<>();
            // the first line is description, just skip it
            br.readLine();
            String line = null;

            while ((line = br.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                
                String[] words = line.split("\\s+");
                // the format should be in this order:
                // <host name> <IP address> <port number>
                String hostName = words[0];
                String IP = words[1];
                int portNumber = Integer.parseInt(words[2]);
                hostList.add(new Host(hostName, IP, portNumber));
            }

            return hostList;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        // Should never reach here.
        return null;
    }

    /** Build the mapping between host name and host object */
    private static Map<String, Host> buildHostNameMap() {
        Map<String, Host> hostNameMap = new HashMap<>();
        for (Host host : hostList) {
            hostNameMap.put(host.getHostName(), host);
        }
        return hostNameMap;
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

    // unit test
    public static void main(String[] args) {
        System.out.println(hostList);
        System.out.println(getNumHosts());
    }
}
