import java.util.concurrent.TimeUnit;

/**
 * Catalog stores parameters about the whole system.
 */
public class Catalog {

    /** Settings for gossip group membership service */

    /** T_cleanup in milliseconds */
    public static final long CLEANUP_TIME = 4000;
    /** T_fail in milliseconds */
    public static final long FAIL_TIME = 1500;
    /** gap between gossiping membership list, measured in milliseconds */
    public static final long GOSSIP_GAP = 300;
    /** gap between negotiating with introducer, measured in milliseconds */
    public static final long INTRODUCER_NEGOTIATE_GAP = 2000;
    /** specify the time unit */
    public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    /** specify the max packet bytes required for receiving membership list */
    public static final int MAX_UDP_PACKET_BYTES = 5000;
    /** specify the number of leave messages to gossip when voluntarily leave */
    public static final int NUM_LEAVE_GOSSIP = 1;
    /** specify the address of introducer */
    public static final String INTRODUCER_ADDRESS = "fa15-cs425-g13-01.cs.illinois.edu";
    /** specify the port number on which membership service is running */
    public static final int MEMBERSHIP_SERVICE_PORT = 60003;
    /** specify the log to which membership service writes */
    public static final String MEMBERSHIP_SERVICE_LOG = "ms.log";

    /** Settings for log query service */

    /** specify the port number on which log query service is running */
    public static final int LOG_QUERY_SERVICE_PORT = 60001;

    /** Settings for remote grep client */
    public static final String[] HOST_LIST = new String[] { "fa15-cs425-g13-01.cs.illinois.edu",
            "fa15-cs425-g13-02.cs.illinois.edu", "fa15-cs425-g13-03.cs.illinois.edu",
            "fa15-cs425-g13-04.cs.illinois.edu", "fa15-cs425-g13-05.cs.illinois.edu",
            "fa15-cs425-g13-06.cs.illinois.edu", "fa15-cs425-g13-07.cs.illinois.edu" };

    /** other global settings */

    /** specify the root directory for logs */
    public static final String LOG_DIR = "log/";
    /** Specify the character encoding used by the whole system */
    public static final String encoding = "UTF-8";
}

// /** the path of the file which keeps host information */
// private static final String HOST_FILE_PATH = "conf/host_list";
//
// /**
// * Host stores information about a host, including host name, IP address,
// * and port number.
// */
// public static class Host {
// private final String IP;
// private final int portNumber;
// private final String hostName;
//
// public Host(String hostName, String IP, int portNumber) {
// this.IP = IP;
// this.portNumber = portNumber;
// this.hostName = hostName;
// }
//
// public String getIP() {
// return this.IP;
// }
//
// public int getPortNumber() {
// return this.portNumber;
// }
//
// public String getHostName() {
// return this.hostName;
// }
//
// @Override
// public String toString() {
// return String.format("<%s[%s:%s]>", hostName, IP, portNumber);
// }
// }
//
//
// private static final List<Host> hostList = buildHostList();
// private static final Map<String, Host> hostNameMap = buildHostNameMap();
//
// /** Specify the character encoding used by the whole system */
// public static final String encoding = "UTF-8";
//
// /** Build host list from file */
// private static List<Host> buildHostList() {
// try (BufferedReader br = new BufferedReader(new
// FileReader(Catalog.HOST_FILE_PATH))) {
//
// List<Host> hostList = new ArrayList<>();
// // the first line is description, just skip it
// br.readLine();
// String line = null;
//
// while ((line = br.readLine()) != null) {
// if (line.trim().length() == 0) {
// continue;
// }
//
// String[] words = line.split("\\s+");
// // the format should be in this order:
// // <host name> <IP address> <port number>
// String hostName = words[0];
// String IP = words[1];
// int portNumber = Integer.parseInt(words[2]);
// hostList.add(new Host(hostName, IP, portNumber));
// }
//
// return hostList;
// } catch (Exception e) {
// e.printStackTrace();
// System.exit(-1);
// }
//
// // Should never reach here.
// return null;
// }
//
// /** Build the mapping between host name and host object */
// private static Map<String, Host> buildHostNameMap() {
// Map<String, Host> hostNameMap = new HashMap<>();
// for (Host host : hostList) {
// hostNameMap.put(host.getHostName(), host);
// }
// return hostNameMap;
// }
//
// public static List<Host> getHosts() {
// return hostList;
// }
//
// public static int getNumHosts() {
// return hostList.size();
// }
//
// public static Host getHostByName(String hostName) {
// return hostNameMap.get(hostName);
// }
//
// // unit test
// public static void main(String[] args) {
// System.out.println(hostList);
// System.out.println(getNumHosts());
// }
