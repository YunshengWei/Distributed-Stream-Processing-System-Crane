public class Catalog {
    // public static final int LOGDIR = "log/";
    public String hostName = "";
    public String portName = "";

    public Catalog(String hostName, String portName) {
        this.hostName = hostName;
        this.portName = portName;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getPortName() {
        return portName;
    }

    public void setPortName(String portName) {
        this.portName = portName;
    }

    @Override
    public String toString() {
        return "Catalog [hostName=" + hostName + ", portName=" + portName + "]";
    }

}
