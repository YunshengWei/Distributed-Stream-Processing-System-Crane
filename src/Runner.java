import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;

public class Runner {

    public static void main(String[] args) throws IOException {
        // LogQueryService lqs =
        // LogQueryService.create(Catalog.LOG_QUERY_SERVICE_PORT);
        // lqs.startServe();
        GossipGroupMembershipService ggms = new GossipGroupMembershipService(
                InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        ggms.startServe();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                ggms.stopServe();
            } else if (line.equals("Stop log query service")) {
                // lqs.stopServe();
            } else if (line.equals("Join group")) {
                ggms.startServe();
            }
        }
        in.close();
    }

}
