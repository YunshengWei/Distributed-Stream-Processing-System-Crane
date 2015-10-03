import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;

public class Runner {

    public static void main(String[] args) throws IOException {
        GossipGroupMembershipService ggms = new GossipGroupMembershipService(
                InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        ggms.startServe();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                ggms.stopServe();
            } else if (line.equals("Join group")) {
                ggms.startServe();
            } else if (line.equals("Show membership list")) {
                System.out.println(ggms.getMembershipList());
            } else if (line.equals("Show self id")) {
                System.out.println(ggms.getSelfId());
            }
        }
        in.close();
    }

}
