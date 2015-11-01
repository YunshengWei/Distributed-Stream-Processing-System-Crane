package sdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

public class SDFSService implements DaemonService {

    

    private GossipGroupMembershipService ggms;
    private ServerSocket serverSocket;
    private Identity nameNode;

    private final static Logger LOGGER = initializeLogger();

    private static Logger initializeLogger() {
        return null;
    }

    public Identity getNameNode() {
        return nameNode;
    }

    public SDFSService(InetAddress introducerIP) {
        ggms = new GossipGroupMembershipService(introducerIP);
    }

    @Override
    public void startServe() throws IOException {
        ggms.startServe();
        serverSocket = new ServerSocket(Catalog.SDFS_DATANODE_PORT);
        new Thread(new DataNodeWorker()).start();
    }

    @Override
    public void stopServe() {
        ggms.stopServe();

    }

    public static void main(String[] args) throws IOException {
        SDFSService sdfss = new SDFSService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        sdfss.startServe();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                sdfss.ggms.stopServe();
            } else if (line.equals("Join group")) {
                sdfss.ggms.startServe();
            } else if (line.equals("Show membership list")) {
                System.out.println(sdfss.ggms.getMembershipList());
            } else if (line.equals("Show self id")) {
                System.out.println(sdfss.ggms.getSelfId());
            } else if (line.startsWith("put")) {
                String[] parts = line.split("\\s+");
                String localFileName = parts[1];
                String sdfsFileName = parts[2];
                // TODO
            } else if (line.startsWith("get")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                String localFileName = parts[2];
                // TODO
            } else if (line.startsWith("delete")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                // TODO
            } else if (line.equals("store")) {
                System.out.println(sdfss.getSDFSFiles());
            } else if (line.startsWith("list")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                // TODO
            }
        }
        in.close();
    }
}
