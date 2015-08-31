import java.io.*;
import java.net.*;
import java.util.*;

public class ClientLogQuerier {

    static ArrayList<Catalog> machine_info;
    Socket socketClient = null;

    // reading host name and port number and storing in an arraylist of class
    // Catalog
    public ClientLogQuerier(String filePath) {
        try {
            Catalog c = null;
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            String line;
            machine_info = new ArrayList<Catalog>();
            while ((line = br.readLine()) != null) {
                String[] ar = line.split(",");
                c = new Catalog(ar[0], ar[1]);
                c.setHostName(ar[0]);
                c.setPortName(ar[1]);
                machine_info.add(c);
            }
            System.out.println(machine_info);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // connecting to server
    public void connect(String hostName, String portName)
            throws UnknownHostException, IOException {
        System.out.println("Connecting to " + hostName + " " + portName);
        socketClient = new Socket(hostName, Integer.parseInt(portName));
        System.out.println("Connection Established to" + hostName);

    }

    // reading response from server
    public void readResponsefromServer() throws IOException {
        String userInput;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(
                socketClient.getInputStream()));

        System.out.print("RESPONSE FROM SERVER:");
        while ((userInput = stdIn.readLine()) != null) {
            System.out.println(userInput);
        }
    }

    // sending query to server
    public void grepQuery() throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                socketClient.getOutputStream()));
        writer.write("grep?");
        writer.newLine();
        writer.flush();
    }

  
    public static void main(String[] args) throws IOException {

        // Creating a ClientLogQuerier object
        ClientLogQuerier c = new ClientLogQuerier("src\\machineinfo");
        try {
            for (Catalog c1 : machine_info) {
                // trying to establish connection to server
                c.connect(c1.getHostName(), c1.getPortName());
                // sending query to server
                c.grepQuery();
                // waiting to read response from server
                c.readResponsefromServer();

            }
        } catch (UnknownHostException e) {
            System.err.println("Host unknown. Cannot establish connection");
        } catch (IOException e) {
            System.err
                    .println("Cannot establish connection. Server may not be up."
                            + e.getMessage());
        }

    }

}
