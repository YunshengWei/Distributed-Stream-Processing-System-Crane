import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Server {
    private final ServerSocket serverSocket;

    private class ServerThread implements Runnable {
        private final Socket socket;

        public ServerThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    OutputStream output = socket.getOutputStream()) {
                String line;
                while ((line = br.readLine()) != null) {
                    // We can always assume received message is valid,
                    // because we are only allowed to be contacted by
                    // client programs, and any mistakes should be detected
                    // there.
                    String[] cmd = line.split("\\s+");
                    if (cmd[0].equals("grep")) {
                        String[] args = Arrays.copyOfRange(cmd, 1, cmd.length);
                        CommandLineParser parser = new DefaultParser();
                        Options options = new Options();
                        
                        CommandLine cmdl;
                        try {
                            cmdl = parser.parse(options, args);
                            new Grep(cmdl, output);
                        } catch (ParseException e) {
                            // Should never reach here
                            e.printStackTrace();
                        }
                        
                        // should close it here or in client?
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public Server(int portNumber) throws IOException {
        serverSocket = new ServerSocket(portNumber);
    }

    public void serve() throws IOException {
        while (true) {
            Socket socket = serverSocket.accept();
            (new Thread(new ServerThread(socket))).start();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java Server <port number>");
            System.exit(-1);
        }

        int portNumber = Integer.parseInt(args[0]);
        Server server = new Server(portNumber);
        server.serve();
    }

}
