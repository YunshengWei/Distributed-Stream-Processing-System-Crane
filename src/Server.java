import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * This is the main class for Server. ServerThread is a thread within the
 * Server. Its main task is to take the socket, bind and listen on it. It also
 * calls execute function of Grep class to run grep functionality.
 **/

public class Server {
    private final ServerSocket serverSocket;

    private class ServerThread implements Runnable {
        private final Socket socket;

        public ServerThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), Catalog.encoding));
                    OutputStream os = socket.getOutputStream()) {
                String line;
                while ((line = br.readLine()) != null) {
                    // We can always assume received message is valid,
                    // since we are only allowed to be contacted by
                    // client programs, and any mistakes should be detected
                    // there.
                    JSONArray cmd = (JSONArray) JSONValue.parse(line);
                    switch ((String) cmd.get(0)) {
                    case "grep":
                        @SuppressWarnings("unchecked")
                        String[] args = (String[]) cmd.subList(1, cmd.size()).toArray(new String[0]);
                        ;
                        new Grep(args, os).execute();
                        return;
                    default:
                        // Should never reach here.
                        System.err.println(String.format("Unsupported Operation: %s.", cmd.get(0)));
                        return;
                    }
                }
            } catch (IOException | ParseException e) {
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
