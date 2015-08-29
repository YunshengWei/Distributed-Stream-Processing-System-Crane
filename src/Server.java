import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
