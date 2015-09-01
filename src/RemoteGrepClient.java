import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.commons.cli.ParseException;

/**
 * RemoteGrepClient is a client program which sends `grep' command to servers,
 * and output matched lines received from servers to standard output. The
 * command options is the same as Grep.
 */
public class RemoteGrepClient {

    private static class QueryThread implements Runnable {
        private final Catalog.Host host;
        private final String[] args;

        QueryThread(Catalog.Host host, String[] args) {
            this.host = host;
            this.args = args;
        }

        /**
         * Attempt to connect the specified host.
         * 
         * @return the connected socket if succeed, null otherwise.
         */
        private Socket connect() {
            try {
                Socket socket = new Socket(host.getIP(), host.getPortNumber());
                System.err.println(String.format("%s: Connection set up successfully.", host));
                return socket;
            } catch (IOException e) {
                System.err.println(String.format("%s: Failed to establish connection.", host));
            }
            return null;
        }

        /**
         * Send `grep' query over the specified socket, and output received
         * responses to standard output.
         * 
         * @param socket
         *            requires the socket not null and open.
         */
        private void executeQuery(Socket socket) {
            try (PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                // Notice: A tricky detail here.
                // Use json to send args
                // TODO
                pw.println("grep " + String.join(" ", args) + " " + Catalog.getLogDirectory());

                String matchedLine = null;
                while ((matchedLine = br.readLine()) != null) {
                    System.out.println(String.format("%s:%s", host, matchedLine));
                }
            } catch (IOException e) {
                System.err.println(String.format("%s: %s", host, e.getMessage()));
            }
        }

        private void close(Socket socket) {
            try {
                // flush System.out to ensure log lines appear after regular
                // lines.
                System.out.flush();
                socket.close();
            } catch (IOException e) {
            } finally {
                System.err.println(String.format("%s: Connection closed", host));
            }
        }

        @Override
        public void run() {
            Socket socket = connect();

            if (socket != null) {
                executeQuery(socket);
                close(socket);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // args errors are detected here,
        // so no invalid commands will be sent to other servers.
        try {
            Grep grep = new Grep(args, null);

            for (Catalog.Host host : Catalog.getHosts()) {
                new Thread(new QueryThread(host, args)).start();
            }
        } catch (ParseException e) {
            System.err.println("Invalid argument.");
            Grep.printUsage();
            System.exit(-1);
        }
    }

}
