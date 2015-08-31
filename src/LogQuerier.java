import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;

/**
 * LogQuerier is a client program which query logs on all servers. The options
 * for LogQuerier are almost the same as Grep (except without specifying files).
 */
public class LogQuerier {

    private static class QueryThread implements Runnable {
        private final Catalog.Host host;
        private final String[] args;

        QueryThread(Catalog.Host host, String[] args) {
            this.host = host;
            this.args = args;
        }

        @Override
        public void run() {
            Socket socket = null;
            try {
                socket = new Socket(host.getIP(), host.getPortNumber());
                System.err.println(host + ": Connected Successfully.");
                try (PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    // Notice: A tricky detail here.
                    // Maybe use json to send args?
                    /*for (int i = 0; i < args.length; i++) {
                        args[i] = "\"" + args[i] + "\"";
                    }*/
                    pw.println("grep " + String.join(" ", args) + " " + Catalog.getLogDirectory());
                    
                    String matchedLine = null;
                    while ((matchedLine = br.readLine()) != null) {
                        System.out.println(host + ":" + matchedLine);
                    }
                }

            } catch (IOException e) {
                System.err.println(host + ": " + e.getMessage());
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                        System.err.println(host + ": Connection closed.");
                    } catch (IOException e) {
                        System.err.println(host + ": Errors occur when closing socket.");
                    }
                }
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
