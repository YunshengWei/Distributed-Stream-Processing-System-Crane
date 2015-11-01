package sdfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import system.DaemonService;

/**
 * SDFSNameNodeService is responsible for the the task of a name node in SDFS.
 */
public class SDFSNameNodeService implements DaemonService {
    
    private ServerSocket serverSocket;
    
    private class SDFSNameNodeWorker implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    handle(socket);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }
        
        private void executePutRequest(Socket socket, byte[] command) {
            
        }
        
        private void executeGetRequest(Socket socket, byte[] command) {
            
        }
        
        private void executeDelete(Socket socket, byte[] command) {
            
        }
        
        private void executeBlockReport(Socket socket, byte[] command) {
            
        }
        
        private void handle(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = SDFSUtils.readAllBytes(socket);
                
            } finally {
                socket.close();
            }
        }
        
    }

    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub
        
    }

}
