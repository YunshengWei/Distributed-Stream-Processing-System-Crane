import java.io.IOException;

public interface DaemonService {
    /**
     * Start the service as a daemon service, i.e. in other threads, and the
     * method will return immediately.
     * 
     * @throws IOException
     *             if IO error occurs.
     */
    public void startServe() throws IOException;

    /**
     * Stop the service.
     */
    public void stopServe();
}
