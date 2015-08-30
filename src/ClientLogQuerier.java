import java.io.*;
import java.nio.channels.*;
import java.util.*;


public class ClientLogQuerier {
	
	ArrayList<Catalog> machine_info;
	public ClientLogQuerier(String filePath) 
	{
		try{
			Catalog c = null;
		FileReader fr= new FileReader(filePath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		machine_info = new ArrayList<Catalog>();
        while ((line = br.readLine()) != null) {
        	String[] ar=line.split(",");
        	c= new Catalog(ar[0],ar[1]);
        	machine_info.add(c);        	
        }
        c.toString();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

    public static void main(String[] args) throws IOException {
    	
    	/*//Reading IP addresses and port number from machineinfo file
    	
    	//creating a selector 
    	Selector selector = Selector.open();
    	Channel channel;
    	//registering channels with the selector
    	channel.configureBlocking(false);

    	SelectionKey key = channel.register(selector, SelectionKey.OP_READ);*/
    	System.out.println("Enter");
    	ClientLogQuerier c= new ClientLogQuerier("src\\machineinfo");
    }

}
