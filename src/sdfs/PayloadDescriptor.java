package sdfs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * All commands sent to data node, name node, client are in the following format:
 * 
 * [1 byte payload descriptor] [other command parameters]...
 */
public class PayloadDescriptor {
    private static final Map<Byte, String> des2com;
    private static final Map<String, Byte> com2des;

    static {
        Map<Byte, String> t1 = new HashMap<>();
        // sent to data node
        t1.put((byte) 0, "put");
        // sent to data node
        t1.put((byte) 1, "get");
        // sent to both data node and name node
        t1.put((byte) 2, "delete");
        // sent from name node to data node
        t1.put((byte) 3, "replicate");
        // sent to name node
        t1.put((byte) 4, "put_request");
        // sent to name node
        t1.put((byte) 5, "get_request");
        // sent to data node, name node, and client
        t1.put((byte) 100, "success");

        Map<String, Byte> t2 = new HashMap<>();
        for (Map.Entry<Byte, String> entry : t1.entrySet()) {
            t2.put(entry.getValue(), entry.getKey());
        }

        des2com = Collections.unmodifiableMap(t1);
        com2des = Collections.unmodifiableMap(t2);
    }

    public static String getCommand(byte descriptor) {
        return des2com.get(descriptor);
    }

    public static byte getDescriptor(String cmd) {
        return com2des.get(cmd);
    }
}
