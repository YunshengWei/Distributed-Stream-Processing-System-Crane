package sdfs;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import system.Catalog;

/**
 * Metadata is a thread-safe class that stores meta data information about SDFS.
 */
public class Metadata {
    private Map<String, Set<InetAddress>> fileLocations = new HashMap<>();
    private Map<InetAddress, Set<String>> FilesOnNode = new HashMap<>();
    private Map<InetAddress, Datanode> IP2Datanode = new HashMap<>();

    private synchronized void cleanUp() {
        for (Iterator<Map.Entry<String, Set<InetAddress>>> itr = fileLocations.entrySet()
                .iterator(); itr.hasNext();) {
            Map.Entry<String, Set<InetAddress>> entry = itr.next();
            if (entry.getValue().isEmpty()) {
                itr.remove();
            }
        }
    }

    private synchronized List<InetAddress> getKidlestNodesExcept(int k, InetAddress... addresses) {
        List<InetAddress> nodes = new ArrayList<>(FilesOnNode.keySet());
        for (InetAddress address : addresses) {
            nodes.remove(address);
        }
        nodes.sort(new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                return FilesOnNode.get(o1).size() - FilesOnNode.get(o2).size();
            }
        });
        return nodes.subList(0, Math.min(k, nodes.size()));
    }

    public synchronized List<Datanode> getKidlestNodes(int k) {
        List<InetAddress> nodes = getKidlestNodesExcept(k);

        List<Datanode> datanodes = new ArrayList<>();
        for (InetAddress IP : nodes) {
            datanodes.add(IP2Datanode.get(IP));
        }
        return datanodes;
    }

    public synchronized List<Datanode> getFileLocations(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations == null || locations.isEmpty()) {
            return new ArrayList<Datanode>();
        }

        List<Datanode> datanodes = new ArrayList<>();
        for (InetAddress IP : locations) {
            datanodes.add(IP2Datanode.get(IP));
        }
        return datanodes;
    }

    public synchronized List<InetAddress> getFileLocationIPs(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations == null || locations.isEmpty()) {
            return new ArrayList<InetAddress>();
        }
        return new ArrayList<InetAddress>(locations);
    }

    public synchronized void deleteNode(InetAddress IP) {
        IP2Datanode.remove(IP);
        FilesOnNode.remove(IP);
        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            entry.getValue().remove(IP);
        }
        cleanUp();
    }

    public synchronized void deleteNodes(List<InetAddress> addresses) {
        for (InetAddress address : addresses) {
            deleteNode(address);
        }
    }

    public synchronized void mergeBlockReport(BlockReport blockreport) {
        IP2Datanode.put(blockreport.getIPAddress(), blockreport.getDatanode());
        FilesOnNode.put(blockreport.getIPAddress(), new HashSet<String>(blockreport.getFiles()));
        for (String file : blockreport.getFiles()) {
            fileLocations.putIfAbsent(file, new HashSet<InetAddress>());
            fileLocations.get(file).add(blockreport.getIPAddress());
        }

        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            if (!blockreport.getFiles().contains(entry.getKey())) {
                entry.getValue().remove(blockreport.getIPAddress());
            }
        }
        cleanUp();
    }

    public synchronized Datanode getDatanode(InetAddress IP) {
        return IP2Datanode.get(IP);
    }

    public synchronized void deleteFile(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations != null) {
            for (InetAddress IP : locations) {
                FilesOnNode.get(IP).remove(fileName);
            }
            fileLocations.remove(fileName);
        }
    }

    public synchronized void addFile(String file, InetAddress IP) {
        fileLocations.putIfAbsent(file, new HashSet<InetAddress>());
        fileLocations.get(file).add(IP);
        FilesOnNode.get(IP).add(file);
    }

    /**
     * Check whether there is any file to be replicated.
     * 
     * @return one file that needs to be replicated. If no file needs to be
     *         replicated, then return <code>null</code>. return format: String
     *         fileName, Datanode from, Datanode to...
     */
    public synchronized List<Object> getReplicationRequest() {
        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            int numToReplicate = Catalog.REPLICATION_FACTOR - entry.getValue().size();
            if (numToReplicate > 0) {
                List<InetAddress> tos = getKidlestNodesExcept(numToReplicate,
                        entry.getValue().toArray(new InetAddress[entry.getValue().size()]));
                if (tos.isEmpty()) {
                    return null;
                } else {
                    List<Object> ret = new ArrayList<>();
                    ret.add(entry.getKey());
                    Datanode from = getDatanode(entry.getValue().iterator().next());
                    ret.add(from);
                    for (InetAddress IP : tos) {
                        Datanode to = getDatanode(IP);
                        ret.add(to);
                    }
                    return ret;
                }
            }
        }
        return null;
    }
}
