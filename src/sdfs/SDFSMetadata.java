package sdfs;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SDFSMetadata resides on name node.
 */
public class SDFSMetadata {
    private Map<InetAddress, Set<String>> nodeFiles = new HashMap<>();
    private Map<String, Set<InetAddress>> fileLocations = new HashMap<>();

    public List<InetAddress> getKidlestNodes(int k) {
        List<InetAddress> nodes = new ArrayList<>(nodeFiles.keySet());
        nodes.sort(new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                return nodeFiles.get(o1).size() - nodeFiles.get(o2).size();
            }
        });

        return nodes.subList(0, Math.min(k, nodes.size()));
    }

    public void mergeBlockreport(InetAddress node, Set<String> files) {
        nodeFiles.put(node, files);
        for (String file : files) {
            fileLocations.putIfAbsent(file, new HashSet<InetAddress>());
            fileLocations.get(file).add(node);
        }
        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            if (!files.contains(entry.getValue())) {
                entry.getValue().remove(node);
            }
        }
        cleanUp();
    }
    
    private void cleanUp() {
        List<String> filesToDelete = new ArrayList<>();
        for (String file : fileLocations.keySet()) {
            if (fileLocations.get(file).isEmpty()) {
                filesToDelete.add(file);
            }
        }
        for (String file : filesToDelete) {
            fileLocations.remove(file);
        }
    }
    
    public List<InetAddress> getFileLocations(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations == null || locations.isEmpty()) {
            return new ArrayList<InetAddress>();
        }
        return new ArrayList<InetAddress>(locations);
    }
    
    public void deleteNode(InetAddress node) {
        nodeFiles.remove(node);
    }
    
    public void deleteFile(String file) {
        fileLocations.remove(file);
    }
    
    public void addFileToNode(InetAddress node, String file) {
        nodeFiles.get(node).add(file);
    }
    
    public List<byte[]> getReplicateCmds() {
        List<byte[]> repCmds = new ArrayList<>();
        
        return repCmds;
    }
}
