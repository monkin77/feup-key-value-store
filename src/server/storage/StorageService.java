package server.storage;

import common.Message;
import common.Sender;
import common.Utils;
import server.Constants;
import server.cluster.Node;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

public class StorageService implements KeyValue {
    private final TreeMap<String, Node> nodeMap;
    private final String ownID;
    private final String dbFolder;
    private final String tombstoneFolder;
    private ExecutorService executorService;

    public StorageService(TreeMap<String, Node> nodeMap, String ownID) {
        this.nodeMap = nodeMap;
        this.ownID = ownID;
        this.executorService = null;
        this.dbFolder = Utils.generateFolderPath(ownID);
        this.tombstoneFolder = dbFolder + "tombstones/";
        createTombstoneFolder();
    }

    @Override
    public Message put(String key, byte[] value) {
        Node node = getResponsibleNode(key);
        if (!node.getId().equals(ownID))
            return buildRedirectMessage(node);

        if (hasFile(key)) return new Message("REP", "ok", null);

        String filePath = dbFolder + key;
        synchronized (filePath.intern()) {
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(value);
            } catch (IOException e) {
                String error = "Error opening file in put operation: " + filePath;
                System.out.println(error);
                return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
            }
        }

        // Send the file to the following nodes (Replication)
        for (int i = 1; i < Constants.replicationFactor; ++i) {
            final Node nextNode = getNextNode(node);
            if (nextNode.getId().equals(ownID)) break; // Not enough nodes available

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(out);
            try {
                dos.write(key.getBytes(StandardCharsets.UTF_8));
                dos.write(Utils.newLine.getBytes(StandardCharsets.UTF_8));
                dos.writeLong(0);
                dos.write(value);

                Message msg = new Message("REQ", "saveFile", out.toByteArray());

                // If the node is down, the node should recover when it gets back up
                executorService.submit(() -> Sender.sendTCPMessage(msg.toBytes(), nextNode.getId(), nextNode.getPort()));
            } catch (IOException e) {
                System.out.println("Error sending saveFile message to " + node.getId());
                e.printStackTrace();
            }

            node = nextNode;
        }

        return new Message("REP", "ok", null);
    }

    @Override
    public Message get(String key) {

        Node node = getResponsibleNode(key);
        // The node can have the file due to replication
        if (!node.getId().equals(ownID) && !hasFile(key))
            return buildRedirectMessage(node);

        String filePath = dbFolder + key;
        byte[] value;

        synchronized (filePath.intern()) {
            try (FileInputStream fis = new FileInputStream(filePath)) {
                value = fis.readAllBytes();
            } catch (IOException e) {
                String error = "Error opening file in get operation: " + key;
                System.out.println(error);
                return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
            }
        }

        return buildTombstoneMessage(key, value);
    }

    @Override
    public Message delete(String key) {
        Node node = getResponsibleNode(key);
        if (!node.getId().equals(ownID))
            return buildRedirectMessage(node);

        // File is not in the system
        if (!hasFile(key)) return new Message("REP", "ok", null);

        this.safeDelete(key);

        // Tell the following nodes to delete the file (Replication)
        for (int i = 1; i < Constants.replicationFactor; ++i) {
            final Node nextNode = getNextNode(node);
            if (nextNode.getId().equals(ownID)) break; // Not enough nodes available

            Message msg = new Message("REQ", "safeDelete", key.getBytes(StandardCharsets.UTF_8));

            // If the node is down, the node should recover when it gets back up
            executorService.submit(() -> Sender.sendTCPMessage(msg.toBytes(), nextNode.getId(), nextNode.getPort()));
            node = nextNode;
        }

        return new Message("REP", "ok", null);
    }

    public Message getAndDelete(String key) {
        String filePath = dbFolder + key;
        byte[] value;

        synchronized (filePath.intern()) {
            try (FileInputStream fis = new FileInputStream(filePath)) {
                value = fis.readAllBytes();
            } catch (IOException e) {
                String error = "Error opening file in get operation: " + key;
                System.out.println(error);
                return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
            }
        }

        Message reply = buildTombstoneMessage(key, value);
        deleteFilePermanently(key);

        return reply;
    }

    public Message saveFile(String key, byte[] data) {
        String filePath = dbFolder + key;
        if (hasFile(key)) return new Message("REP", "ok", null);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        try {
            long tombTimestamp = dis.readLong();
            byte[] file = dis.readAllBytes();

            synchronized (filePath.intern()) {
                try (FileOutputStream fos = new FileOutputStream(filePath)) {
                    fos.write(file);
                }
            }

            if (tombTimestamp != 0)
                this.saveTombstone(key, tombTimestamp);

            return new Message("REP", "ok", null);
        } catch (IOException e) {
            String error = "Error opening file in put operation: " + filePath;
            System.out.println(error);
            return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
        }
    }

    public Message safeDelete(String key) {
        try {
            saveTombstone(key, System.currentTimeMillis());
            return new Message("REP", "ok", null);
        } catch (IOException e) {
            String error = "Error creating tombstone file: " + key;
            System.out.println(error);
            return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
        }
    }

    public void saveTombstone(String key, long timestamp) throws IOException {
        String filePath = tombstoneFolder + key;

        synchronized (filePath.intern()) {
            FileOutputStream fos = new FileOutputStream(filePath);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(timestamp);
        }
    }

    public List<String> getFiles() {
        File folder = new File(dbFolder);
        String[] folderArray = folder.list();
        List<String> folderList = new ArrayList<>();
        if (folderArray == null) return folderList;

        for (String file : folderArray) {
            if (!file.equals("tombstones") && !file.equals("membership.log") &&
                    !file.equals("membershipCounter.txt")) {
                folderList.add(file);
            }
        }

        return folderList;
    }

    public String getDbFolder() {
        return dbFolder;
    }

    public String getTombstoneFolder() {
        return tombstoneFolder;
    }

    public int getNumberOfNodes() {
        return nodeMap.size();
    }

    public Node getNextNode(Node prevNode) {
        String prevKey = Utils.generateKey(prevNode.getId());
        Map.Entry<String, Node> nodeEntry = nodeMap.higherEntry(prevKey);

        // No node with greater key -> Go to the start of the circle (first node)
        if (nodeEntry == null) nodeEntry = nodeMap.firstEntry();

        return nodeEntry.getValue();
    }

    public Node getPreviousNode(Node node) {
        String key = Utils.generateKey(node.getId());
        Map.Entry<String, Node> nodeEntry = nodeMap.lowerEntry(key);

        // No node with lower key -> Go to the end of the circle (last node)
        if (nodeEntry == null) nodeEntry = nodeMap.lastEntry();

        return nodeEntry.getValue();
    }

    private boolean hasFile(String key) {
        String filePath = dbFolder + key;
        File file = new File(filePath);
        return file.exists();
    }

    /**
     * This method ensures the binary search's O(log N) time complexity by using the
     * TreeMap.ceilingKey() method, which takes advantage of a Red-Black BST.
     */
    public Node getResponsibleNode(String key) {
        Map.Entry<String, Node> nodeEntry = nodeMap.ceilingEntry(key);

        // No node with greater key -> Go to the start of the circle (first node)
        if (nodeEntry == null) nodeEntry = nodeMap.firstEntry();

        return nodeEntry.getValue();
    }

    public void deleteFilePermanently(String key) {
        String filePath = dbFolder + key;

        synchronized (filePath.intern()) {
            File file = new File(filePath);
            if (!file.delete())
                System.out.println("Failed to delete the file: " + key);
        }

        String tombstonePath = tombstoneFolder + key;

        synchronized (tombstonePath.intern()) {
            File tombstoneFile = new File(tombstonePath);
            if (tombstoneFile.exists()) {
                if (!tombstoneFile.delete())
                    System.out.println("Failed to delete the tombstone file: " + key);
            }
        }
    }

    private Message buildRedirectMessage(Node newNode) {
        String redirectInfo = newNode.getId() + Utils.newLine + newNode.getPort();
        return new Message("REP", "redirect", redirectInfo.getBytes(StandardCharsets.UTF_8));
    }

    private void createTombstoneFolder() {
        File folder = new File(tombstoneFolder);
        if (!folder.exists()) {
            if (!folder.mkdir()) {
                System.out.println("Error creating tombstone folder");
            }
        }
    }

    private Message buildTombstoneMessage(String key, byte[] value) {
        String tombstonePath = tombstoneFolder + key;
        File tombstoneFile = new File(tombstonePath);

        synchronized (tombstonePath.intern()) {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                DataOutputStream dos = new DataOutputStream(bos);
                dos.writeLong(tombstoneFile.exists() ?
                        TombstoneManager.getTimestamp(tombstonePath) : 0);
                dos.write(value);
                return new Message("REP", "ok", bos.toByteArray());
            } catch (IOException e) {
                String error = "Error opening tombstone file operation: " + key;
                System.out.println(error);
                return new Message("REP", "error", error.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
