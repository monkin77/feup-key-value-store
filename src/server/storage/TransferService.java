package server.storage;

import common.Message;
import common.Sender;
import server.Constants;
import common.Utils;
import server.cluster.MembershipService;
import server.cluster.Node;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TransferService {
    private final StorageService storageService;
    private final Node node;
    public TransferService(StorageService storageService, Node node) {
        this.storageService = storageService;
        this.node = node;
    }

    public void join() {
        // Cluster is not fulfilling the replication factor -> replicate all files
        if (Constants.replicationFactor >= storageService.getNumberOfNodes()) {
            Node curNode = storageService.getNextNode(this.node);
            boolean copyOwnFiles = true;
            while (!curNode.getId().equals(this.node.getId())) {
                ArrayList<String> curNodeFiles = this.getNodeFileNames(curNode);
                if (copyOwnFiles) {
                    ArrayList<String> filesToTransfer = filterResponsibleFiles(curNodeFiles, this.node);
                    getFiles(filesToTransfer, curNode, false);
                    copyOwnFiles = false;
                }

                ArrayList<String> filesToTransfer = filterResponsibleFiles(curNodeFiles, curNode);
                getFiles(filesToTransfer, curNode, false);
                curNode = storageService.getNextNode(curNode);
            }
            return;
        }

        // Transfer replicas to the new node (including own files)
        Node responsibleNode = this.node;
        Node replicaNode = this.node;
        for (int i = 0; i < Constants.replicationFactor; ++i)
            replicaNode = this.storageService.getNextNode(replicaNode);

        for (int i = 0; i < Constants.replicationFactor; ++i) {
            ArrayList<String> replicaNodeFiles = this.getNodeFileNames(replicaNode);
            ArrayList<String> filesToReplicate = filterResponsibleFiles(replicaNodeFiles, responsibleNode);
            getFiles(filesToReplicate, replicaNode, true);

            replicaNode = this.storageService.getPreviousNode(replicaNode);
            responsibleNode = this.storageService.getPreviousNode(responsibleNode);
        }
    }

    public void leave() {
        // If there are less than replicationFactor nodes left then the nodes already have all the files
        if (Constants.replicationFactor >= storageService.getNumberOfNodes())
            return;

        File folder = new File(storageService.getDbFolder());
        ArrayList<String> folderFiles = new ArrayList<>(List.of(Objects.requireNonNull(folder.list())));

        Node curNode = this.node;
        Node receivingNode = curNode;

        for (int i = 0; i < Constants.replicationFactor; ++i)
            receivingNode = this.storageService.getNextNode(receivingNode);

        for (int i = 0; i < Constants.replicationFactor; ++i) {
            ArrayList<String> filesToTransfer = filterResponsibleFiles(folderFiles, curNode);
            sendNodeFiles(filesToTransfer, receivingNode);
            curNode = storageService.getPreviousNode(curNode);
            receivingNode = storageService.getPreviousNode(receivingNode);
        }
    }

    /**
     * Recovers from a crash by updating the node's files and deleting invalid ones
     */
    public void recoverFromCrash() {
        if (storageService.getNumberOfNodes() == 1) return;

        // Copy own files
        Node nextNode = storageService.getNextNode(this.node);
        ArrayList<String> nextNodeFiles = this.getNodeFileNames(nextNode);
        ArrayList<String> filesToTransfer = filterResponsibleFiles(nextNodeFiles, this.node);

        List<String> validFiles = new ArrayList<>(filesToTransfer);
        getFiles(filesToTransfer, nextNode, false);

        // get replicas from previous replicationFactor nodes
        Node curNode = storageService.getPreviousNode(this.node);
        for (int i = 0; i < Constants.replicationFactor - 1; ++i) {
            if (curNode.getId().equals(this.node.getId())) break;

            ArrayList<String> curNodeFiles = this.getNodeFileNames(curNode);
            filesToTransfer = filterResponsibleFiles(curNodeFiles, curNode);
            validFiles.addAll(filesToTransfer);

            getFiles(filesToTransfer, curNode, false);
            curNode = storageService.getPreviousNode(curNode);
        }

        // delete invalid files
        List<String> allFiles = storageService.getFiles();
        for (String file : allFiles) {
            if (!validFiles.contains(file)) storageService.deleteFilePermanently(file);
        }
    }

    /**
     * Creates a Message request to save a file
     * @param fileName name of the file to be saved
     * @return if everything went well Message with saveFile action,
     *         otherwise Message with error action
     */
    private Message createMsgFromFile(String fileName) throws IOException {
        final String filePath = storageService.getDbFolder() + fileName;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(out);
        byte[] fileBytes;

        synchronized (filePath.intern()) {
            File file = new File(filePath);
            try (FileInputStream fis = new FileInputStream(file.getPath())) {
                dos.write(file.getName().getBytes(StandardCharsets.UTF_8));
                dos.write("\r\n".getBytes(StandardCharsets.UTF_8));
                fileBytes = fis.readAllBytes();
            } catch (IOException e) {
                System.out.println("Error opening file in createMsgFromFile: " + file.getPath());
                throw e;
            }
        }

        final String tombstonePath = storageService.getTombstoneFolder() + fileName;
        final File tombstone = new File(tombstonePath);
        if (tombstone.exists()) {
            try {
                long timestamp = TombstoneManager.getTimestamp(tombstonePath);
                dos.writeLong(timestamp);
            } catch (IOException e) {
                System.out.println("Error opening tombstone in createMsgFromFile: " + tombstone.getPath());
            }
        }

        try {
            dos.write(fileBytes);
        } catch (IOException e) {
            System.out.println("Error writing file bytes in createMsgFromFile: " + filePath);
        }
        return new Message("REQ", "saveFile", out.toByteArray());
    }

    /**
     * Sends the files in fileNames to a node
     * @param fileNames array with the file names to send
     * @param node node to which we want to send the files
     */
    private void sendNodeFiles(ArrayList<String> fileNames, Node node) {
        for (String fileName : fileNames) {
            try {
                Message msg = createMsgFromFile(fileName);
                Sender.sendTCPMessage(msg.toBytes(), node.getId(), node.getPort());
            } catch (IOException e) {
                System.out.println("Could not send file to node: " + node.getId());
            }
        }
    }

    private void getFiles(ArrayList<String> fileNames, Node node, boolean deleteFiles) {
        for (String fileName : fileNames) {
            try {
                Message msg = new Message("REQ", deleteFiles ? "getAndDelete" : "get",
                        fileName.getBytes(StandardCharsets.UTF_8));

                byte[] response = Sender.sendTCPMessage(msg.toBytes(), node.getId(), node.getPort());
                Message responseMsg = new Message(response);
                if (!responseMsg.getAction().equals("ok")) continue;

                storageService.saveFile(fileName, responseMsg.getBody());
            } catch (IOException e) {
                System.out.println("Could not get the files from the node: " + node.getId());
            }
        }
    }

    /**
     * gets the name of the files in a node by requesting it through TCP
     * @param node node from which we want to get the files' names
     * @return array with the name of the files stored in node's database
     */
    private ArrayList<String> getNodeFileNames(Node node) {
        try {
            Message message = new Message("REQ", "getFiles", null);
            Message reply = new Message(Sender.sendTCPMessage(message.toBytes(), node.getId(), node.getPort()));
            if (!reply.getAction().equals("ok")) return new ArrayList<>();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(reply.getBody())));

            ArrayList<String> fileNames = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                fileNames.add(line);
            }

            return fileNames;
        } catch (IOException e) {
            System.out.println("Error getting files from node: " + node.getId());
            return new ArrayList<>();
        }
    }

    private ArrayList<String> filterResponsibleFiles(ArrayList<String> fileNames, Node node) {
        final ArrayList<String> filteredFileNames = new ArrayList<>();
        for (String fileName : fileNames) {
            if (storageService.getResponsibleNode(fileName).getId().equals(node.getId())
                && !fileName.equals("tombstones") && !fileName.equals("membership.log")
                && !fileName.equals("membershipCounter.txt")) {
                    filteredFileNames.add(fileName);
            }
        }

        return filteredFileNames;
    }

}
