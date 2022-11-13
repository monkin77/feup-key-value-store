package server.cluster;

import common.Message;
import common.MessageTypes;
import common.Sender;
import common.Utils;
import server.Constants;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ElectionService implements Runnable{
        private final String folderPath;
        private final String multicastIPAddr;
        private final int multicastPort;

        private final String nodeId;

        private final TreeMap<String, Node> nodeMap;

        public ElectionService(String nodeId, String folderPath, String multicastIPAddr, int multicastPort, TreeMap<String, Node> nodeMap) {
                this.folderPath = folderPath;
                this.multicastIPAddr = multicastIPAddr;
                this.multicastPort = multicastPort;
                this.nodeId = nodeId;
                this.nodeMap = nodeMap;
        }

        public static void sendRequest(String nodeId, TreeMap<String, Node> nodeMap) {
                Path path = Paths.get(Utils.generateFolderPath(nodeId) + Constants.membershipLogFileName);
                try {
                        byte[] fileData = Files.readAllBytes(path);
                        String nodeIdLine = nodeId + Utils.newLine;

                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        out.write(nodeIdLine.getBytes(StandardCharsets.UTF_8));
                        out.write(fileData);

                        Message electionMessage = new Message(MessageTypes.REQUEST.getCode(), MessageTypes.ELECTION_REQUEST.getCode(), out.toByteArray());

                        sendSafeMessage(nodeId, nodeMap, electionMessage, "Request");
                } catch (IOException e) {
                        System.out.println("Error while starting election request");
                }
        }

        public static void propagateRequest(String nodeId, TreeMap<String, Node> nodeMap, Message message) {
                try {
                        sendSafeMessage(nodeId, nodeMap, message, "Propagate");
                } catch (IOException e) {
                        System.out.println("Error while propagating election request");
                }
        }

        /**
         * If the leader leaves the cluster, it will send a leave request to the next Node so that it starts an election process.
         */
        public static void sendLeave(TreeMap<String, Node> nodeMap, byte[] leaveBody) {
                try {
                        Message electionMessage = new Message(MessageTypes.REQUEST.getCode(), MessageTypes.ELECTION_LEAVE.getCode(), leaveBody);
                        sendSafeByFirst(nodeMap, electionMessage, "Leave");
                } catch (IOException e) {
                        System.out.println("Error while sending leave request");
                }
        }

        /**
         *  Gets next node to node with key. If there is no node with higher key value,
         *  then gets the first node, so it behaves like a circular map
         * @return Next Node
         */
        public static Node getNextNode(String nodeId, TreeMap<String, Node> nodeMap) {
                if (nodeMap.size() == 0) return null;
                String key = Utils.generateKey(nodeId);

                // Get the next node to store the files
                Map.Entry<String, Node> nextEntry = nodeMap.higherEntry(key);
                if (nextEntry == null) nextEntry = nodeMap.firstEntry();
                Node nextNode = nextEntry.getValue();
                return nextNode;
        }

        private static void sendSafeMessage(String sourceNodeId, TreeMap<String, Node> nodeMap, Message electionMessage, String label) throws IOException {
                String currNodeId = sourceNodeId;
                while (true) {
                        Node nextNode = getNextNode(currNodeId, nodeMap);
                        if (nextNode == null) return;
                        currNodeId = nextNode.getId();

                        System.out.printf("Sending %s Election message to %s...\n", label, nextNode.getId());

                        byte[] electionRes = Sender.sendTCPMessage(electionMessage.toBytes(), nextNode.getId(), nextNode.getPort());

                        Message resMessage = new Message(electionRes);
                        if (resMessage.getAction().equals(MessageTypes.OK.getCode()))
                                return;
                }
        }

        private static void sendSafeByFirst(TreeMap<String, Node> nodeMap, Message electionMessage, String label) throws IOException {
                if (nodeMap.size() == 0) return;
                Node firstNode = nodeMap.firstEntry().getValue();

                System.out.printf("Sending %s Election message to %s...\n", label, firstNode.getId());

                byte[] electionRes = Sender.sendTCPMessage(electionMessage.toBytes(), firstNode.getId(), firstNode.getPort());
                Message resMessage = new Message(electionRes);
                if (resMessage.getAction().equals(MessageTypes.OK.getCode()))
                        return;

                sendSafeMessage(firstNode.getId(), nodeMap, electionMessage, label);
        }

        @Override
        public void run() {
                try {
                        while (true) {
                                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                                String nodeIdLine = nodeId + Utils.newLine;
                                byteOut.write(nodeIdLine.getBytes(StandardCharsets.UTF_8));

                                byte[] electionBody = LogHandler.buildLogsBytes(this.folderPath, this.nodeMap);
                                byteOut.write(electionBody);

                                Message msg = new Message(MessageTypes.REQUEST.getCode(), MessageTypes.ELECTION_PING.getCode(), byteOut.toByteArray());

                                Sender.sendMulticast(msg.toBytes(), this.multicastIPAddr, this.multicastPort);

                                Thread.sleep(Constants.electionPingTime);
                                // TODO: Check if needs to detect any exception to stop
                        }
                } catch (InterruptedException e) {
                        System.out.println("Node stopped being a leader.");
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}
