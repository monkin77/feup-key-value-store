package server.cluster;

import common.Message;
import common.Sender;
import common.Utils;
import server.Constants;
import server.network.TCPListener;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MembershipService implements ClusterMembership {
    private final TreeMap<String, Node> nodeMap;
    private final String multicastIpAddr;
    private final int multicastIPPort;
    private final String nodeId;

    private final int tcpPort;
    private final String folderPath;
    private int membershipCounter = 0; // NEEDS TO BE STORED IN NON-VOLATILE MEMORY TO SURVIVE NODE CRASHES
    private static final int maxRetransmissions = 3;
    private final HashSet<String> membershipReplyNodes;
    private final HashSet<String> repliedNodes;

    private boolean isElected = false;
    Future<?> electionPingThread = null;

    public MembershipService(String multicastIPAddr, int multicastIPPort, String nodeId, int tcpPort) {
        this.nodeMap = new TreeMap<>();
        this.multicastIpAddr = multicastIPAddr;
        this.multicastIPPort = multicastIPPort;
        this.nodeId = nodeId;
        this.tcpPort = tcpPort;
        this.folderPath = Utils.generateFolderPath(nodeId);
        this.membershipReplyNodes = new HashSet<>();
        this.repliedNodes = new HashSet<>();
        this.createNodeFolder();
    }

    /**
     * This methods throws a RuntimeException if it fails
     */
    @Override
    public void join() {
        if (this.membershipCounter == 0 || !isClusterMember(this.membershipCounter)) {
            if (this.membershipCounter != 0) // Edge case for the first join
                this.updateMembershipCounter(this.membershipCounter + 1);

            this.multicastJoin();

            // Send election request
            ElectionService.sendRequest(this.nodeId, this.nodeMap);
        } else {
            throw new RuntimeException("Attempting to join the cluster while being already a member.");
        }
    }

    /**
     * This methods throws a RuntimeException if it fails
     */
    @Override
    public void leave() {
        if (isClusterMember(this.membershipCounter)) {
            this.removeNodeFromMap(this.nodeId);
            this.updateMembershipCounter(this.membershipCounter + 1);
            this.addLog(this.nodeId, this.membershipCounter, this.tcpPort);
            this.multicastLeave();

            if (this.isElected) {
                ElectionService.sendLeave(this.nodeMap, this.buildMembershipMsgBody());

                this.isElected = false;
                if (this.electionPingThread != null) this.electionPingThread.cancel(true);
            }
        }
    }

    /**
     * Updates the membershipCounter value and stores it in non-volatile memory
     */
    public void updateMembershipCounter(int newCounter) {
        this.membershipCounter = newCounter;
        String filePath = this.folderPath + Constants.membershipCounterFileName;

        synchronized (filePath.intern()) {
            try {
                File memberCounter = new File(filePath);
                FileWriter counterWriter;
                counterWriter = new FileWriter(memberCounter, false);
                counterWriter.write(String.valueOf(newCounter));
                counterWriter.close();
            } catch (IOException e) {
                System.out.println("Failed to update membership counter.");
            }
        }
    }

    public TreeMap<String, Node> getNodeMap() {
        return nodeMap;
    }

    private void multicastJoin() {
        byte[] joinBody;
        try {
            joinBody = buildMembershipBody();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Message msg = new Message("REQ", "join", joinBody);

        // Add this node information
        this.addNodeToMap(this.nodeId, this.tcpPort);
        this.addLog(this.nodeId, this.membershipCounter, this.tcpPort);

        int retransmissionCounter = 0;
        while (retransmissionCounter < maxRetransmissions) {
            int elapsedTime = 0;
            try {
                Sender.sendMulticast(msg.toBytes(), this.multicastIpAddr, this.multicastIPPort);
                while (elapsedTime < Constants.timeoutTime) {
                    Thread.sleep(Constants.multicastStepTime);
                    if (this.membershipReplyNodes.size() >= Constants.numMembershipMessages) {
                        this.membershipReplyNodes.clear();
                        System.out.println("New Node joined the distributed store");
                        return;
                    }

                    elapsedTime += Constants.multicastStepTime;
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }

            retransmissionCounter++;
        }

        System.out.println("Prime Node joined the distributed store.");
    }

    private void multicastLeave() {
        byte[] leaveBody;
        try {
            leaveBody = buildMembershipBody();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Message msg = new Message("REQ", "leave", leaveBody);

        try {
            Sender.sendMulticast(msg.toBytes(), this.multicastIpAddr, this.multicastIPPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Node left the distributed store.");
    }

    /**
     * Body has nodeId, tcp port and membership Counter
     */
    private byte[] buildMembershipBody() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(this.nodeId).append(Utils.newLine);
        sb.append(this.tcpPort).append(Utils.newLine);
        sb.append(this.membershipCounter).append(Utils.newLine);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        return out.toByteArray();
    }

    /**
     * Beware that when counter is 0 it can or not be a member
     * @param counter
     * @return true if a node is a member of the nodeMap. False otherwise.
     */
    public static boolean isClusterMember(int counter) {
        return counter % 2 == 0;
    }

    public int getMulticastIPPort() {
        return multicastIPPort;
    }

    public String getMulticastIpAddr() {
        return multicastIpAddr;
    }

    /**
     * Adds a new node to the nodeMap.
     */
    public void addNodeToMap(String newNodeId, int newNodePort) {
        String key = Utils.generateKey(newNodeId);
        if (!this.nodeMap.containsKey(key)) {
            Node newNode = new Node(newNodeId, newNodePort);
            this.nodeMap.put(key, newNode);
        }
    }

    /***
     * Removes a node from the map with a specific id
     * 
     * @param oldNodeId nodeId
     */
    public void removeNodeFromMap(String oldNodeId) {
        String key = Utils.generateKey(oldNodeId);
        this.nodeMap.remove(key);
    }

    private void createNodeFolder() {
        File folder = new File(this.folderPath);

        if (!folder.mkdirs() && !folder.isDirectory()) {
            System.out.println("Error creating the node's folder: " + this.folderPath);
        } else {
            // The membership log should be updated with the one received by other nodes
            // already belonging to the system
            this.initializeNodeFiles();
        }
    }

    private void initializeNodeFiles() {
        try {
            String counterPath = this.folderPath + Constants.membershipCounterFileName;

            synchronized (counterPath.intern()) {
                File memberCounter = new File(counterPath);
                boolean foundCounter = false;
                if (!memberCounter.createNewFile()) {
                    // Recover membership counter
                    Scanner counterScanner = new Scanner(memberCounter);
                    if (counterScanner.hasNextInt()) {
                        this.membershipCounter = counterScanner.nextInt();
                        foundCounter = true;
                    }

                    counterScanner.close();
                }
                if (!foundCounter) {
                    // Create file and store membership counter
                    FileWriter counterWriter = new FileWriter(memberCounter, false);
                    counterWriter.write(String.valueOf(this.membershipCounter));
                    counterWriter.close();
                }
            }

            String logPath = this.folderPath + Constants.membershipLogFileName;

            synchronized (logPath.intern()) {
                File memberLog = new File(logPath);
                if (!memberLog.exists()) {
                    // Set initial log to be the current node
                    FileWriter writer = new FileWriter(memberLog, false);
                    writer.write(String.format("%s %d", this.nodeId, this.membershipCounter));
                    writer.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a new log to the beginning of the membership log removing existing logs
     * for that nodeId
     */
    public void addLog(String newNodeId, int newMemberCounter, int newNodePort) {
        String logPath = this.folderPath + Constants.membershipLogFileName;

        try {
            synchronized (logPath.intern()) {
                File file = new File(logPath);
                boolean isDeprecatedLog = false;

                FileReader fr = new FileReader(file); // reads the file
                BufferedReader br = new BufferedReader(fr);
                String line;

                // Check if there is a more recent log for this node
                while ((line = br.readLine()) != null) {
                    String[] lineData = line.split(" ");
                    String lineId = lineData[0];
                    if (newNodeId.equals(lineId)) {
                        int lineCounter = Integer.parseInt(lineData[1]);
                        if (lineCounter >= newMemberCounter)
                            isDeprecatedLog = true;
                        break;
                    }
                }

                if (!isDeprecatedLog) {
                    List<String> filteredFile = new ArrayList<>();

                    filteredFile.add(String.format("%s %d", newNodeId, newMemberCounter));
                    filteredFile.addAll(
                            Files.lines(file.toPath()).filter(streamLine -> {
                                Optional<String> optId = Arrays.stream(streamLine.split(" ")).findFirst();
                                String rowId = optId.orElse("");
                                return !newNodeId.equals(rowId);
                            }).toList());

                    Files.write(file.toPath(), filteredFile, StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING);
                } 
            }

            if (isClusterMember(newMemberCounter)) {
                // if the nodeMap does not contain this node
                if ((newNodePort != Constants.invalidPort) && !this.nodeMap.containsKey(Utils.generateKey(newNodeId)))
                    this.nodeMap.put(Utils.generateKey(newNodeId), new Node(newNodeId, newNodePort));
            } else {
                // Remove the node from the nodeMap
                this.removeNodeFromMap(newNodeId);
            }
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] buildMembershipMsgBody() {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        try {
            // Write node id
            byteOut.write(this.nodeId.getBytes(StandardCharsets.UTF_8));
            byteOut.write(Utils.newLine.getBytes(StandardCharsets.UTF_8));

            byteOut.write(LogHandler.buildLogsBytes(this.folderPath, null));

            byteOut.write(Utils.newLine.getBytes(StandardCharsets.UTF_8));

            for (Node node : this.getNodeMap().values()) {
                String entryLine = node.getId() + " " + node.getPort() + Utils.newLine;
                byteOut.write(entryLine.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return byteOut.toByteArray();
    }

    public HashSet<String> getMembershipReplyNodes() {
        return membershipReplyNodes;
    }

    public void handleJoinRequest(String nodeId, int tcpPort, int membershipCounter) {
        if (this.repliedNodes.contains(Utils.generateKey(nodeId))) {
            System.out.println("Received join from node that was already replied.");
            return;
        }

        // Updates view of the cluster membership and adds the log
        this.addNodeToMap(nodeId, tcpPort);
        this.addLog(nodeId, membershipCounter, tcpPort);

        this.repliedNodes.clear();
        this.repliedNodes.add(Utils.generateKey(nodeId));

        final int randomWait = new Random().nextInt(Constants.maxResponseTime);
        try {
            Thread.sleep(randomWait);

            final byte[] body = this.buildMembershipMsgBody();
            Message msg = new Message("REQ", "join", body);
            Sender.sendTCPMessage(msg.toBytes(), nodeId, tcpPort);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void handleLeaveRequest(String nodeId,  int membershipCounter, int tcpPort) {
        // Updates view of the cluster membership and adds the log
        this.removeNodeFromMap(nodeId);
        this.addLog(nodeId, membershipCounter, tcpPort);

        this.repliedNodes.clear();
    }

    public void handleMembershipResponse(Message message) {
        if (this.getMembershipReplyNodes().size() >= Constants.numMembershipMessages)
            return;

        System.out.println("Received tcp reply to join");
        InputStream is = new ByteArrayInputStream(message.getBody());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line, nodeId;
        final ArrayList<String> membershipLogs = new ArrayList<>();
        try {
            nodeId = br.readLine();
            this.getMembershipReplyNodes().add(Utils.generateKey(nodeId)); // adds node to set if not present

            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    break; // Reached the end of membership log
                membershipLogs.add(line);
            }

            while ((line = br.readLine()) != null) {
                String[] data = line.split(" ");
                String newNodeId = data[0];
                int newNodePort = Integer.parseInt(data[1]);
                this.addNodeToMap(newNodeId, newNodePort); // Check what happens when adding node that already exists
            }
        } catch (IOException e) {
            System.out.println("Error while handling membership response:");
            e.printStackTrace();
            return;
        }

        updateMembershipInfo(membershipLogs);

        System.out.println("Received membership Logs: " + membershipLogs + "\nnodeMap: " + this.getNodeMap());
    }

    public int getMembershipCounter() {
        return membershipCounter;
    }

    /**
     *
     * @return true if node crashed while being a member of the cluster
     */
    public boolean hasCrashed() {
        if (!isClusterMember(this.membershipCounter)) return false;

        // In case a cluster is composed by only 1 node joining and leaving
        if (this.membershipCounter != 0) return true;

        try {
            Path path = Paths.get(this.folderPath + Constants.membershipLogFileName);
            return Files.lines(path).count() > 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public void handleElectionRequest(Message message, ExecutorService executorService) {

        InputStream is = new ByteArrayInputStream(message.getBody());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line, newNodeId;
        final HashMap<String, Integer> membershipLogs = new HashMap<>();
        try {
            newNodeId = br.readLine();
            // Verify if newNodeId received is this node (meaning this node was elected)
            if (newNodeId.equals(this.nodeId) && !this.isElected) {
                 if (executorService != null) {
                     this.electionPingThread = executorService.submit(new ElectionService(this.nodeId, this.folderPath, this.multicastIpAddr, this.multicastIPPort, this.nodeMap));
                     this.isElected = true;
                     System.out.println("THIS NODE WAS ELECTED");
                 }
                return;
            }

            System.out.printf("Received election request from %s\n", newNodeId);
            while ((line = br.readLine()) != null) {
                String[] logData = line.split(" ");
                membershipLogs.put(logData[0], Integer.parseInt(logData[1]));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (LogHandler.shouldPropagate(membershipLogs, newNodeId, this.folderPath, this.nodeId)) {
            // Propagate the message to the next node?
            System.out.println("Log is more recent! propagate to next node");

            // Check if this node was the previous leader
            if (this.isElected) {
                this.isElected = false;
                if (this.electionPingThread != null) this.electionPingThread.cancel(true);
            }

            // Send election request
            ElectionService.propagateRequest(this.nodeId, this.nodeMap, message);
        }
    }

    public void handleElectionPing(Message message) {
        ByteArrayInputStream is = new ByteArrayInputStream(message.getBody());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        final HashMap<String, ArrayList<Integer>> newMembershipLogs = new HashMap<>();
        final HashMap<String, Integer> newParsedLogs = new HashMap<>();
        try {
            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    break;
                String[] logData = line.split(" ");
                newMembershipLogs.put(logData[0], new ArrayList<>(List.of( Integer.parseInt(logData[1]), Integer.parseInt(logData[2]) )));
                newParsedLogs.put(logData[0], Integer.parseInt(logData[1]));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Update current node logs
        final HashMap<String, Integer> currMembershipLogs = LogHandler.buildLogsMap(this.folderPath, Integer.MAX_VALUE);
        for (String iterNodeId : newMembershipLogs.keySet()) {
            int currCounter = -1;
            if (currMembershipLogs.containsKey(iterNodeId))
                currCounter = currMembershipLogs.get(iterNodeId);

            Integer newCounter = newParsedLogs.get(iterNodeId);
            if (newCounter > currCounter) {
                this.addLog(iterNodeId, newCounter, newMembershipLogs.get(iterNodeId).get(1));
            }
        }

        if (LogHandler.shouldBeElected(newParsedLogs, this.folderPath)) {
            // Propagate the message to the next node?
            System.out.println("Node is more recent than the current leader. Starting an election request...");

            // Send election request
            ElectionService.sendRequest(this.nodeId, this.nodeMap);
        }
    }
    public void handleElectionLeave(Message message) {
        System.out.println("Received election leave.");

        InputStream is = new ByteArrayInputStream(message.getBody());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        final ArrayList<String> newMembershipLogs = new ArrayList<>();
        try {
            br.readLine();

            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    break; // Reached the end of membership log
                newMembershipLogs.add(line);
            }

            while ((line = br.readLine()) != null) {
                String[] data = line.split(" ");
                String newNodeId = data[0];
                int newNodePort = Integer.parseInt(data[1]);
                this.addNodeToMap(newNodeId, newNodePort); // Check what happens when adding node that already exists
            }
        } catch (IOException e) {
            System.out.println("Error while handling election leave:");
            e.printStackTrace();
            return;
        }

        updateMembershipInfo(newMembershipLogs);

        // Send election request to become the new leader
        ElectionService.sendRequest(this.nodeId, this.nodeMap);
    }

    public void handleElectionTimeout() {
        System.out.println("Election Ping timeout detected! Sending an election request...");
        ElectionService.sendRequest(this.nodeId, this.nodeMap);
    }

    /**
     * This method updates this node membershipInfo efficiently, according to the new received logs
     * @param newMembershipLogs
     */
    private void updateMembershipInfo(ArrayList<String> newMembershipLogs) {
        HashMap<String, Integer> currMembershipLogs = LogHandler.buildLogsMap(this.folderPath, Integer.MAX_VALUE);

        for (String newLog : newMembershipLogs) {
            String[] logData = newLog.split(" ");
            String logId = logData[0];
            int logCounter = Integer.parseInt(logData[1]);

            int currCounter = -1;
            if (currMembershipLogs.containsKey(logId))
                currCounter = currMembershipLogs.get(logId);

            if (logCounter > currCounter)
                this.addLog(logId, logCounter, Constants.invalidPort);
        }
    }

    public boolean getIsElected() {
        return this.isElected;
    }
}
