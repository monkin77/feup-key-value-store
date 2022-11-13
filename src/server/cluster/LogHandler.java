package server.cluster;

import common.Utils;
import server.Constants;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LogHandler {
    /**
     * Compare this node logs with the logs from a new node relative to their recency.
     * @param newLogs
     * @param folderPath
     * @return comparison result. If > 0 newLogs are more recent.
     */
    public static int compareLogs(HashMap<String, Integer> newLogs, String folderPath) {
        int score = 0;

        HashMap<String, Integer> currLogs = buildLogsMap(folderPath, Constants.numLogEvents);

        Set<String> logs = new HashSet<>(currLogs.keySet());
        logs.addAll(newLogs.keySet());

        for (final String currNodeId : logs) {
            int currCounter = -1;
            int newCounter = -1;
            if (currLogs.containsKey(currNodeId))
                currCounter = currLogs.get(currNodeId);

            if (newLogs.containsKey(currNodeId))
                newCounter = newLogs.get(currNodeId);

            if (newCounter > currCounter)
                score += 1;
            else if (currCounter > newCounter)
                score -= 1;
        }

        return score;
    }

    public static boolean shouldPropagate(HashMap<String, Integer> newLogs, String newNodeId, String folderPath, String nodeId) {
        int score = compareLogs(newLogs, folderPath);

        if (score == 0) {
            // Node with the lower hash id will be elected
            return Utils.generateKey(newNodeId).compareTo(Utils.generateKey(nodeId)) < 0;
        }

        return score > 0;
    }

    public static boolean shouldBeElected(HashMap<String, Integer> newLogs, String folderPath) {
        int score = compareLogs(newLogs, folderPath);

        return score < 0;
    }

    public static HashMap<String, Integer> buildLogsMap(String folderPath, int maxLogs) {
        String logPath = folderPath + Constants.membershipLogFileName;

        HashMap<String, Integer> nodesMap = new HashMap<>();
        synchronized (logPath.intern()) {
            File file = new File(logPath);

            try {
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line;
                int logCounter = 0;
                while ((line = br.readLine()) != null) {
                    if (logCounter >= maxLogs) break;

                    String[] lineData = line.split(" ");
                    nodesMap.put(lineData[0], Integer.parseInt(lineData[1]));
                    logCounter++;
                }
            } catch (IOException e) {
                return nodesMap;
            }
        }

        return nodesMap;
    }

    /**
     * Builds a byte[] with the most recent 32 logs from the membershipLog
     * @param folderPath
     * @param nodeMap if This is null, it will send the tcpPort of the node in the log line
     * @return byte array
     */
    public static byte[] buildLogsBytes(String folderPath, TreeMap<String, Node> nodeMap) {
        String logPath = folderPath + Constants.membershipLogFileName;

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        synchronized (logPath.intern()) {
            File file = new File(logPath);
            try {
                Scanner myReader = new Scanner(file);
                for (int i = 0; i < Constants.numLogEvents; i++) {
                    if (!myReader.hasNextLine())
                        break;
                    String line = myReader.nextLine();

                    StringBuilder sb = new StringBuilder();
                    sb.append(line);

                    if (nodeMap != null ) {
                        sb.append(" ");
                        // if nodeMap does not find the port, send the invalid port number (-1)
                        String nodeId = line.split(" ")[0];
                        String nodeKey = Utils.generateKey(nodeId);

                        if (nodeMap.containsKey(nodeKey)) sb.append(nodeMap.get(nodeKey).getPort());
                        else sb.append(Constants.invalidPort);
                    }

                    sb.append(Utils.newLine);
                    byteOut.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                }
                myReader.close();
            } catch (IOException e) {
                return byteOut.toByteArray();   // Return byteArray as is
            }
        }

        return byteOut.toByteArray();
    }
}
