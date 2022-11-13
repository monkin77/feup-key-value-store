package server.network;

import common.Message;
import common.MessageTypes;
import common.Utils;
import server.cluster.MembershipService;
import server.storage.StorageService;
import server.storage.TransferService;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class TCPListener implements Runnable {
    private final StorageService storageService;
    private final MembershipService membershipService;
    private final TransferService transferService;
    private final ExecutorService executorService;

    private final ServerSocket serverSocket;

    public TCPListener(StorageService storageService, MembershipService membershipService, TransferService transferService,
                       ExecutorService executorService, ServerSocket serverSocket) {
        this.storageService = storageService;
        this.membershipService = membershipService;
        this.transferService = transferService;
        this.executorService = executorService;
        this.serverSocket = serverSocket;
    }

    public void run() {
        try {
            System.out.println("Listening for TCP Messages in address " + serverSocket.getInetAddress() +
                    " port " + serverSocket.getLocalPort());
            while (true) {
                Socket socket = this.serverSocket.accept();
                DataInputStream istream = new DataInputStream(socket.getInputStream());
                DataOutputStream ostream = new DataOutputStream(socket.getOutputStream());

                Message message = new Message(istream.readAllBytes());
                executorService.submit(() -> {
                    try {
                        processEvent(message, ostream);
                        istream.close();
                        ostream.close();
                    } catch (IOException e) {
                        Message errorMsg = new Message(MessageTypes.REPLY.getCode(), "error", null);
                        try {
                            ostream.write(errorMsg.toBytes());
                            istream.close();
                            ostream.close();
                        } catch (IOException ioException) {
                            System.out.println("Error sending error message");
                        }
                    }
                });

                if (message.getAction().equals("exit"))
                    break;
            }
            serverSocket.close();
        } catch (SocketException se) {
            System.out.println("[TCPListener] Detected SocketException.");
        } catch (IOException e) {
            System.out.println("Error opening TCP server");
            throw new RuntimeException(e);
        }
    }

    private void processEvent(Message message, DataOutputStream ostream) throws IOException {
        final ByteArrayInputStream stream = new ByteArrayInputStream(message.getBody());
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(message.getBody())));

        Message reply;
        switch (message.getAction()) {
            case "electionRequest" -> {
                this.membershipService.handleElectionRequest(message, this.executorService);
                reply = new Message(MessageTypes.REPLY.getCode(), MessageTypes.OK.getCode(), "".getBytes(StandardCharsets.UTF_8));
            }
            case "electionLeave" -> {
                this.membershipService.handleElectionLeave(message);
                reply = new Message(MessageTypes.REPLY.getCode(), MessageTypes.OK.getCode(), "".getBytes(StandardCharsets.UTF_8));
            }
            case "join" -> {
                this.membershipService.handleMembershipResponse(message);

                reply = new Message(MessageTypes.REPLY.getCode(), MessageTypes.OK.getCode(), "".getBytes(StandardCharsets.UTF_8));
            }
            case "get" -> reply = storageService.get(new String(message.getBody()));
            case "put" -> {
                String key = reader.readLine();
                byte[] file = readFileBytes(key, stream);
                reply = storageService.put(key, file);
            }
            case "saveFile" -> {
                String key = reader.readLine();
                byte[] file = readFileBytes(key, stream);
                reply = storageService.saveFile(key, file);
            }
            case "getAndDelete" -> {
                String key = reader.readLine();
                reply = storageService.getAndDelete(key);
            }
            case "getFiles" -> {
                List<String> nodeFiles = storageService.getFiles();
                StringBuilder sb = new StringBuilder();

                if (nodeFiles != null) {
                    for (String fileName : nodeFiles) {
                        sb.append(fileName).append("\r\n");
                    }
                }

                reply = new Message(MessageTypes.REPLY.getCode(), MessageTypes.OK.getCode(), sb.toString().getBytes(StandardCharsets.UTF_8));
            }
            case "delete" -> reply = storageService.delete(new String(message.getBody()));
            case "safeDelete" -> reply = storageService.safeDelete(new String(message.getBody()));
            default -> {
                System.out.println("Invalid event received! - " + message.getAction());
                return;
            }
        }

        ostream.write(reply.toBytes());
    }

    private byte[] readFileBytes(String key, ByteArrayInputStream stream) {
        int offset = key.length() + 2; // 2 = \r\n

        // noinspection ResultOfMethodCallIgnored
        stream.skip(offset);
        return stream.readAllBytes();
    }
}
