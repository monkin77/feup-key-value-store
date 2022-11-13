package common;

import server.Constants;

import java.io.*;
import java.net.*;

public class Sender {
    public static void sendMulticast(byte[] msg, String multicastIpAddr, int multicastIPPort) throws IOException {
        //noinspection resource
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(multicastIpAddr);

        DatagramPacket packet = new DatagramPacket(
                msg,
                msg.length,
                group,
                multicastIPPort);

        socket.send(packet);
    }

    public static byte[] sendTCPMessage(byte[] msg, String ipAddr, int ipPort) throws IOException {
        Socket socket = new Socket();;
        byte[] reply = new Message(MessageTypes.REPLY.getCode(), MessageTypes.TIMEOUT.getCode(), null).toBytes();
        try {
            socket.setSoTimeout(Constants.timeoutTime);
            socket.connect(new InetSocketAddress(ipAddr, ipPort), Constants.timeoutTime);
        } catch (IOException ioException) {
            return reply;
        }

        DataOutputStream ostream = new DataOutputStream(socket.getOutputStream());
        DataInputStream istream = new DataInputStream(socket.getInputStream());

        ostream.write(msg);
        socket.shutdownOutput();

        long maxResponseTime = System.currentTimeMillis() + Constants.timeoutTime;
        while (System.currentTimeMillis() < maxResponseTime) {
            // Node sent some data so it's alive
            if (istream.available() > 0) {
                reply = istream.readAllBytes();
                break;
            }

            // Otherwise, try again later
            try {
                Thread.sleep(Constants.tcpStepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        istream.close();
        socket.close();

        return reply;
    }
}
