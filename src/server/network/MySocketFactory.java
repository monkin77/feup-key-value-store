package server.network;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.RMISocketFactory;

public class MySocketFactory extends RMISocketFactory implements Serializable {
    final InetAddress ipInterface;

    public MySocketFactory(InetAddress ipInterface) {
        this.ipInterface = ipInterface;
    }

    @Override
    public Socket createSocket(String s, int port) throws IOException {
        return new Socket(this.ipInterface, port);
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port, 50, this.ipInterface);
    }
}
