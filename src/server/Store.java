package server;

import common.Message;
import common.Utils;
import server.cluster.MembershipService;
import server.cluster.Node;
import server.network.MySocketFactory;
import server.network.TCPListener;
import server.network.UDPListener;
import server.storage.StorageService;
import server.storage.TombstoneManager;
import server.storage.TransferService;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Store implements Server{
    private final String multicastIPAddr;
    private final int multicastIPPort;
    private final String nodeId;
    private final int storePort;
    private final MembershipService membershipService;
    private final StorageService storageService;
    private final TransferService transferService;

    private ExecutorService executorService;

    private ServerSocket serverSocket = null;
    private MulticastSocket multicastSocket = null;
    private boolean hasCrashed = false;

    public Store(String multicastIPAddr, int multicastIPPort, String nodeId, int storePort) throws RemoteException {
        this.multicastIPAddr = multicastIPAddr;
        this.multicastIPPort = multicastIPPort;
        this.nodeId = nodeId;
        this.storePort = storePort;

        executorService = Executors.newCachedThreadPool();

        this.membershipService = new MembershipService(multicastIPAddr, multicastIPPort, nodeId, storePort);
        this.storageService = new StorageService(membershipService.getNodeMap(), nodeId);
        this.storageService.setExecutorService(executorService);
        this.transferService = new TransferService(storageService, new Node(nodeId, storePort));

        // CHECK IF CRASHED (IF membershipCounter is EVEN - i.e part of the Cluster)
        this.checkNodeCrash();
    }
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Wrong number of arguments. Please invoke the program as:");
            System.out.println("java Store <IP_mcast_addr> <IP_mcast_port> <node_id> <Store_port>");
            System.exit(1);
        }

        final String multicastIPAddr = args[0];
        final int multicastIPPort = Integer.parseInt(args[1]);
        final String nodeId = args[2];
        final int storePort = Integer.parseInt(args[3]);

        try {
            InetAddress address = InetAddress.getByName(nodeId);
            MySocketFactory sf = new MySocketFactory(address);
            LocateRegistry.createRegistry(1099, null, sf);

            Store store = new Store(multicastIPAddr, multicastIPPort, nodeId, storePort);
            Server stub = (Server) UnicastRemoteObject.exportObject(store, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry(nodeId, 1099, sf);
            registry.bind("Server", stub);

            System.out.println("Server ready");
        } catch (RemoteException | AlreadyBoundException e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void join() throws RemoteException {
        executorService = Executors.newCachedThreadPool();
        this.storageService.setExecutorService(executorService);

        executorService.submit(() -> {
            // Checking serverSocket == null since when membershipCounter=0 it can be a member or not
            if ( (MembershipService.isClusterMember(this.membershipService.getMembershipCounter()) &&
                    this.membershipService.getMembershipCounter() != 0)  || (serverSocket != null)) {
                System.err.println("Attempting to join a cluster while being a member.");
                return;
            }

            try {
                try {
                    InetAddress addr = InetAddress.getByName(nodeId);
                    serverSocket = new ServerSocket(storePort, 50, addr);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                executorService.submit(new TCPListener(storageService, membershipService, transferService, executorService, serverSocket));

                this.membershipService.join();
                if (!this.hasCrashed)
                    this.transferService.join();
                else
                    this.transferService.recoverFromCrash();

                try {
                    multicastSocket = new MulticastSocket(multicastIPPort);
                    executorService.submit(new UDPListener(storageService, membershipService, transferService, executorService, multicastSocket));
                    executorService.submit(new TombstoneManager(storageService.getDbFolder()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch(RuntimeException re) {
                System.err.println(re.getMessage());
                // Clear ExecutorService threads?
            }
        });
    }

    @Override
    public void leave() throws RemoteException {
        executorService.submit(() -> {
            // Checking serverSocket == null since when membershipCounter=0 it can be a member or not
            if (!MembershipService.isClusterMember(this.membershipService.getMembershipCounter()) || (serverSocket == null)) {
                System.err.println("Attempting to leave the cluster while not being a member.");
                return;
            }

            try {
                try {
                    serverSocket.close();
                    serverSocket = null;

                    InetSocketAddress group = new InetSocketAddress(multicastIPAddr, multicastIPPort);
                    NetworkInterface netInf = NetworkInterface.getByIndex(0);
                    multicastSocket.leaveGroup(group, netInf);
                    multicastSocket.close();
                    multicastSocket = null;

                    if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                        System.out.println("Executor terminated.");
                    } else {
                        System.out.println("Executor still running");
                    }
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
                transferService.leave();
                membershipService.leave();
            } catch (RuntimeException re) {
                System.err.println(re.getMessage());
                // Clear ExecutorService threads?
            }
            executorService.shutdownNow();
        });
    }

    private void checkNodeCrash() throws RemoteException {
        if (this.membershipService.hasCrashed()) {
            this.hasCrashed = true;
            System.out.println("Restoring the Store state after crash...");
            // This means the node crashed while being a part
            this.membershipService.leave();
            this.join();

            this.hasCrashed = false;
        }
    }
}
