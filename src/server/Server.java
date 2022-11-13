package server;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Server extends Remote {
    void join() throws RemoteException;
    void leave() throws RemoteException;
}
