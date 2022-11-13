# CPD Project2 - Distributed and Partitioned Key-Value Store

This project consists of a **Distributed key-value persistent store** for a large cluster. The main features requested were implemented: **Membership Service**, **Storage Srevice**, including a third internal service responsible for transferring files between nodes in membership events. Aside from those, the following enhancements were implemented:
- **Replication** - to increase availability, key-value pairs are replicated with a given factor.
- **Fault-Tolerance** - the system must handle failures of nodes and recover from them.
- **Concurrency** - Having a large distributed system requires thread paralelization in each node. Solving possible concurrenc problems (eg. race conditions) is an important factor of the system.
- **RMI** - Remote Method Invocation provides an easy-to-user interface to the client, facilitating the integration of our system.

For more details, please refer to the [Project Report](./doc/report.pdf)

## How to compile the program
First, make sure you have the Java JDK updated to at least version 17.
Under the `src` folder, run the following command in the terminal:
```
javac client/*.java common/*.java server/*.java server/cluster/*.java server/network/*.java server/storage/*.java
```

## How to execute the program

In order to run the Store, run the following command in the `src` folder:
```
java -cp . server.Store <IP_mcast_addr> <IP_mcast_port> <node_id> <Store_port>
```

In order to run the TestClient, run the following command in the `src` folder:
```
java -cp . client.TestClient <node_ap> <operation> [<opnd>]
```

## Final Grade: 18.20/20.00
