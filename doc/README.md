# CPD Project2 - Compilation and Execution Instructions

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
