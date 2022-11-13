package server;

public class Constants {
    public static final String membershipLogFileName = "membership.log";
    public static final String membershipCounterFileName = "membershipCounter.txt";
    public static final int numLogEvents = 32;
    public static final int maxResponseTime = 1000;    // TODO: CHECK THIS VALUES
    public static final int timeoutTime = 3000;
    public static final int multicastStepTime = 500;
    public static final int tcpStepTime = 100;
    public static final int numMembershipMessages = 3;
    public static final int replicationFactor = 3;
    public static final long tombstoneCheckIntervalMS = 200;
    public static final long tombstoneExpirationMS = 10000;
    public static final int electionPingTime = 1000;
    public static final long electionPingTimeout = 5000;
    public static final int invalidPort = -1;
}
