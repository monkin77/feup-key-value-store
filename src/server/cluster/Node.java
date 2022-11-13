package server.cluster;

import java.io.Serializable;

public record Node(String id, int port) implements Serializable {

    public String getId() {
        return id;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("Node #%s (port %d)", this.id, this.port);
    }
}
