package server.storage;

import common.Message;

public interface KeyValue {
    Message put(String key, byte[] value);
    Message get(String key);
    Message delete(String key);
}
