package server.storage;

import server.Constants;

import java.io.*;

public class TombstoneManager implements Runnable {
    private final File tombstoneFolder;
    private final String dbFolder;
    private final String tombstoneFolderPath;

    public TombstoneManager(String dbFolder) {
        this.tombstoneFolderPath = dbFolder + "tombstones/";
        this.dbFolder = dbFolder;
        this.tombstoneFolder = new File(tombstoneFolderPath);
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(Constants.tombstoneCheckIntervalMS);
            } catch (InterruptedException ignored) {
            }

            File[] tombstones = tombstoneFolder.listFiles();
            if (tombstones == null || tombstones.length == 0) {
                continue;
            }

            for (File file : tombstones) {
                try {
                    String tombstonePath = tombstoneFolderPath + file.getName();
                    long timestamp = getTimestamp(tombstonePath);

                    if (System.currentTimeMillis() - timestamp > Constants.tombstoneExpirationMS) {

                        String realFilePath = dbFolder + file.getName();
                        synchronized (realFilePath.intern()) {
                            File realFile = new File(realFilePath);
                            if (!realFile.exists()) {
                                System.out.println("File corresponding to the tombstone does not exist: " + realFile.getName());
                                break; // It's possible to receive a delete request before the respective put request
                            }
                            if (!realFile.delete()) System.out.println("Error deleting real file: "+ realFile.getName());
                        }

                        synchronized (tombstonePath.intern()) {
                            if (!file.delete()) System.out.println("Error deleting tombstone file: " + file.getName());
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Error opening tombstone file in manager: " + file.getName());
                }
            }
        }
    }

    public static long getTimestamp(String filePath) throws IOException {
        synchronized (filePath.intern()) {
            File file = new File(filePath);
            try (DataInputStream fis = new DataInputStream(new FileInputStream(file))) {
                return fis.readLong();
            }
        }
    }
}
