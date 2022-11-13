package common;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Message Structure
 * | type             |     ( request or reply )
 * | action           |     ( join/leave/get/put/delete )
 * | CRLF             |
 * | Body             |
 */
public class Message {
    static public int MAX_MSG_SIZE = 10000;
    private final String type;
    private final String action;
    private final byte[] body;

    public Message(String type, String action, byte[] body) {
        this.type = type;
        this.action = action;
        this.body = body;
    }

    public Message(byte[] bytes) throws IOException {
        final ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
        this.type = reader.readLine();
        this.action = reader.readLine();
        reader.readLine(); // last empty line

        int bodyOffset = type.length() + action.length() + 6; // 2 chars used for newlines per row on top

        //noinspection ResultOfMethodCallIgnored
        stream.skip(bodyOffset);
        this.body = stream.readAllBytes();
    }

    /**
     * Builds a Message according to our Message Structure
     * where the first line is the header, in the format
     * Type
     * Action
     * (empty line)
     * Body
     * @return Byte array with the message
     */
    public byte[] toBytes() throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(type).append(Utils.newLine);
        sb.append(action).append(Utils.newLine);

        // empty line
        sb.append(Utils.newLine);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        if (body != null) out.write(body);

        return out.toByteArray();
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public byte[] getBody() {
        return body;
    }
}
