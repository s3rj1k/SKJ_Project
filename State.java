import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class State {
    public static final Integer heartbeatInterval = 15 * 1000; // ms
    public static final Integer clientConnectionTimeout = 5 * 1000; // ms
    public static final Integer requestIdTTL = 300 * 1000; // ms

    static final String tcpPortCLIFlag = "-tcpport";
    static final String recordCLIFlag = "-record";
    static final String connectCLIFlag = "-connect";
    public DB db = new DB();
    public ExpiringStringSet RecursiveRequestsTracking = new ExpiringStringSet(requestIdTTL);
    public Map<InetSocketAddress, Boolean> nodes = new ConcurrentHashMap<>();
    private Integer tcpPort;

    public static InetSocketAddress parseAddress(String address) throws ApplicationException {
        try {
            URI uri = new URI(null, address, null, null, null);
            String host = uri.getHost();
            int port = uri.getPort();
            if (host == null || port == -1) {
                throw new ApplicationException(String.format("Invalid connection address value provided -> `%s`.", address));
            }
            return new InetSocketAddress(host, port);
        } catch (URISyntaxException e) {
            throw new ApplicationException(String.format("Invalid connection address value provided -> `%s`.", address));
        }
    }

    public Integer tcpPort() {
        return this.tcpPort;
    }

    public void init(String[] args) throws ApplicationException {
        System.setProperty("java.net.preferIPv4Stack", "true"); // Disable IPv6

        boolean IsTCPPortValueProvided = false;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(tcpPortCLIFlag)) {
                if (args.length >= (i + 1)) {
                    String param = args[i + 1];
                    try {
                        this.tcpPort = Integer.parseInt(param);
                        IsTCPPortValueProvided = true;
                    } catch (NumberFormatException e) {
                        throw new ApplicationException(String.format("Invalid value for `%s` provided -> `%s`.", tcpPortCLIFlag, param));
                    }
                } else {
                    throw new ApplicationException(String.format("No value for `%s` provided.", tcpPortCLIFlag));
                }
            }

            if (args[i].equals(recordCLIFlag)) {
                if (args.length >= (i + 1)) {
                    String record = args[i + 1].trim();
                    try {
                        KV kv = new KV(record);
                        if (!db.newValue(kv)) {
                            throw new ApplicationException(String.format("Failed to store `%s` value into database.", record));
                        }
                    } catch (IllegalArgumentException e) {
                        throw new ApplicationException(String.format("Invalid value for `%s` provided -> `%s`.", recordCLIFlag, record));
                    }
                } else {
                    throw new ApplicationException(String.format("No value for `%s` provided.", recordCLIFlag));
                }
            }

            if (args[i].equals(connectCLIFlag)) {
                if (args.length >= (i + 1)) {
                    String key = args[i + 1].trim();
                    try {
                        InetSocketAddress value = parseAddress(key);
                        nodes.put(value, false);
                    } catch (Exception e) {
                        throw new ApplicationException(String.format("Failed to store provided connection address value -> `%s`.", key));
                    }
                } else {
                    throw new ApplicationException(String.format("No value for `%s` provided.", connectCLIFlag));
                }
            }
        }

        if (!IsTCPPortValueProvided) {
            throw new ApplicationException(String.format("Required `%s` argument not provided.", connectCLIFlag));
        }
    }
}
