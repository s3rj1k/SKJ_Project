import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseNode {
    public static State state = new State();

    public static void main(String[] args) {
        try {
            state.init(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        int totalReadyNodes = 0;

        while (state.nodes.size() != totalReadyNodes) {
            for (InetSocketAddress node : state.nodes.keySet()) {
                if (state.nodes.get(node)) {
                    totalReadyNodes++;

                    continue;
                }

                try {
                    verify(node, null);
                    state.nodes.put(node, true);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }

        serve();
    }

    public static void serve() {
        try {
            final ExecutorService service = Executors.newCachedThreadPool();
            ServerSocket server;

            server = new ServerSocket(state.tcpPort());
            server.setReuseAddress(true);

            System.out.printf("Server started at TCP port: %d\n", server.getLocalPort());

            for (InetSocketAddress node : state.nodes.keySet()) {
                service.submit(new register(node, server));
            }

            Thread shutdownHook = new Thread(() -> {
                try {
                    server.close();
                    service.shutdownNow();
                    System.out.println("Server is shut down!");
                } catch (IOException e) {
                    System.err.printf("Error during handling shutdown hook: %s\n", e.getMessage());
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            service.submit(new heartbeat(server));

            while (!server.isClosed() && !service.isShutdown()) {
                Socket client;
                try {
                    client = server.accept();
                    service.submit(new ClientHandler(server, client));
                } catch (Exception e) {
                    if (!server.isClosed() && !service.isShutdown()) {
                        System.err.printf("Error during handling Server accept: %s\n", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void verify(InetSocketAddress address, String from) throws ApplicationException {
        if (!Command.ping(address.getHostName(), address.getPort(), from)) {
            throw new ApplicationException(String.format("Failed to verify remote node -> %s", address));
        }
    }

    private static void sleep(Integer millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception ignore) {
        }
    }

    private static void waitForReadiness(ServerSocket server) {
        while (!server.isBound()) {
            sleep(100);
        }
    }

    public static class register implements Runnable {
        private final InetSocketAddress node;
        private final ServerSocket server;

        public register(InetSocketAddress node, ServerSocket server) {
            this.node = node;
            this.server = server;
        }

        public void run() {
            waitForReadiness(server);

            String localAddress = new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString().startsWith("/")
                    ? new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString().substring(1)
                    : new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString();

            try {
                if (Command.register(this.node.getHostName(), this.node.getPort(), server.getLocalPort(), localAddress)) {
                    System.out.printf("Succeeded `register` operation on host=`%s`, port=`%d`, self TCP port %d.\n", this.node.getHostName(), this.node.getPort(), server.getLocalPort());
                } else {
                    System.err.printf("Failed `register` operation on host=`%s`, port=`%d`.\n", this.node.getHostName(), this.node.getPort());
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static class heartbeat implements Runnable {
        private final ServerSocket server;

        public heartbeat(ServerSocket server) {
            this.server = server;
        }

        public void run() {
            waitForReadiness(server);

            String localAddress = new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString().startsWith("/")
                    ? new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString().substring(1)
                    : new InetSocketAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()).toString();

            while (!server.isClosed()) {
                for (InetSocketAddress node : state.nodes.keySet()) {
                    sleep(State.heartbeatInterval);

                    try {
                        verify(node, localAddress);
                        if (state.nodes.replace(node, false, true)) {
                            System.out.printf("Heartbeat node online -> host=`%s`, port=`%d`\n", node.getHostName(), node.getPort());
                        }
                    } catch (Exception e) {
                        if (state.nodes.replace(node, true, false)) {
                            System.out.printf("Heartbeat node offline -> host=`%s`, port=`%d`\n", node.getHostName(), node.getPort());
                        }
                    }
                }
            }
        }
    }

    public static class ClientHandler implements Runnable {
        private final ServerSocket server;
        private final Socket client;

        public ClientHandler(ServerSocket server, Socket client) {
            this.server = server;
            this.client = client;
        }

        private void closeClientConnection(PrintWriter out, BufferedReader in, Socket client) {
            try {
                out.close();
                in.close();
                client.close();
            } catch (Exception ignored) {
            }
        }

        public void run() {
            String localAddress = client.getLocalSocketAddress().toString().startsWith("/")
                    ? client.getLocalSocketAddress().toString().substring(1)
                    : client.getLocalSocketAddress().toString();

            PrintWriter out;
            BufferedReader in;

            try {
                out = new PrintWriter(client.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(client.getInputStream()));

                String line;
                while (!client.isClosed() && (line = in.readLine()) != null) {
                    if (line.trim().length() == 0) {
                        continue;
                    }

                    String remoteClientAddress = client.getRemoteSocketAddress().toString().startsWith("/")
                            ? client.getRemoteSocketAddress().toString().substring(1)
                            : client.getRemoteSocketAddress().toString();

                    String remoteClientHost = client.getInetAddress().toString().startsWith("/")
                            ? client.getInetAddress().toString().substring(1)
                            : client.getInetAddress().toString();

                    Operation op;

                    try {
                        op = new Operation(line);
                    } catch (Exception e) {
                        System.err.printf("%s ] %s\n", remoteClientAddress, e.getMessage());
                        out.println(Operation.resultError);
                        closeClientConnection(out, in, client);

                        break;
                    }

                    // Set known request ID for DEBUG
                    // state.RecursiveRequestsTracking.set("ec58cb76-fbb0-4d71-8fc1-0b405ed49458");

                    if (Arrays.asList(Operation.knownRecursiveOperations).contains(op.operation)) {
                        if (op.id == null) {
                            op.id = UUID.randomUUID().toString();
                        } else {
                            try {
                                op.id = UUID.fromString(op.id).toString();
                            } catch (Exception e) {
                                System.err.printf("%s ] Invalid request ID, not UUID: %s\n", remoteClientAddress, e.getMessage());
                                out.println(Operation.resultError);
                                closeClientConnection(out, in, client);

                                break;
                            }

                            if (state.RecursiveRequestsTracking.contains(op.id)) {
                                System.out.printf("%s ] Already seen request ID: %s\n", remoteClientAddress, op.id);
                                out.println(Operation.resultSeen);
                                closeClientConnection(out, in, client);

                                break;
                            }
                        }

                        state.RecursiveRequestsTracking.set(op.id);
                    }

                    switch (op.operation) {
                        case Operation.operationSetValue: {
                            int OKCount = 0;

                            if (state.db.setValue(op.key, op.value)) {
                                OKCount++;
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    if (Command.setValue(node.getHostName(), node.getPort(), op, localAddress)) {
                                        OKCount++;
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            if (OKCount > 0) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                                out.println(Operation.resultOk);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetValue: {
                            Integer value = state.db.getValue(op.key);
                            if (value != null) {
                                String result = String.format("%s%s%s", op.key, Operation.parameterDelimiter, value);

                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, result);
                                out.println(result);

                                break;
                            }

                            int OKCount = 0;

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    KV kv = Command.getValue(node.getHostName(), node.getPort(), op, localAddress);
                                    if (kv != null) {
                                        System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, kv);
                                        out.println(kv);
                                        OKCount++;

                                        break;
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            if (OKCount == 0) {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationFindKey: {
                            if (state.db.exists(op.key)) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, localAddress);
                                out.println(localAddress);

                                break;
                            }

                            int OKCount = 0;

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    String value = Command.findKey(node.getHostName(), node.getPort(), op, localAddress);
                                    if (value != null) {
                                        System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, value);
                                        out.println(value);
                                        OKCount++;

                                        break;
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            if (OKCount == 0) {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetMax: {
                            KV max = null;
                            {
                                KV kv = state.db.getMax();
                                if (kv != null && kv.key != null && kv.value != null) {
                                    max = kv;
                                }
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    KV kv = Command.getMax(node.getHostName(), node.getPort(), op, localAddress);
                                    if (kv != null && kv.key != null && kv.value != null) {
                                        if (max != null && max.key != null && max.value != null) {
                                            if (max.value < kv.value) {
                                                max = kv;
                                            }
                                        } else {
                                            max = kv;
                                        }
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            if (max != null && max.key != null && max.value != null) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, max);
                                out.println(max);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetMin: {
                            KV min = null;
                            {
                                KV kv = state.db.getMin();
                                if (kv != null && kv.key != null && kv.value != null) {
                                    min = kv;
                                }
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    KV kv = Command.getMin(node.getHostName(), node.getPort(), op, localAddress);
                                    if (kv != null && kv.key != null && kv.value != null) {
                                        if (min != null && min.key != null && min.value != null) {
                                            if (min.value > kv.value) {
                                                min = kv;
                                            }
                                        } else {
                                            min = kv;
                                        }
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            if (min != null && min.key != null && min.value != null) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, min);
                                out.println(min);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationNewRecord: {
                            if (state.db.newValue(op.key, op.value)) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                                out.println(Operation.resultOk);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationTerminate: {
                            for (InetSocketAddress node : state.nodes.keySet()) {
                                try {
                                    if (Command.unregister(node.getHostName(), node.getPort(), server.getLocalPort(), localAddress)) {
                                        System.out.printf("Succeeded `unregister` operation on host=`%s`, port=`%d`, self TCP port %d.\n", node.getHostName(), node.getPort(), server.getLocalPort());
                                    } else {
                                        System.err.printf("Failed `unregister` operation on host=`%s`, port=`%d`.\n", node.getHostName(), node.getPort());
                                    }
                                } catch (Exception e) {
                                    System.err.println(e.getMessage());
                                }
                            }

                            System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                            out.println(Operation.resultOk);

                            closeClientConnection(out, in, client);
                            try {
                                System.exit(0);
                            } catch (Exception ignored) {
                            }

                            break;
                        }
                        case Operation.operationPing: {
                            System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                            out.println(Operation.resultOk);

                            break;
                        }
                        case Operation.operationRegister: {
                            InetSocketAddress node = new InetSocketAddress(remoteClientHost, op.key);
                            try {
                                verify(node, localAddress);
                                state.nodes.put(node, true);
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                                out.println(Operation.resultOk);
                            } catch (Exception e) {
                                System.err.printf("%s ] %s -> %s: %s\n", remoteClientAddress, op, Operation.resultError, e.getMessage());
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationUnregister: {
                            InetSocketAddress node = new InetSocketAddress(remoteClientHost, op.key);
                            try {
                                state.nodes.remove(node);
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultOk);
                                out.println(Operation.resultOk);
                            } catch (Exception ignore) {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op, Operation.resultError);
                                out.println(Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationListRemotes: {
                            StringBuilder sb = new StringBuilder();

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }
                                System.out.printf("%s ] %s -> host=`%s`, port=`%d`\n", remoteClientAddress, op, node.getHostName(), node.getPort());

                                sb.append(node);
                                sb.append(" ");
                            }

                            String result = sb.toString().trim();
                            if (result.length() == 0) {
                                out.println(Operation.resultEmpty);
                            } else {
                                out.println(result);
                            }

                            break;
                        }
                        default: {
                            System.err.printf("%s ] %s -> %s\n", remoteClientAddress, line.trim(), Operation.resultError);
                            out.println(Operation.resultError);

                            break;
                        }
                    }

                    // client connection needs to be closed
                    closeClientConnection(out, in, client);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
