import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseNode {
    public static State state = new State();

    public static void main(String[] args) {
        try {
            state.init(args);
        } catch (Exception e) {
            System.err.printf("%s\n", e.getMessage());
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
                    verify(node);
                    state.nodes.put(node, true);
                } catch (Exception e) {
                    System.err.printf("%s\n", e.getMessage());
                }
            }
        }

        serve();
    }

    public static void serve() {
        try {
            final ExecutorService service = Executors.newCachedThreadPool();
            ServerSocket server;

            server = new ServerSocket(state.tcpPort);
            server.setReuseAddress(true);

            System.out.printf("Server started at TCP port: %d\n", server.getLocalPort());

            for (InetSocketAddress node : state.nodes.keySet()) {
                service.submit(new registerSelf(node, server));
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

    public static void verify(InetSocketAddress address) throws ApplicationException {
        if (!Client.ping(address.getHostName(), address.getPort())) {
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

    public static class registerSelf implements Runnable {
        private final InetSocketAddress node;
        private final ServerSocket server;

        public registerSelf(InetSocketAddress node, ServerSocket server) {
            this.node = node;
            this.server = server;
        }

        public void run() {
            waitForReadiness(server);

            try {
                if (Client.registerSelf(this.node.getHostName(), this.node.getPort(), server.getLocalPort())) {
                    System.out.printf("Succeeded `registerSelf` operation, remote node (host=`%s`, port=`%d`), self TCP port %d.\n", this.node.getHostName(), this.node.getPort(), server.getLocalPort());
                } else {
                    System.err.printf("Failed `registerSelf` operation, remote node (host=`%s`, port=`%d`).\n", this.node.getHostName(), this.node.getPort());
                }
            } catch (Exception e) {
                System.err.printf("%s\n", e.getMessage());
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

            while (!server.isClosed()) {
                for (InetSocketAddress node : state.nodes.keySet()) {
                    sleep(State.heartbeatInterval);

                    try {
                        verify(node);
                        if (state.nodes.replace(node, false, true)) {
                            System.out.printf("Heartbeat -> remote node (host=`%s`, port=`%d`), online\n", node.getHostName(), node.getPort());
                        }
                    } catch (Exception e) {
                        if (state.nodes.replace(node, true, false)) {
                            System.out.printf("Heartbeat -> remote node (host=`%s`, port=`%d`), offline\n", node.getHostName(), node.getPort());
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

                    Operation op = new Operation();

                    try {
                        op.parse(line);
                    } catch (Exception e) {
                        System.err.printf("%s ] %s\n", remoteClientAddress, e.getMessage());
                        out.printf("%s\n", Operation.resultError);

                        // client connection needs to be closed
                        try {
                            out.close();
                            in.close();
                            client.close();
                        } catch (Exception ignored) {
                        }

                        continue;
                    }

                    switch (op.operation) {
                        case Operation.operationSetLocalValue: {
                            if (state.db.setLocalValue(op.key, op.value)) {
                                System.out.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            } else {
                                System.err.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationSetValue: {
                            int OKCount = 0;

                            if (state.db.setLocalValue(op.key, op.value)) {
                                OKCount++;
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    if (Client.setLocalValue(node.getHostName(), node.getPort(), op.key, op.value)) {
                                        OKCount++;
                                    }
                                } catch (Exception e) {
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            if (OKCount > 0) {
                                System.out.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            } else {
                                System.err.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetLocalValue: {
                            Integer value = state.db.getLocalValue(op.key);
                            if (value != null) {
                                System.out.printf("%s ] %s %s -> %s:%s\n", remoteClientAddress, op.operation, op.key, op.key, value);
                                out.printf("%s:%s\n", op.key, value);
                            } else {
                                System.err.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetValue: {
                            Integer value = state.db.getLocalValue(op.key);
                            if (value != null) {
                                System.out.printf("%s ] %s %s -> %s:%s\n", remoteClientAddress, op.operation, op.key, op.key, value);
                                out.printf("%s:%s\n", op.key, value);

                                break;
                            }

                            int OKCount = 0;

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    DB.KV kv = Client.getLocalValue(node.getHostName(), node.getPort(), op.key);
                                    if (kv != null) {
                                        System.out.printf("%s ] %s %s -> %s:%s\n", remoteClientAddress, op.operation, op.key, kv.key, kv.value);
                                        out.printf("%s:%s\n", kv.key, kv.value);
                                        OKCount++;

                                        break;
                                    }
                                } catch (Exception e) {
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            if (OKCount == 0) {
                                System.err.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationFindLocalKey: {
                            if (state.db.existsLocally(op.key)) {
                                System.out.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, localAddress);
                                out.printf("%s\n", localAddress);
                            } else {
                                System.err.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationFindKey: {
                            if (state.db.existsLocally(op.key)) {
                                System.out.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, localAddress);
                                out.printf("%s\n", localAddress);

                                break;
                            }

                            int OKCount = 0;

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    String value = Client.findLocalKey(node.getHostName(), node.getPort(), op.key);
                                    if (value != null) {
                                        System.out.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, value);
                                        out.printf("%s\n", value);
                                        OKCount++;

                                        break;
                                    }
                                } catch (Exception e) {
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            if (OKCount == 0) {
                                System.err.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetLocalMax: {
                            DB.KV kv = state.db.getLocalMax();
                            if (kv != null && kv.key != null && kv.value != null) {
                                System.out.printf("%s ] %s -> %s:%s\n", remoteClientAddress, op.operation, kv.key, kv.value);
                                out.printf("%s:%s\n", kv.key, kv.value);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetMax: {
                            DB.KV max = null;
                            {
                                DB.KV kv = state.db.getLocalMax();
                                if (kv != null && kv.key != null && kv.value != null) {
                                    max = kv;
                                }
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    DB.KV kv = Client.getLocalMax(node.getHostName(), node.getPort());
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
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            if (max != null && max.key != null && max.value != null) {
                                System.out.printf("%s ] %s -> %s:%s\n", remoteClientAddress, op.operation, max.key, max.value);
                                out.printf("%s:%s\n", max.key, max.value);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetLocalMin: {
                            DB.KV kv = state.db.getLocalMin();
                            if (kv != null && kv.key != null && kv.value != null) {
                                System.out.printf("%s ] %s -> %s:%s\n", remoteClientAddress, op.operation, kv.key, kv.value);
                                out.printf("%s:%s\n", kv.key, kv.value);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationGetMin: {
                            DB.KV min = null;
                            {
                                DB.KV kv = state.db.getLocalMin();
                                if (kv != null && kv.key != null && kv.value != null) {
                                    min = kv;
                                }
                            }

                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }

                                try {
                                    DB.KV kv = Client.getLocalMin(node.getHostName(), node.getPort());
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
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            if (min != null && min.key != null && min.value != null) {
                                System.out.printf("%s ] %s -> %s:%s\n", remoteClientAddress, op.operation, min.key, min.value);
                                out.printf("%s:%s\n", min.key, min.value);
                            } else {
                                System.err.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationNewRecord: {
                            if (state.db.newLocalValue(op.key, op.value)) {
                                System.out.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            } else {
                                System.err.printf("%s ] %s %s:%s -> %s\n", remoteClientAddress, op.operation, op.key, op.value, Operation.resultError);
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationUnregister: {
                            InetSocketAddress node = new InetSocketAddress(remoteClientHost, op.key);
                            try {
                                state.nodes.remove(node);
                                System.out.printf("%s ] %s %s -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            } catch (Exception ignore) {
                            }

                            break;
                        }
                        case Operation.operationTerminate: {
                            for (InetSocketAddress node : state.nodes.keySet()) {
                                try {
                                    if (Client.unregisterSelf(node.getHostName(), node.getPort(), server.getLocalPort())) {
                                        System.out.printf("Succeeded `unregisterSelf` operation, remote node (host=`%s`, port=`%d`), self TCP port %d.\n", node.getHostName(), node.getPort(), server.getLocalPort());
                                    } else {
                                        System.err.printf("Failed `unregisterSelf` operation, remote node (host=`%s`, port=`%d`).\n", node.getHostName(), node.getPort());
                                    }
                                } catch (Exception e) {
                                    System.err.printf("%s\n", e.getMessage());
                                }
                            }

                            System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultOk);
                            out.printf("%s\n", Operation.resultOk);

                            try {
                                out.close();
                                in.close();
                                client.close();
                                System.exit(0);
                            } catch (Exception ignored) {
                            }

                            break;
                        }
                        case Operation.operationQuit: {
                            System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultOk);
                            out.printf("%s\n", Operation.resultOk);

                            try {
                                out.close();
                                in.close();
                                client.close();
                            } catch (Exception ignored) {
                            }

                            break;
                        }
                        case Operation.operationPing: {
                            System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultOk);
                            out.printf("%s\n", Operation.resultOk);

                            break;
                        }
                        case Operation.operationRegister: {
                            InetSocketAddress node = new InetSocketAddress(remoteClientHost, op.key);
                            try {
                                verify(node);
                                state.nodes.put(node, true);
                                System.out.printf("%s ] %s %d -> %s\n", remoteClientAddress, op.operation, op.key, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            } catch (Exception e) {
                                System.err.printf("%s ] %s %s -> %s (%s)\n", remoteClientAddress, op.operation, op.key, Operation.resultError, e.getMessage());
                                out.printf("%s\n", Operation.resultError);
                            }

                            break;
                        }
                        case Operation.operationListRemotes: {
                            for (InetSocketAddress node : state.nodes.keySet()) {
                                if (!state.nodes.get(node)) {
                                    continue;
                                }
                                System.out.printf("%s ] %s -> host=`%s`, port=`%d`\n", remoteClientAddress, op.operation, node.getHostName(), node.getPort());
                                out.printf("host=`%s`, port=`%d`\n", node.getHostName(), node.getPort());
                            }

                            if (state.nodes.size() == 0) {
                                System.out.printf("%s ] %s -> %s\n", remoteClientAddress, op.operation, Operation.resultOk);
                                out.printf("%s\n", Operation.resultOk);
                            }

                            break;
                        }
                        default: {
                            System.err.printf("%s ] %s -> %s\n", remoteClientAddress, line.trim(), Operation.resultError);
                            out.printf("%s\n", Operation.resultError);

                            break;
                        }
                    }

                    // client connection needs to be closed
                    try {
                        out.close();
                        in.close();
                        client.close();
                    } catch (Exception ignored) {
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
