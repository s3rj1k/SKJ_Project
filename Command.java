import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

class Command {
    private static String sendData(String host, Integer port, Operation op, String from) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            if (from == null) {
                System.out.printf("Sending `%s` to host=`%s`, port=`%d`.\n", op, host, port);
            } else {
                System.out.printf("%s -> ] Sending `%s` to host=`%s`, port=`%d`.\n", from, op, host, port);
            }

            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(op);
            return in.readLine().trim();
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `%s` command on host=`%s`, port=`%d`.", op.operation, host, port));
        }
    }

    private static boolean resultNotOK(String result) {
        return Arrays.asList(Operation.resultNotOk).contains(result);
    }

    public static Boolean setValue(String host, Integer port, Operation op, String from) throws ApplicationException {
        return sendData(host, port, op, from).equals(Operation.resultOk);
    }

    public static KV getValue(String host, Integer port, Operation op, String from) throws ApplicationException {
        String result = sendData(host, port, op, from);
        if (resultNotOK(result)) {
            return null;
        }

        return new KV(result);
    }

    public static String findKey(String host, Integer port, Operation op, String from) throws ApplicationException {
        String result = sendData(host, port, op, from);
        if (resultNotOK(result)) {
            return null;
        }

        return result;
    }

    public static KV getMax(String host, Integer port, Operation op, String from) throws ApplicationException {
        String result = sendData(host, port, op, from);
        if (resultNotOK(result)) {
            return null;
        }

        return new KV(result);
    }

    public static KV getMin(String host, Integer port, Operation op, String from) throws ApplicationException {
        String result = sendData(host, port, op, from);
        if (resultNotOK(result)) {
            return null;
        }

        return new KV(result);
    }

    public static Boolean ping(String host, Integer port, String from) throws ApplicationException {
        Operation op = new Operation(Operation.operationPing);
        return sendData(host, port, op, from).equals(Operation.resultOk);
    }

    public static Boolean register(String host, Integer port, Integer localPort, String from) throws ApplicationException {
        Operation op = new Operation(String.format("%s %d", Operation.operationRegister, localPort));
        return sendData(host, port, op, from).equals(Operation.resultOk);
    }

    public static Boolean unregister(String host, Integer port, Integer localPort, String from) throws ApplicationException {
        Operation op = new Operation(String.format("%s %d", Operation.operationUnregister, localPort));
        return sendData(host, port, op, from).equals(Operation.resultOk);
    }
}
