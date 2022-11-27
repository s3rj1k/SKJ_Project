import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

class Client {
    public static Boolean setLocalValue(String host, Integer port, Integer key, Integer value) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s %d:%d\n", Operation.operationSetLocalValue, key, value);
            return in.readLine().trim().equals(Operation.resultOk);
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `setLocalValue` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static DB.KV getLocalValue(String host, Integer port, Integer key) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s %d\n", Operation.operationGetLocalValue, key);
            String result = in.readLine().trim();
            if (result.equals(Operation.resultError)) {
                return null;
            }

            DB.KV kv = new DB.KV();
            kv.key = Record.parseKey(result);
            kv.value = Record.parseValue(result);

            return kv;
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `getLocalValue` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static String findLocalKey(String host, Integer port, Integer key) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s %d\n", Operation.operationFindLocalKey, key);
            String result = in.readLine().trim();
            if (result.equals(Operation.resultError)) {
                return null;
            }

            return result;
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `findLocalKey` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static DB.KV getLocalMax(String host, Integer port) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s\n", Operation.operationGetLocalMax);
            String result = in.readLine().trim();
            if (result.equals(Operation.resultError)) {
                return null;
            }

            DB.KV kv = new DB.KV();
            kv.key = Record.parseKey(result);
            kv.value = Record.parseValue(result);

            return kv;
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `getLocalMax` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static DB.KV getLocalMin(String host, Integer port) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s\n", Operation.operationGetLocalMin);
            String result = in.readLine().trim();
            if (result.equals(Operation.resultError)) {
                return null;
            }

            DB.KV kv = new DB.KV();
            kv.key = Record.parseKey(result);
            kv.value = Record.parseValue(result);

            return kv;
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `getLocalMin` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static Boolean ping(String host, Integer port) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s\n", Operation.operationPing);
            return in.readLine().trim().equals(Operation.resultOk);
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `ping` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static Boolean registerSelf(String host, Integer port, Integer localPort) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s %d\n", Operation.operationRegister, localPort);
            return in.readLine().trim().equals(Operation.resultOk);
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `registerSelf` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }

    public static Boolean unregisterSelf(String host, Integer port, Integer localPort) throws ApplicationException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(State.clientConnectionTimeout);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.printf("%s %d\n", Operation.operationUnregister, localPort);
            return in.readLine().trim().equals(Operation.resultOk);
        } catch (Exception ex) {
            throw new ApplicationException(String.format("Failed `unregisterSelf` operation, remote node (host=`%s`, port=`%d`).", host, port));
        }
    }
}
