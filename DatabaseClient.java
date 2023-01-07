/* -----------------------------------------------------------------
 * Klasa klienta dla projektu "Rozproszona baza danych".
 * The client class for the "A distributed database" project.
 *
 * Kompilacja/Compilation:
 * javac DatabaseClient.java
 * Uruchomienie/Execution:
 * java DatabaseClient -gateway <adress>:<TCP port number> -operation <operation with parameters>
 *
 * Klient zakłada, że podane parametry są poprawne oraz, że jest ich odpowiednia
 * liczba. Nie jest sprawdzana poprawność wywołania.
 *
 * The client assumes, that the parameters are correct and there are enough
 * of them. Their correctness is not checked.
 *
 * SKJ, 2022/23, Łukasz Maśko
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class DatabaseClient {
    public static void main(String[] args) {
        // parameter storage
        String gateway = null;
        int port = 0;
        String command = null;
        boolean verbose = true;

        // Parameter scan loop
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-gateway":
                    String[] gatewayArray = args[++i].split(":");
                    gateway = gatewayArray[0];
                    port = Integer.parseInt(gatewayArray[1]);
                    break;
                case "-operation":
                    break;
                default:
                    if (command == null) command = args[i];
                    else if (!"TERMINATE".equals(command)) command += " " + args[i];
            }
        }

        if ((System.getenv("verbose") != null) && (System.getenv("verbose").equalsIgnoreCase("false"))) {
            verbose = false;
        }

        // communication socket and streams
        Socket netSocket;
        PrintWriter out;
        BufferedReader in;
        try {
            if (verbose) {
                System.out.println(); // to visually separate commands
                System.out.println("Connecting with: " + gateway + " at port " + port);
            }

            netSocket = new Socket(gateway, port);
            out = new PrintWriter(netSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(netSocket.getInputStream()));

            if (verbose) {
                System.out.println("Connected");
                System.out.println("Sending: " + command);
            }

            out.println(command);
            // Read and print out the response
            String response;
            while ((response = in.readLine()) != null) {
                if (verbose) {
                    System.out.println(response);
                } else {
                    System.out.printf("{%s} -> [%s] -> %s\n", new InetSocketAddress(gateway, port), command, response);
                }
            }

            // Terminate - close all the streams and the socket
            out.close();
            in.close();
            netSocket.close();
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + gateway + ".");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("No connection with " + gateway + ".");
            System.exit(1);
        }
    }
}
