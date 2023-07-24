package org.uom.distributed.systems;


import org.java_websocket.server.WebSocketServer;
import org.uom.distributed.systems.server.MonitoringServer;

import java.net.InetSocketAddress;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner; //

public class Main {

    private final static String host = "localhost";
    private final static int port = 8887;
    private static WebSocketServer server = null;
    public static void main(String[] args) throws InterruptedException {
        String file = "";
        if (args.length != 0) {
            if (args[0].split("=")[0].equals("--file"))
                file = args[0].split("=")[1];
            else {
                System.out.println("run as : java -jar executableJarFileName.jar --file=\"/path/to/input.txt\"");
                System.exit(0);
            }
        }
        else {
            System.out.println("run as : java -jar executableJarFileName.jar --file=\"/path/to/input.txt\"");
            System.exit(0);
        }

        List<List<Integer>> nodes = parseFile(file);
        server = new MonitoringServer(new InetSocketAddress(host, port), nodes);
        Thread serverThread = new Thread(server::run);
        serverThread.start();

        serverThread.join();
    }

    public static List<List<Integer>> parseFile(String fileName) {
        List<List<Integer>> nodes = new ArrayList<>();
        try {
            File myObj = new File(fileName);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                System.out.println(data);
                boolean sub_array_start = false;
                boolean sub_array_end = false;
                StringBuilder intStringValue = new StringBuilder();
                List<Character> characters = new ArrayList<>();
                for (char value : data.toCharArray()) {
                    if (value == ' ') {
                        continue;
                    }
                    characters.add(value);
                }

                int sub_array_index = 0;

                List<Integer> node = new ArrayList<>();

                for (char value : characters) {
                    if (value == '(') {
                        sub_array_start = true;
                        sub_array_index = 0;
                        node = new ArrayList<>();
                        intStringValue = new StringBuilder();
                        continue;
                    } else if (value == ')') {
                        sub_array_start = false;
                        node.add(Integer.valueOf(intStringValue.toString()));
                        continue;
                    } else if (!sub_array_start && value == ',') {
                        continue;
                    }

                    if (sub_array_start) {
                        if (sub_array_index > 1) {
                            nodes.add(node);
                            intStringValue.append(value);
                            sub_array_index = 0;
                            continue;
                        }
                        if (isNumber(value)) {
                            intStringValue.append(value);
                        }
                        else if (value == ',') {
                            node.add(Integer.valueOf(intStringValue.toString()));
                            intStringValue = new StringBuilder();
                            sub_array_index++;
                        }
                    }
                }

                System.out.println(nodes);

            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return nodes;
    }

    private static boolean isNumber(char value) {
        if (
                value == '0' ||
                value == '1' ||
                value == '2' ||
                value == '3' ||
                value == '4' ||
                value == '5' ||
                value == '6' ||
                value == '7' ||
                value == '8' ||
                value == '9'
            ) {
            return true;
        }



        return false;
    }
}