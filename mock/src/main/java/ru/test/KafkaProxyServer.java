package ru.test;

import kafka.Kafka;
import kafka.network.SocketServer;
import kafka.security.CredentialProvider;
import kafka.server.KafkaConfig;
import kafka.server.Server;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import ru.test.server.KafkaRequestHandler;
import ru.test.server.KafkaServer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class KafkaProxyServer {

    public static void main(String[] args) {
//        try (ServerSocket serverSocket = new ServerSocket(9092)) {  // Порт, на который будет подключаться Kafka Producer
//            System.out.println("Kafka Proxy Server started...");
//            while (true) {
//                Socket socket = serverSocket.accept();
//                System.out.println("Client connected: " + socket.getPort());
//
//                // Создаем поток для обработки входящих сообщений от Producer
//                new Thread(new KafkaRequestHandler()).start();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Properties serverProps = Kafka.getPropsFromArgs(args);
        KafkaConfig config = KafkaConfig.fromProps(serverProps, false);

        KafkaServer kafkaServer = new KafkaServer(config, Time.SYSTEM);
        kafkaServer.startup();
    }

//    static class KafkaRequestHandler implements Runnable {
//
//        private Socket socket;
//
//        KafkaRequestHandler(Socket socket) {
//            this.socket = socket;
//        }
//
//        @Override
//        public void run() {
//            try (InputStream inputStream = socket.getInputStream();
//                 OutputStream outputStream = socket.getOutputStream()) {
//
//                // Чтение данных от Producer
//                byte[] requestBuffer = new byte[1024];
//                int bytesRead = inputStream.read(requestBuffer);
//                if (bytesRead != -1) {
//                    // Имитация обработки запроса
//                    System.out.println("Received message from Kafka Producer: " + new String(requestBuffer, 0, bytesRead));
//
//                    // Отправка ответа (потверждения) обратно в Producer
//                    String response = "Acknowledgement from Kafka Proxy Server";
//                    outputStream.write(response.getBytes());
//                    outputStream.flush();
//                    System.out.println("Sent acknowledgement to Kafka Producer");
//
//                }
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public static class KafkaRequestReader implements Runnable {
//
//        private ServerSocket serverSocket;
//
//        public KafkaRequestReader(ServerSocket socket) {
//            this.serverSocket = socket;
//        }
//
//        @Override
//        public void run() {
//
//                while (true) {
//                    try (Socket clientSocket = serverSocket.accept()) {
//                        System.out.println("Accepted connection from " + clientSocket.getInetAddress());
//
//                        DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
//
//                        // Читаем длину запроса
//                        int length = inputStream.readInt();
//                        System.out.println("Request length: " + length);
//
//                        // Читаем тело запроса
//                        byte[] requestBody = new byte[length];
//                        inputStream.readFully(requestBody);
//
//                        System.out.println(new String(requestBody));
//
//                        // Логируем запрос
//                        System.out.println("Received request:");
//                        logRequest(requestBody);
//
//                        // TODO: Обработка запроса и отправка ответа клиенту
//                    } catch (Exception e) {
//                        System.err.println("Error handling connection: " + e.getMessage());
//                    }
//                }
//        }
//
//        private static void logRequest(byte[] request) {
//            // Преобразуем запрос в удобный для чтения вид (например, hex)
//            StringBuilder sb = new StringBuilder();
//            for (byte b : request) {
//                sb.append(String.format("%02X ", b));
//            }
//            System.out.println(sb.toString());
//        }
//    }
}
