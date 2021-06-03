package RegionManagers.SocketManager;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

// 这里将负责和客户端进行socket通信的从节点socket manager改成多线程的模式

public class ClientSocketManager implements Runnable {

    private ServerSocket serverSocket;
    private MasterSocketManager masterSocketManager;
    private HashMap<Socket, Thread> clientHashMap;

    public ClientSocketManager(int port, MasterSocketManager masterSocketManager)
            throws IOException, InterruptedException {
        this.serverSocket = new ServerSocket(port);
        this.masterSocketManager = masterSocketManager;
        this.clientHashMap = new HashMap<Socket, Thread>();
    }

    public void listenClient()
            throws InterruptedException, IOException {

    }

    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(1000);
                // 等待与之连接的客户端
                Socket socket = serverSocket.accept();
                // 建立子线程并启动
                ClientThread clientThread = new ClientThread(socket, masterSocketManager);
                Thread thread = new Thread(clientThread);
                // 把子线程放入hashmap中
                this.clientHashMap.put(socket, thread);
                thread.start();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
