package RegionManagers.SocketManager;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

// 这里将负责和客户端进行socket通信的从节点socket manager改成多线程的模式

public class ClientSocketManager implements Runnable {

    private ServerSocket serverSocket;

    public ClientSocketManager(int port)
            throws IOException, InterruptedException {
        this.serverSocket = new ServerSocket(port);
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
                ClientThread clientThread = new ClientThread(socket);
                Thread thread = new Thread(clientThread);
                thread.start();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
