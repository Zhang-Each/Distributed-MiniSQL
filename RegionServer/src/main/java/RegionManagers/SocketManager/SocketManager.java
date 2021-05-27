package RegionManagers.SocketManager;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketManager {

    private ServerSocket serverSocket;

    public SocketManager(int port)
            throws IOException, InterruptedException {
        this.serverSocket = new ServerSocket(port);
        this.listenClient();
    }

    public void listenClient()
            throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(1000);
            // 等待与之连接的客户端
            Socket socket = serverSocket.accept();
            // 建立子线程并启动
            ClientThread clientThread = new ClientThread(socket);
            Thread thread = new Thread(clientThread);
            thread.start();
        }
    }
}
