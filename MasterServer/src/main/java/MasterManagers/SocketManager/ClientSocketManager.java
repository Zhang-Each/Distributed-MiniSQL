package MasterManagers.SocketManager;

import MasterManagers.ServiceManger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientSocketManager {

    private ServerSocket serverSocket;
    private ServiceManger serviceManger;

    public ClientSocketManager(int port, ServiceManger serviceManger)
            throws IOException, InterruptedException {
        this.serverSocket = new ServerSocket(port);
        this.serviceManger = serviceManger;
        this.listenClient();
    }

    public void listenClient()
            throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(200);
            // 等待与之连接的客户端
            Socket socket = serverSocket.accept();
            // 建立子线程并启动
            ClientThread clientThread = new ClientThread(socket, serviceManger);
            Thread thread = new Thread(clientThread);
            thread.start();
        }
    }
}
