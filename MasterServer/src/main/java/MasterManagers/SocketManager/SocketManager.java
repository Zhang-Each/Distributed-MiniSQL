package MasterManagers.SocketManager;

import MasterManagers.ClientServiceManger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketManager {

    private ServerSocket serverSocket;
    private ClientServiceManger clientServiceManger;

    public SocketManager(int port)
            throws IOException, InterruptedException {
        this.clientServiceManger = new ClientServiceManger();
        this.serverSocket = new ServerSocket(port);
        this.listenClient();
    }

    public void listenClient()
            throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(200);
            // 等待与之连接的客户端
            Socket socket = serverSocket.accept();
            // 建立子线程并启动
            SocketThread socketThread = new SocketThread(socket, clientServiceManger);
            Thread thread = new Thread(socketThread);
            thread.start();
        }
    }
/**
 *  首先依次与各从节点通信，获得从节点存储的数据表。之后开始监听，并对消息进行分类处理
 *  1.如果是从节点的表的变更通知，如删除某张表，则更新主节点的元数据表
 *  2.如果是客户端的请求，则查询表对应的从节点在哪里，并返回相应结果
 */
    public void startService() throws InterruptedException {
        Thread.sleep(200000);
    }
}
