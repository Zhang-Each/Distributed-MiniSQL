package MasterManagers.SocketManager;

import MasterManagers.TableManger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketManager {

    private ServerSocket serverSocket;
    private TableManger tableManger;

    public SocketManager(int port)
            throws IOException, InterruptedException {
        this.tableManger = new TableManger();
        this.serverSocket = new ServerSocket(port);
    }

    /**
     * 1. 主节点启动
     * 2. 从节点启动，先完成zookeeper的注册，再将本节点存储的表名通过socket都发给主节点，格式是<region>[1]name name name
     * 3. 等待从节点的表格更改消息<region>[2]name delete/add
     * 4. 等待客户端的表格查询信息<client>[1]name,返回<master>[1]ip
     * 5. 等待客户端的表格创建信息<client>[2]name,做负载均衡处理后返回<master>[2]ip
     */
    public void startService() throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(200);
            // 等待与之连接的客户端
            Socket socket = serverSocket.accept();
            // 建立子线程并启动
            SocketThread socketThread = new SocketThread(socket,tableManger);
            Thread thread = new Thread(socketThread);
            thread.start();
        }
    }
}
