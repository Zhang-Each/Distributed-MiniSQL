package MasterManagers;

import MasterManagers.SocketManager.SocketManager;

import java.io.IOException;

public class MasterManager {
    private ZookeeperManager zookeeperManager;
    private SocketManager socketManager;
    private TableManger tableManger;

    private final int PORT = 12345;

    public MasterManager() throws IOException, InterruptedException {
        tableManger = new TableManger();
        zookeeperManager = new ZookeeperManager(tableManger);
        socketManager = new SocketManager(PORT,tableManger);
    }

    public void initialize() throws InterruptedException, IOException {
        // 第一个线程在启动时向ZooKeeper发送请求，获得ZNODE目录下的信息并且持续监控，如果发生了目录的变化则执行回调函数，处理相应策略。
        Thread zkServiceThread = new Thread(zookeeperManager);
        zkServiceThread.start();

        // 第二个线程负责处理与从节点之间的通信，以及响应客户端的请求
        socketManager.startService();
    }
}
