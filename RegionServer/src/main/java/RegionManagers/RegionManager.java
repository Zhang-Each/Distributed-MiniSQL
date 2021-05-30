package RegionManagers;

import RegionManagers.SocketManager.ClientSocketManager;
import RegionManagers.SocketManager.MasterSocketManager;
import miniSQL.Interpreter;

import java.io.IOException;

// 整个Region Server的manager
public class RegionManager {
    private DataBaseManager dataBaseManager;
    private Interpreter interpreter;
    private ClientSocketManager clientSocketManager;
    private MasterSocketManager masterSocketManager;
    private zkServiceManager zkServiceManager;

    private final int PORT = 22222;

    public RegionManager() throws IOException, InterruptedException {
        dataBaseManager = new DataBaseManager();
        interpreter = new Interpreter();
        clientSocketManager = new ClientSocketManager(PORT);
        Thread clientThread = new Thread(clientSocketManager);
        clientThread.start();
        masterSocketManager = new MasterSocketManager();
        zkServiceManager = new zkServiceManager();
        // 测试代码，测试region和master的沟通情况
//        masterSocketManager.sendToMaster(dataBaseManager.getMetaInfo());
//        Thread masterThread = new Thread(masterSocketManager);
//        masterThread.start();
    }

    public void run() {
        // 线程1：在应用启动的时候自动将本机的Host信息注册到ZooKeeper，然后阻塞，直到应用退出的时候也同时退出
        Thread zkServiceThread = new Thread(zkServiceManager);
        zkServiceThread.start();

        System.out.println("从节点开始运行！");
    }
}
