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

    private final int PORT = 22222;

    public RegionManager() throws IOException, InterruptedException {
        dataBaseManager = new DataBaseManager();
        interpreter = new Interpreter();
        clientSocketManager = new ClientSocketManager(PORT);
        Thread clientThread = new Thread(clientSocketManager);
        clientThread.start();
        masterSocketManager = new MasterSocketManager();
        // 测试代码，测试region和master的沟通情况
        masterSocketManager.sendToMaster(dataBaseManager.getMetaInfo());
        Thread masterThread = new Thread(masterSocketManager);
        masterThread.start();
    }

    public void run() {
        System.out.println("从节点开始运行！");
    }
}
