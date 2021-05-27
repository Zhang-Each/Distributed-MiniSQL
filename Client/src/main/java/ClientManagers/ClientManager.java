package ClientManagers;

import ClientManagers.SocketManager.MasterSocketManager;
import ClientManagers.SocketManager.RegionSocketManager;

import java.io.IOException;

/**
 * 客户端管理程序
 */
public class ClientManager {
    private CacheManager cacheManager;
    private CommandManager commandManager;
    private MasterSocketManager masterSocketManager;
    private RegionSocketManager regionSocketManager;

    public ClientManager() throws IOException {
        cacheManager = new CacheManager();
        masterSocketManager = new MasterSocketManager();
        regionSocketManager = new RegionSocketManager();
        commandManager = new CommandManager(cacheManager, masterSocketManager, regionSocketManager);
    }

    public void run() throws IOException {
        System.out.println("Distributed-MiniSQL客户端启动！");
        commandManager.run();
    }
}
