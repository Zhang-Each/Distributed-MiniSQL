package ClientManagers;

import java.io.IOException;

/**
 * 客户端管理程序
 */
public class ClientManager {
    private CacheManager cacheManager;
    private CommandManager commandManager;
    private SocketManager socketManager;

    public ClientManager() throws IOException {
        cacheManager = new CacheManager();
        socketManager = new SocketManager();
        commandManager = new CommandManager(cacheManager, socketManager);
    }

    public void run() throws IOException {
        System.out.println("Distributed-MiniSQL客户端启动！");
        commandManager.run();
    }
}
