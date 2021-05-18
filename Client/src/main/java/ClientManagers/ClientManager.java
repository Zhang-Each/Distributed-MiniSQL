package ClientManagers;

/**
 * 客户端管理程序
 */
public class ClientManager {
    private CacheManager cacheManager;
    private CommandManager commandManager;
    private SocketManager socketManager;

    public ClientManager() {
        cacheManager = new CacheManager();
        commandManager = new CommandManager();
        socketManager = new SocketManager();
    }

    public void run() {
        System.out.println("Distributed-MiniSQL客户端启动！");
    }
}
