package RegionManagers;

import RegionManagers.SocketManager.SocketManager;
import miniSQL.Interpreter;

import java.io.IOException;

// 整个Region Server的manager
public class RegionManager {
    private DataBaseManager dataBaseManager;
    private Interpreter interpreter;
    private SocketManager socketManager;

    private final int PORT = 22222;
    public RegionManager() throws IOException, InterruptedException {
        dataBaseManager = new DataBaseManager();
        interpreter = new Interpreter();
        socketManager = new SocketManager(PORT);
    }

    public void run() {

    }
}
