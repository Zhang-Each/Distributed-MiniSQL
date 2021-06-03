import RegionManagers.DataBaseManager;
import RegionManagers.RegionManager;
import RegionManagers.SocketManager.ClientThread;
import miniSQL.API;

import java.io.IOException;

/**
 * 在主节点注册的时候，节点名前缀为"Region_"，data 为 url
 */

public class RegionServer {
    public static void main(String[] args) throws Exception {
        RegionManager regionManager = new RegionManager();
        regionManager.run();
    }
}
