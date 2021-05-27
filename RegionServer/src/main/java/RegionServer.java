import RegionManagers.DataBaseManager;
import RegionManagers.RegionManager;
import RegionManagers.SocketManager.ClientThread;
import miniSQL.API;

import java.io.IOException;

public class RegionServer {
    public static void main(String[] args) throws Exception {
        //DataBaseManager dataBaseManager = new DataBaseManager();
        //dataBaseManager.showMetaInfo();
        API.initial();
        RegionManager regionManager = new RegionManager();
        regionManager.run();
    }
}
