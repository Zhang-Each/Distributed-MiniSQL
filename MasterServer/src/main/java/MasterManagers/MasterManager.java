package MasterManagers;

import MasterManagers.SocketManager.ClientSocketManager;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Map;

public class MasterManager {
    private ZookeeperManager zookeeperManager;
    private ServiceManger serviceManger;
    private ClientSocketManager socketManager;

    private final int PORT = 12345;

    public MasterManager() throws IOException, InterruptedException {
        zookeeperManager = new ZookeeperManager();
        serviceManger = new ServiceManger();
        socketManager = new ClientSocketManager(PORT, serviceManger);
    }

    public void initialize() throws KeeperException, InterruptedException {
        Map<String, String> meta = this.zookeeperManager.getRegionList();
        for (Map.Entry<String, String> entry: meta.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
