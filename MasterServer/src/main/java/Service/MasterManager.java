package Service;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Map;

public class MasterManager {
    private ZookeeperManager zookeeperManager;
    private ServiceManger serviceManger;

    public MasterManager() throws IOException {
        zookeeperManager = new ZookeeperManager();
        serviceManger = new ServiceManger();
    }

    public void initialize() throws KeeperException, InterruptedException {
        Map<String, String> meta = this.zookeeperManager.getRegionList();
        for (Map.Entry<String, String> entry: meta.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
