import MasterManagers.MasterManager;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class MasterServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        MasterManager masterManager = new MasterManager();
        masterManager.initialize();
    }
}
