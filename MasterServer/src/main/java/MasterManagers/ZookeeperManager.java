package MasterManagers;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;

public class ZookeeperManager {
    private ZooKeeper zooKeeper;
    //ZooKeeper集群访问的端口
    public static final String ZK_HOST = "localhost:2181";
    //ZooKeeper会话超时时间
    public static final Integer ZK_SESSION_TIMEOUT = 3000;
    //ZooKeeper连接超时时间
    public static final Integer ZK_CONNECTION_TIMEOUT = 3000;


    public ZookeeperManager() throws IOException {
        // 初始化一个zookeeper节点
        zooKeeper = new ZooKeeper(ZK_HOST, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 发生变更的节点路径
                String path = watchedEvent.getPath();
                System.out.println("path:" + path);

                // 通知状态
                Watcher.Event.KeeperState state = watchedEvent.getState();
                System.out.println("KeeperState:" + state);

                // 事件类型
                Watcher.Event.EventType type = watchedEvent.getType();
                System.out.println("EventType:" + type);
            }
        });

    }

    /**
     * 获取所有服务器的信息，一般存储的是端口号，在zookeeper中所有的Region信息存储都要用Region_开头
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Map<String, String> getRegionList() throws KeeperException, InterruptedException {

        try {
            System.out.println("开始获取信息");
            List<String> children = zooKeeper.getChildren("/", false);
            Map<String, String> result = new HashMap<>();
            Stat stat = new Stat();
            for (String child: children) {
                if (child.startsWith("Region_")) {
                    byte[] node = zooKeeper.getData("/" + child, false, stat);
                    String info = new String (node);
                    result.put(child, info);
                }
            }
            return result;
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }

    // 一些zookeeper的测试代码
    public static void main(String[] args) throws IOException {
        String host = "localhost:2181";
        String zPath = "/";
        ZooKeeper zooKeeper = new ZooKeeper(host, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 发生变更的节点路径
                String path = watchedEvent.getPath();
                System.out.println("path:" + path);

                // 通知状态
                Watcher.Event.KeeperState state = watchedEvent.getState();
                System.out.println("KeeperState:" + state);

                // 事件类型
                Watcher.Event.EventType type = watchedEvent.getType();
                System.out.println("EventType:" + type);
            }
        });
        try {
            List<String> zooChild = zooKeeper.getChildren(zPath, false);
            System.out.println(zooChild.size());
            Stat stat = new Stat();
            for (String child: zooChild) {
                byte[] node = zooKeeper.getData("/" + child, false, stat);
                String info = new String (node);
                System.out.println(child + ": " + info);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
