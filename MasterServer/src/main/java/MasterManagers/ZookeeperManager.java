package MasterManagers;

import MasterManagers.utils.CuratorHolder;
import MasterManagers.utils.ServiceMonitor;
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
    //ZooKeeper集群内各个服务器注册的节点路径
    public static final String ZNODE = "/db";
    //ZooKeeper集群内各个服务器注册自身信息的节点名前缀
    public static final String HOST_NAME_PREFIX = "Region_";


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
    public static void main(String[] args) throws Exception {
        try {
            // 开启一个连接
            CuratorHolder curatorClientHolder = new CuratorHolder(ZK_HOST);
            // 创建服务器主目录
            if (!curatorClientHolder.checkNodeExist(ZNODE)){
                curatorClientHolder.createNode(ZNODE,"服务器主目录");
            }
            System.out.println("服务器目录下有子节点：" + curatorClientHolder.getChildren(ZNODE));

            // 开始监听服务器目录，如果有节点的变化，则处理相应事件
            curatorClientHolder.monitorChildrenNodes(ZNODE, new ServiceMonitor(curatorClientHolder));
        } catch (Exception e) {
            throw e;
        }
    }
}
