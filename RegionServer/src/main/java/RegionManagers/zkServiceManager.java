package RegionManagers;

import MasterManagers.ZookeeperManager;
import MasterManagers.utils.CuratorHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 *  @description: ZooKeeper 服务管理逻辑
 */
@Slf4j
public class zkServiceManager implements Runnable {
    @Override
    public void run() {
        this.serviceRegister();
    }

    private void serviceRegister() {
        try {
            // 向ZooKeeper注册临时节点
            CuratorHolder curatorClientHolder = new CuratorHolder();
            int nChildren = curatorClientHolder.getChildren(ZookeeperManager.ZNODE).size();
            curatorClientHolder.createNode(getRegisterPath()+nChildren,getHostAddress(),CreateMode.EPHEMERAL);

            // 阻塞该线程，直到发生异常或者主动退出
            synchronized (this) {
                wait();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    /**
     * 获得本机IP
     *
     * @return
     */
    private String getHostAddress() {
        String ip = null;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("获取本机IP失败");
        }
        return ip;
    }

    /**
     * @description: 获取Zookeeper注册的路径
     */
    private static String getRegisterPath() {
        return ZookeeperManager.ZNODE + "/" + ZookeeperManager.HOST_NAME_PREFIX;
    }
}

