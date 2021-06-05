package MasterManagers.utils;

import MasterManagers.TableManger;
import MasterManagers.ZookeeperManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;

/**
 * ZooKeeper的节点监视器，将发生的事件进行处理，
 */
@Slf4j
public class ServiceMonitor implements PathChildrenCacheListener {

    private CuratorHolder client;
    private ServiceStrategyExecutor strategyExecutor;
    private TableManger tableManger;

    public ServiceMonitor(CuratorHolder curatorClientHolder, TableManger tableManger) {
        this.tableManger = tableManger;
        this.strategyExecutor = new ServiceStrategyExecutor(tableManger);
        this.client = curatorClientHolder;
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
        String hostName, hostUrl;
        String eventPath = pathChildrenCacheEvent.getData() != null ? pathChildrenCacheEvent.getData().getPath() : null;

        // 接收到事件，对事件类型进行判断并执行相应策略
        switch (pathChildrenCacheEvent.getType()) {
            case CHILD_ADDED:
                log.info("服务器目录新增节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerAppear(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        client.getData(eventPath));
                break;
            case CHILD_REMOVED:
                log.info("服务器目录删除节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerDisappear(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        new String(pathChildrenCacheEvent.getData().getData()));
                break;
            case CHILD_UPDATED:
                log.info("服务器目录更新节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerUpdate(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        client.getData(eventPath));
                break;
            default:
        }
    }

    /**
     * 处理服务器节点出现事件
     *
     * @param hostName
     * @param hostUrl
     */
    public void eventServerAppear(String hostName, String hostUrl) {
        log.warn("新增服务器节点：主机名 {}, 地址 {}", hostName, hostUrl);
        if (strategyExecutor.existServer(hostUrl)) {
            // 该服务器已经存在，即从失效状态中恢复
            log.warn("对该服务器{}执行恢复策略", hostUrl);
            strategyExecutor.execStrategy(hostUrl, StrategyTypeEnum.RECOVER);
        } else {
            // 新发现的服务器，新增一份数据
            strategyExecutor.addServer(hostUrl);
            log.warn("对该服务器{}执行新增策略", hostName);
            strategyExecutor.execStrategy(hostUrl, StrategyTypeEnum.DISCOVER);
        }
    }

    /**
     * 处理服务器节点失效事件
     *  @param hostName
     * @param hostUrl*/
    public void eventServerDisappear(String hostName, String hostUrl) {
        log.warn("删除服务器节点：主机名 {}, 地址 {}", hostName, hostUrl);
        tableManger.deleteServer(hostUrl);
        System.out.println(tableManger.getNumOfServer());
//        DataServer thisServer;
//        if (ServiceStrategyExecutor.DataHolder.dataServers.get(hostName) == null) {
//            throw new RuntimeException("需要删除信息的服务器不存在于服务器列表中");
//        } else {
//            // 更新并处理下线的服务器
//            thisServer = ServiceStrategyExecutor.DataHolder.dataServers.get(hostName);
//            log.warn("对该服务器{}执行失效策略", hostName);
//            strategyExecutor.execStrategy(thisServer, StrategyType.INVALID);
//        }
    }

    /**
     * 处理服务器节点更新事件
     *
     * @param hostName
     * @param hostUrl
     */
    public void eventServerUpdate(String hostName, String hostUrl) {
        log.warn("更新服务器节点：主机名 {}, 地址 {}", hostName, hostUrl);
//        DataServer thisServer;
//        if (ServiceStrategyExecutor.DataHolder.dataServers.get(hostName) == null) {
//            throw new RuntimeException("需要更新信息的服务器不存在于服务器列表中");
//        } else {
//            // 更新服务器的URL
//            thisServer = ServiceStrategyExecutor.DataHolder.dataServers.get(hostName);
//            thisServer.setHostUrl(hostUrl);
//        }
    }
}
