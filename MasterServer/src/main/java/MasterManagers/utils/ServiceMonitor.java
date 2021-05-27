package MasterManagers.utils;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;

/**
 * ZooKeeper的节点监视器，将发生的事件进行处理，
 */
public class ServiceMonitor implements PathChildrenCacheListener {
    // 子节点出现变化对策略处理模块，未完成
//    private ServiceStrategyManager serviceStrategyManager;

    private CuratorHolder client;

    public ServiceMonitor(CuratorHolder curatorClientHolder) {
//        this.serviceStrategyManager = new ServiceStrategyManager();
        this.client = curatorClientHolder;
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
        String hostName, hostUrl;
        String eventPath = pathChildrenCacheEvent.getData() != null ? pathChildrenCacheEvent.getData().getPath() : null;

        // 接收到事件，对事件类型进行判断并执行相应策略
        switch (pathChildrenCacheEvent.getType()) {
            case CHILD_ADDED:
                System.out.println("服务器目录新增节点: " + pathChildrenCacheEvent.getData().getPath());
//                serviceStrategyManager.eventServerAppear(
//                        eventPath.replaceFirst(ZkConstant.ZNODE + "/", ""),
//                        client.getData(eventPath));
                break;
            case CHILD_REMOVED:
                System.out.println("服务器目录删除节点: " + pathChildrenCacheEvent.getData().getPath());
//                serviceStrategyManager.eventServerDisappear(
//                        eventPath.replaceFirst(ZkConstant.ZNODE + "/", ""));
                break;
            case CHILD_UPDATED:
                System.out.println("服务器目录更新节点: " + pathChildrenCacheEvent.getData().getPath());
//                serviceStrategyManager.eventServerUpdate(
//                        eventPath.replaceFirst(ZkConstant.ZNODE + "/", ""),
//                        client.getData(eventPath));
                break;
            default:
        }
    }
}
