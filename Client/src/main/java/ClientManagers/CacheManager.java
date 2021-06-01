package ClientManagers;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓存管理
 */
public class CacheManager {
    //客户端缓存表
    private Map<String, Integer> cache;

    public CacheManager() {
        this.cache = new HashMap<>();
    }

    /**
     * 查询某张表是否存在客户端中，如果存在就直接返回表名
     * @param table 要查询的表名
     * @return
     */
    public Integer getCache(String table) {
        if (this.cache.containsKey(table)) {
            return this.cache.get(table);
        }
        return null;
    }

    /**
     * 在客户端缓存中存储已知的表和所在的服务器
     * @param table 数据表的名称
     * @param server 服务器的IP地址和端口号
     */
    public void setCache(String table, Integer server) {
        cache.put(table, server);
        System.out.println("存入缓存：表名" + table + " 端口号：" + table);
    }
}
