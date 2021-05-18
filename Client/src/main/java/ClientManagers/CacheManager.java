package ClientManagers;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓存管理
 */
public class CacheManager {
    //客户端缓存表
    private Map<String, String> cache;

    public CacheManager() {
        this.cache = new HashMap<>();
    }

    /**
     * 查询某张表是否存在客户端中，如果存在就直接返回表名
     * @param table 要查询的表名
     * @return
     */
    public String getTable(String table) {
        String res = cache.get(table);
        if (res.isEmpty()) {
            return null;
        } else {
            return res;
        }
    }

    /**
     * 在客户端缓存中存储已知的表和所在的服务器
     * @param table 数据表的名称
     * @param server 服务器的IP地址和端口号
     */
    public void setTable(String table, String server) {
        cache.put(table, server);
    }
}
