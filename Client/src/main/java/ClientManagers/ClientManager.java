package ClientManagers;

import ClientManagers.SocketManager.MasterSocketManager;
import ClientManagers.SocketManager.RegionSocketManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * 客户端管理程序
 */

public class ClientManager {

    public CacheManager cacheManager;
    public MasterSocketManager masterSocketManager;
    public RegionSocketManager regionSocketManager;

    public ClientManager() throws IOException {
        // 绑定一个cacheManager
        this.cacheManager = new CacheManager();
        this.masterSocketManager = new MasterSocketManager(this);
        this.regionSocketManager = new RegionSocketManager();
    }

    // 在客户端做一个简单的interpreter，先对sql语句进行一个简单的解析，然后在客户端缓存中查询表是否已经存在
    public void run()
            throws IOException, InterruptedException {
        System.out.println("Distributed-MiniSQL客户端启动！");
        Scanner input = new Scanner(System.in);
        String line = "";
        StringBuilder sql = new StringBuilder();

        while (true) {
            // 读入一句完整的SQL语句
            System.out.println("新消息>>>请输入你想要执行的SQL语句：");
            // System.out.print("DisMiniSQL>>>");
            while (line.isEmpty() || line.charAt(line.length() - 1) != ';') {
                line = input.nextLine();
                if (line.isEmpty()) {
                    //System.out.print("          >>>");
                    continue;
                }
                //System.out.print("          >>>");
                sql.append(line);
                sql.append(' ');
            }
            line = "";
            System.out.println(sql.toString());
            if (sql.toString().trim().equals("quit;")) {
                this.masterSocketManager.closeMasterSocket();
                if (this.regionSocketManager.socket != null) {
                    this.regionSocketManager.closeRegionSocket();
                }
                break;
            }

            // 获得目标表名和索引名
            Map<String, String> target = this.interpreter(sql.toString());
            if (target.containsKey("error")) {
                System.out.println("新消息>>>输入有误，请重试！");
            }

            String table = target.get("name");
            table = table.substring(0, table.length() - 1);
            String cache = null;
            System.out.println("新消息>>>需要处理的表名是：" + table);
            if (target.get("cache").equals("true")) {
                cache = cacheManager.getCache(table);
                if (cache == null) {
                    System.out.println("新消息>>>客户端缓存中不存在该表！");
                } else {
                    System.out.println("新消息>>>客户端缓存中存在该表！其对应的服务器是：" + cache);
                }
            }

            // 如果cache里面没有找到表所对应的端口号，那么就去masterSocket里面查询
            if (cache == null) {
                this.masterSocketManager.process(sql.toString(), table);
            } else {
                // 如果查到了端口号就直接在RegionSocketManager中进行连接
                this.connectToRegion(cache, sql.toString());
            }
            sql = new StringBuilder();
        }
    }

    // 和从节点建立连接并发送SQL语句过去收到执行结果
    public void connectToRegion(int PORT, String sql) throws IOException, InterruptedException {
        this.regionSocketManager.connectRegionServer(PORT);
        Thread.sleep(100);
        this.regionSocketManager.sendToRegion(sql);
    }

    // 重载方法，使用ip地址进行连接，端口号固定为22222
    public void connectToRegion(String ip, String sql) throws IOException, InterruptedException {
        this.regionSocketManager.connectRegionServer(ip);
        Thread.sleep(100);
        this.regionSocketManager.sendToRegion(sql);
    }

    private Map<String, String> interpreter(String sql) {
        // 粗略地解析需要操作的table和index的名字
        Map<String, String> result = new HashMap<>();
        result.put("cache", "true");
        // 空格替换
        sql = sql.replaceAll("\\s+", " " );
        String[] words = sql.split(" ");
        if (words[0].equals("create")) {
            // 对应create table xxx和create index xxx
            // 此时创建新表，不需要cache
            result.put("cache", "false");
        } else if (words[0].equals("drop") || words[0].equals("insert") || words[0].equals("delete")) {
            // 这三种都是将table和index放在第三个位置的，可以直接取出
            String name = words[2].replace("(", "")
                    .replace(")", "").replace(";", "");
            result.put("name", name);
        } else if (words[0].equals("select")) {
            // select语句的表名放在from后面
            for (int i = 0; i < words.length; i ++) {
                if (words[i].equals("from") && i != words.length - 1) {
                    result.put("name", words[i + 1]);
                    break;
                }
            }
        }
        // 如果没有发现表名就说明出出现错误
        if (!result.containsKey("name")) {
            result.put("error", "true");
        }
        return result;
    }

}
