package ClientManagers.SocketManager;

import ClientManagers.ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MasterSocketManager {

    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private ClientManager clientManager;

    // 服务器的IP和端口号
    private final String master = "localhost";
    private final int PORT = 12345;

    // 使用map来存储需要处理的表名-sql语句的对应关系
    Map<String, String> commandMap = new HashMap<>();

    public MasterSocketManager(ClientManager clientManager) throws IOException {
        socket = new Socket(master, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;

        this.clientManager = clientManager;

        this.listenToMaster(); // 开启监听线程
    }

    // 像主服务器发送信息的api
    // 要加上client标签，可以被主服务器识别
    public void sendToMaster(String info) {
        output.println("<client>[1]" + info);
    }

    public void sendToMasterCreate(String info) {
        output.println("<client>[2]" + info);
    }

    // 接收来自master server的信息并显示
    // 新增代码，查询主服务器中存储的表名和对应的端口号
    // 主服务器返回的内容的格式应该是"<table>table port"，因此args[0]和[1]分别代表了表名和对应的端口号
    public void receiveFromMaster() throws IOException, InterruptedException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("新消息>>>从服务器收到的信息是：" + line);
            // 已经废弃的方案
            if (line.startsWith("<table>")) {
                String[] args = line.substring(7).split(" ");
                String sql = commandMap.get(args[0]);
                System.out.println(sql);
                // 如果查到的端口号有对应的表
                if (sql != null) {
                    int PORT = Integer.parseInt(args[1]);
                    commandMap.remove(args[0]);
                    // 查询到之后在client的cache中设置一个缓存
                    this.clientManager.cacheManager.setCache(args[0], String.valueOf(PORT));
                    this.clientManager.connectToRegion(PORT, sql);
                }
            }
            // 主节点通信协议的解析方案
            else if (line.startsWith("<master>[1]") || line.startsWith("<master>[2]")) {
                // 截取ip地址
                String[] args = line.substring(11).split(" ");
                String ip = args[0], table = args[1];
                this.clientManager.cacheManager.setCache(table, ip);
                this.clientManager.connectToRegion(ip, commandMap.get(table));
            }
        }

    }

    // 开启一个监听线程
    public void listenToMaster() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    // 将sql语句发送到主服务器进一步处理，这里还有待进一步开发，目前仅供实验
    // 进一步开发在这个方法里面扩展

    public void process(String sql, String table) {
        // 来处理sql语句
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        this.sendToMaster(table);
    }

    public void processCreate(String sql, String table) {
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        System.out.println("存入table的是" + table + " " + sql);
        this.sendToMasterCreate(table);
    }

    // 关闭socket的方法，在输入quit的时候直接调用
    public void closeMasterSocket() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.stop();
    }


    // 用一个内部类来实现客户端的监听
    // 这里其实参考了Java应用技术里的聊天室的设计
    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println("新消息>>>客户端的主服务器监听线程启动！");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromMaster();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    sleep(100);
                } catch (InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
