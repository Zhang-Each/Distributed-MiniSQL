package MasterManagers.SocketManager;

import MasterManagers.TableManger;

import java.net.Socket;

/**
 * 1. 从节点启动，先完成zookeeper的注册，再将本节点存储的表名通过socket都发给主节点，格式是[1]name name name。
 *    如果没有存储表，则不用发
 * 2. 等待从节点的表格更改消息[2]name delete/add
 */
public class RegionProcessor {

    private TableManger tableManger;
    private Socket socket;

    public RegionProcessor(TableManger tableManger, Socket socket) {
        this.tableManger = tableManger;
        this.socket = socket;
    }

    public String processRegionCommand(String cmd) {
        String result = "";
        String ipAddress = socket.getInetAddress().getHostAddress();
        if (cmd.startsWith("[1]")) {
            String[] allTable = cmd.substring(3).split(" ");
            for(String temp : allTable) {
                tableManger.addTable(temp, ipAddress);
            }
        } else if (cmd.startsWith("[2]")) {
            String[] line = cmd.substring(3).split(" ");
            if(line[1].equals("delete")){
                tableManger.deleteTable(line[0]);
            }
            else if(line[1].equals("add")){
                tableManger.addTable(line[0],ipAddress);
            }
        }
        return result;
    }

}
