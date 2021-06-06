package MasterManagers.SocketManager;

import MasterManagers.TableManger;

import java.net.Socket;

/**
 * 1.等待客户端的表格查询信息<client>[1]name,返回<master>[1]ip name
 * 2.等待客户端的表格创建信息<client>[2]name,做负载均衡处理后返回<master>[2]ip name
 */
public class ClientProcessor {

    private TableManger tableManger;
    private Socket socket;

    public ClientProcessor(TableManger tableManger, Socket socket){
        this.tableManger = tableManger;
        this.socket = socket;
    }
    public String processClientCommand(String cmd) {
        String result = "";
        String tablename = cmd.substring(3);
        if (cmd.startsWith("[1]")) {
            result = "[1]"+tableManger.getInetAddress(tablename) +" "+ tablename;
        } else if (cmd.startsWith("[2]")) {
            result = "[2]"+tableManger.getBestServer() + " " +tablename;
        }
        return result;
    }

}
