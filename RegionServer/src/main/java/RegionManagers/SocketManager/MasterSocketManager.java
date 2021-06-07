package RegionManagers.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import lombok.SneakyThrows;
import miniSQL.API;
import miniSQL.Interpreter;
import RegionManagers.DataBaseManager;

// 负责和主节点进行通信的类
public class MasterSocketManager implements Runnable {
    private Socket socket;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private FtpUtils ftpUtils;
    private DataBaseManager dataBaseManager;
    private boolean isRunning = false;

    public final int SERVER_PORT = 12345;
    public final String MASTER = "localhost";

    public MasterSocketManager() throws IOException {
        this.socket = new Socket(MASTER, SERVER_PORT);
        this.ftpUtils = new FtpUtils();
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
    }

    public void sendToMaster(String modified_info) {
        output.println(modified_info);
    }

    public void sendTableInfoToMaster(String table_info) {
        output.println("<region>[1]" + table_info);
    }

    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            if (line.startsWith("<master>[3]")) {
                String tableName = line.substring(11);
                String[] tables = tableName.split(";");
                for(String table : tables) {
                    String[] values = table.split("@");
                    String t = values[0];
                    String sql = values[1] + ";";
                    Interpreter.interpret(sql);
                    try {
                        API.store();
                        API.initial();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    ftpUtils.downLoadFile("table", t, "/");
                    ftpUtils.downLoadFile("index", t + "_index.index", "/");
                }
                output.println("<region>[3]Complete disaster recovery");
            }
            else if (line.equals("<master>[4]recover")) {
                String tableName = dataBaseManager.getMetaInfo();
                String[] tableNames = tableName.split(" ");
                for(String table: tableNames) {
                    Interpreter.interpret("drop table " + table + " ;");
                    try {
                        API.store();
                        API.initial();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                output.println("<master>[4]Online");
            }
        }
    }

    @Override
    public void run() {
        System.out.println("新消息>>>从节点的主服务器监听线程启动！");
        while (isRunning) {
            if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                receiveFromMaster();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }
}
