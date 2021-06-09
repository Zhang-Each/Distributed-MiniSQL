package RegionManagers.SocketManager;


import MasterManagers.utils.SocketUtils;
import miniSQL.API;
import miniSQL.Interpreter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * 客户端socket线程，负责和客户端进行通信
 */

public class ClientThread implements Runnable  {

    private Socket socket;
    private MasterSocketManager masterSocketManager;
    private boolean isRunning = false;
    private FtpUtils ftpUtils;

    public BufferedReader input = null;
    public PrintWriter output = null;

    public ClientThread(Socket socket, MasterSocketManager masterSocketManager)
            throws IOException {
        this.socket = socket;
        this.masterSocketManager = masterSocketManager;
        this.isRunning = true;
        this.ftpUtils = new FtpUtils();
        // 基于Socket建立输入输出流
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);
        System.out.println("服务端建立了新的客户端子线程：" + socket.getPort());
    }

    @Override
    public void run() {
        System.out.println("服务器监听客户端消息中" + socket.getInetAddress() + socket.getPort());
        String line;
        try {
            while (isRunning) {
                Thread.sleep(Long.parseLong("1000"));
                line = input.readLine();
                if (line != null) {
                    String result = this.commandProcess(line, socket.getInetAddress().toString());
                    if(!result.equals("No modified")) {
                        masterSocketManager.sendToMaster(result);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendToClient(String info) {
        output.println("<result>" + info);
    }

    // 从服务器处理接收到的命令，和出服务器的交互就在这一方法下面继续扩展

    public String commandProcess(String sql, String ip) throws Exception {
        System.out.println("要处理的命令：" + sql);
        String result = Interpreter.interpret(sql);
        API.store();
        sendToClient(result);
        sendTCToFTP();
        String[] parts = sql.split(" ");
        String[] res = result.split(" ");
        if(res[0].equals("-->Create")) {
            sendToFTP(res[2]);
            return "<region>[2]" + res[2] + " add";
        }
        else if(res[0].equals("-->Drop")) {
            deleteFromFTP(res[2]);
            return "<region>[2]" + res[2] + " delete";
        }
        else if(res[0].equals("-->Insert")) {
            System.out.println(parts[2]);
            deleteFromFTP(parts[2]);
            sendToFTP(parts[2]);
            System.out.println("success");
            return "No modified";
        }
        else if(res[0].equals("-->Delete")) {
            System.out.println(parts[2]);
            deleteFromFTP(parts[2]);
            sendToFTP(parts[2]);
            System.out.println("success");
            return "No modified";
        }
        else return "No modified";
    }

    public void sendToFTP(String fileName) {
        ftpUtils.uploadFile(fileName, "table");
        ftpUtils.uploadFile(fileName + "_index.index", "index");
    }

    public void deleteFromFTP(String fileName) {
        ftpUtils.deleteFile(fileName, "table");
        ftpUtils.deleteFile(fileName + "_index.index", "index");
    }

    public void sendTCToFTP() {
        ftpUtils.uploadFile("table_catalog", SocketUtils.getHostAddress(), "catalog");
        ftpUtils.uploadFile("index_catalog", SocketUtils.getHostAddress(), "catalog");
    }
}
