package RegionManagers.SocketManager;


import miniSQL.API;
import miniSQL.Interpreter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.regex.*;

/**
 * 客户端socket线程，负责和客户端进行通信
 */

public class ClientThread implements Runnable  {

    private Socket socket;
    private MasterSocketManager masterSocketManager;
    private boolean isRunning = false;


    public BufferedReader input = null;
    public PrintWriter output = null;

    public ClientThread(Socket socket, MasterSocketManager masterSocketManager)
            throws IOException {
        this.socket = socket;
        this.masterSocketManager = masterSocketManager;
        this.isRunning = true;
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
                    String result = this.commandProcess(line);
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
    //
    //
    //
    //
    //
    //
    //
    public String commandProcess(String sql) throws Exception {
        System.out.println("要处理的命令：" + sql);
        String result = Interpreter.interpret(sql);
        this.sendToClient(result);
        String[] res = result.split(" ");
        String createPattern = "-->Create table .* successfully";
        String dropPattern = "-->Drop table .* successfully";
        if(res[0].equals("-->Create")) {
            API.store();
            API.initial();
            return "<region>[2]" + res[2] + " add";
        }
        else if(res[0].equals("-->Drop")) {
            API.store();
            API.initial();
            return "<region>[2]" + res[2] + " delete";
        }
        else return "No modified";
    }
}
