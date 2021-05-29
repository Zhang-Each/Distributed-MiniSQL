package MasterManagers.SocketManager;

import MasterManagers.ClientServiceManger;

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
    private ClientServiceManger clientServiceManger;
    private boolean isRunning = false;


    public BufferedReader input = null;
    public PrintWriter output = null;

    public ClientThread(Socket socket, ClientServiceManger clientServiceManger)
            throws IOException {
        this.socket = socket;
        this.clientServiceManger = clientServiceManger;
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
                    this.commandProcess(line);
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }

    public void sendToClient(String info) {
        output.println(info);
    }

    // 处理接收到的命令，和出服务器的交互就在这一方法下面继续扩展
    //
    //
    //
    //
    //
    //
    //
    public void commandProcess(String cmd) {
        System.out.println("要处理的命令：" + cmd);
        this.sendToClient("已收到发送的信息！");
    }
}
