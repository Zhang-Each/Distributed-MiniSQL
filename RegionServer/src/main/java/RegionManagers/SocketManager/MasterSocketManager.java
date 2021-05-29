package RegionManagers.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

// 负责和主节点进行通信的类
public class MasterSocketManager implements Runnable {
    private Socket socket;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;

    public final int SERVER_PORT = 12345;
    public final String MASTER = "localhost";

    public MasterSocketManager() throws IOException {
        this.socket = new Socket(MASTER, SERVER_PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
    }

    public void sendToMaster(String cmd) {
        output.println("<client>" + cmd);
    }

    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("新消息>>>主服务器收到的信息是：" + line);
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
