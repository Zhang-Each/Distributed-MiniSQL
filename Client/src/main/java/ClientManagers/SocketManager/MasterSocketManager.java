package ClientManagers.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class MasterSocketManager {

    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    // 服务器的IP和端口号
    private final String master = "localhost";
    private final int PORT = 12345;

    public MasterSocketManager() throws IOException {
        socket = new Socket(master, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
        this.listenToMaster(); // 开启监听线程
    }

    // 像主服务器发送信息的api
    public void sendToMaster(String info) {
        output.println(info);
    }

    // 接收来自master server的信息并显示
    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("新消息>>>从服务器收到的信息是：" + line);
        }

    }

    // 开启一个监听线程
    public void listenToMaster() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    // 将sql语句发送到主服务器进一步处理，这里还有待进一步开发，目前仅供实验
    // 进一步开发在这个方法里面扩展
    //
    //
    //
    //
    //
    public void process(String sql, String server) {
        // 来处理sql语句
        System.out.println(sql);
        this.sendToMaster(sql);
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
                } catch (IOException e) {
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
