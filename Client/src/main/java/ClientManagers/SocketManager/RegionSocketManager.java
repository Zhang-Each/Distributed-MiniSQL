package ClientManagers.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class RegionSocketManager {
    public Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private String region = "localhost";

    public RegionSocketManager() {

    }

    public void setRegionIP(String ip) {
        this.region = ip;
    }

    // 与Region建立连接
    public void connectRegionServer(int PORT) throws IOException {
        socket = new Socket(region, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
        this.listenToRegion();
        System.out.println("新消息>>>与从节点" + PORT + "建立Socket连接");
    }

    public void connectRegionServer(String ip) throws IOException {
        socket = new Socket(ip, 22222);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
        this.listenToRegion();
        System.out.println("新消息>>>与从节点" + ip + "建立Socket连接");
    }


    public void receiveFromRegion() throws IOException {
        String line = new String("");
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("新消息>>>从服务器收到的信息是：" + line);
        }
    }

    public void listenToRegion() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    public void closeRegionSocket() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.stop();
    }

    // 像从服务器发送信息的api
    public void sendToRegion(String info) {
        System.out.println("发送给从节点的消息是：" + info);
        output.println(info);
    }


    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println("新消息>>>客户端的从服务器监听线程启动！");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromRegion();
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
