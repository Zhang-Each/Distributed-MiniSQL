package MasterManagers.SocketManager;

// 处理Client发过来的消息的类，所有方法都要是static
public class ClientProcessor {

    public static String processClientCommand(String cmd) {
        System.out.println("要处理的命令：" + cmd);
        return "收到了客户端的请求。";
    }

}
