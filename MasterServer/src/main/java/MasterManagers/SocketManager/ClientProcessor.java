package MasterManagers.SocketManager;

// 处理Client发过来的消息的类，所有方法都要是static
public class ClientProcessor {

    public static String processClientCommand(String cmd) {
        System.out.println("要处理的命令：" + cmd);
        // 一个简单的测试，stu2位于运行在端口12345的Region上面，观察其是否能正常使用
        return "<table>stu2 22222";
    }

}
