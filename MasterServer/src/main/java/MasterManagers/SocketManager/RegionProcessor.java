package MasterManagers.SocketManager;

// 处理Region发过来的消息的类，所有方法都要是static
public class RegionProcessor {

    public static String processRegionCommand(String cmd) {
        System.out.println("要处理的命令：" + cmd);
        return "收到了从节点的请求！";
    }

}
