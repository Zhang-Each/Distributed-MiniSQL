package MasterManagers;

import java.util.HashMap;
import java.util.Map;

public class ClientServiceManger {
    // 一个用于记录各种信息的表
    private Map<String, String> serverInfo;

    public ClientServiceManger() {
        serverInfo = new HashMap<String, String>();
    }

}
