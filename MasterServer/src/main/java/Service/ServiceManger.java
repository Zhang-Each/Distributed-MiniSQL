package Service;

import java.util.HashMap;
import java.util.Map;

public class ServiceManger {
    // 一个用于记录各种信息的表
    private Map<String, String> serverInfo;

    public ServiceManger() {
        serverInfo = new HashMap<String, String>();
    }

}
