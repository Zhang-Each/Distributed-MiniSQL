package MasterManagers;

import java.util.*;

public class TableManger {
    // 一个用于记录各种信息的表
    private Map<String, String> tableInfo;
    private Set<String> serverList;

    public TableManger() {
        serverList = new HashSet<>();
        tableInfo = new HashMap<>();
    }

    public void addTable(String table, String inetAddress) {
        tableInfo.put(table, inetAddress);
    }

    public void deleteTable(String table) {
        tableInfo.remove(table);
    }

    public String get(String table){
        return tableInfo.get(table);
    }

    public String getServerList(){
        Map<String,Integer> serverInfo = new HashMap<>();
        String result = "";
        for(String value : tableInfo.values()){
            serverInfo.compute(value, (k, v) -> {
                if (v == null) {
                    return 1;
                }
                return ++v;
            });
        }
        if(serverList.size()>serverInfo.size()){
            for(String temp: serverList){
                if(!serverInfo.containsKey(temp)){
                    return temp;
                }
            }
        }
        Integer min = Integer.MAX_VALUE;
        for(Map.Entry<String, Integer> entry : serverInfo.entrySet()){
            if(entry.getValue()<min){
                result = entry.getKey();
            }
        }
        return result;
    }

    public void addServer(String hostUrl) {
        serverList.add(hostUrl);
    }

    public void deleteServer(String hostUrl) {
        serverList.remove(hostUrl);
    }
    public int getNumOfServer(){
        return serverList.size();
    }
}
