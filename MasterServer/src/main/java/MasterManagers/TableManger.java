package MasterManagers;

import java.util.*;

public class TableManger {
    // 一个用于记录各种信息的表
    private Map<String, String> tableInfo;
    private List<String> serverList;

    public TableManger() {
        serverList = new ArrayList<>();
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

    public String getBestServer(){
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
        if(!existServer(hostUrl))
            serverList.add(hostUrl);
    }

    public void deleteServer(String hostUrl) {
        serverList.removeIf(hostUrl::equals);
    }
    public int getNumOfServer(){
        return serverList.size();
    }

    public boolean existServer(String hostUrl) {
        for(String s : serverList){
            if(s.equals(hostUrl))
                return true;
        }
        return false;
    }
}
