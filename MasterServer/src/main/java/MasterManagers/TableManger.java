package MasterManagers;

import MasterManagers.SocketManager.SocketThread;

import java.io.*;
import java.util.*;

/**
 * 需要一个用于记录所有连接过的ip的list
 * 需要一个用于记录当前活跃的ip，以及每个ip对应的table list
 */
public class TableManger {
    // 一个用于记录各种信息的表
    private Map<String, String> tableInfo;
    //一个用于记录所有连接过的从节点ip的list
    private List<String> serverList;
    //一个用于记录当前活跃的从节点ip，以及每个从节点ip对应的table list
    private Map<String,List<String>> aliveServer;
    // ip地址与相对应的socket
    private Map<String, SocketThread> socketThreadMap;

    public TableManger() throws IOException {
        serverList = new ArrayList<>();
        tableInfo = new HashMap<>();
        aliveServer = new HashMap<>();
        socketThreadMap = new HashMap<>();
    }

    public void addTable(String table, String inetAddress) {
        tableInfo.put(table, inetAddress);
        if(aliveServer.containsKey(inetAddress)){
            aliveServer.get(inetAddress).add(table);
        }
        else{
            List<String> temp = new ArrayList<>();
            temp.add(table);
            aliveServer.put(inetAddress,temp);
        }
    }

    public void deleteTable(String table, String inetAddress) {
        tableInfo.remove(table);
        aliveServer.get(inetAddress).removeIf(table::equals);

    }

    public String getInetAddress(String table){
        for(Map.Entry<String, String> entry : tableInfo.entrySet()){
            if(entry.getKey().equals(table)){
                return entry.getValue();
            }
        }
        return null;
    }

    public String getBestServer(){
        Integer min = Integer.MAX_VALUE;
        String result = "";
        for(Map.Entry<String, List<String>> entry : aliveServer.entrySet()){
            if(entry.getValue().size()<min){
                result = entry.getKey();
            }
        }
        return result;
    }
    public String getBestServer(String hostUrl){
        Integer min = Integer.MAX_VALUE;
        String result = "";
        for(Map.Entry<String, List<String>> entry : aliveServer.entrySet()){
            if(!entry.getKey().equals(hostUrl) && entry.getValue().size()<min){
                result = entry.getKey();
            }
        }
        return result;
    }

    public void addServer(String hostUrl) {
        if(!existServer(hostUrl))
            serverList.add(hostUrl);
        List<String> temp = new ArrayList<>();
        aliveServer.put(hostUrl,temp);
    }

    public boolean existServer(String hostUrl) {
        for(String s : serverList){
            if(s.equals(hostUrl))
                return true;
        }
        return false;
    }

    public List<String> getTableList(String hostUrl) {
        for(Map.Entry<String, List<String>> entry : aliveServer.entrySet()){
            if(entry.getKey().equals(hostUrl)){
                return  entry.getValue();
            }
        }
        return null;
    }

    public void addSocketThread(String hostUrl, SocketThread socketThread) {
        socketThreadMap.put(hostUrl,socketThread);
    }

    public SocketThread getSocketThread(String hostUrl) {
        for(Map.Entry<String, SocketThread> entry : socketThreadMap.entrySet()){
            if(entry.getKey().equals(hostUrl))
                return entry.getValue();
        }
        return null;
    }

    public void exchangeTable(String bestInet, String hostUrl) {
        List <String> tableList = getTableList(hostUrl);
        for(String table : tableList){
            tableInfo.put(table,bestInet);
        }
        List <String> bestInetTable = aliveServer.get(bestInet);
        bestInetTable.addAll(tableList);
        aliveServer.put(bestInet,bestInetTable);
        aliveServer.remove(hostUrl);
    }

    public void recoverServer(String hostUrl) {
        List<String> temp = new ArrayList<>();
        aliveServer.put(hostUrl,temp);
    }

//    public String getSql(String table) {
//        return name2sql.get(table);
//    }

//    public static void writeFile(String file, String conent) {
//        BufferedWriter out = null;
//        try {
//            out = new BufferedWriter(new OutputStreamWriter(
//                    new FileOutputStream(file, true)));
//            out.write(conent+"\r\n");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                out.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//    public void readFile(String file) throws IOException {
//        FileInputStream fileInputStream = new FileInputStream(file);
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
//        String line = null;
//        while ((line = bufferedReader.readLine()) != null) {
//            String []sql = line.split("@");
//            name2sql.put(sql[0],sql[1]);
//        }
//        fileInputStream.close();
//    }
}
