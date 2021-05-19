package miniSQL.CATALOGMANAGER;

import miniSQL.INDEXMANAGER.Index;

import java.io.*;
import java.util.*;

public class CatalogManager {

    private static LinkedHashMap<String, Table> tables = new LinkedHashMap<>();
    private static LinkedHashMap<String, Index> indexes = new LinkedHashMap<>();
    private static String tableFilename = "table_catalog";
    private static String indexFilename = "index_catalog";

    public static void initialCatalog() throws IOException {
        initialTable();
        initialIndex();
    }

    /**
     * 新增的两个方法之一，用于获取当前所有的表的信息
     * @return
     */
    public static LinkedHashMap<String, Table> getTables() {
        return tables;
    }

    /**
     * 新增的两个方法之一，用于获取当前所有的索引的信息
     * @return
     */
    public static LinkedHashMap<String, Index> getIndex() {
        return indexes;
    }

    private static void initialTable() throws IOException {
        File file = new File(tableFilename);
        if (!file.exists()) {
            System.out.println("文件不存在！");
            return;
        }
        FileInputStream fis = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fis);
        String tmpTableName, tmpPrimaryKey;
        int tmpIndexNum, tmpAttributeNum, tmpRowNum;

        while (dis.available() > 0) {
            Vector<Attribute> tmpAttributeVector = new Vector<Attribute>();
            Vector<Index> tmpIndexVector = new Vector<Index>();
            tmpTableName = dis.readUTF();
            tmpPrimaryKey = dis.readUTF();
            tmpRowNum = dis.readInt();
            tmpIndexNum = dis.readInt();
            for (int i = 0; i < tmpIndexNum; i++) {
                String tmpIndexName, tmpAttributeName;
                tmpIndexName = dis.readUTF();
                tmpAttributeName = dis.readUTF();
                tmpIndexVector.addElement(new Index(tmpIndexName, tmpTableName, tmpAttributeName));
            }
            tmpAttributeNum = dis.readInt();
            for (int i = 0; i < tmpAttributeNum; i++) {
                String tmpAttributeName, tmpType;
                NumType tmpNumType;
                int tmpLength;
                boolean tmpIsUnique;
                tmpAttributeName = dis.readUTF();
                tmpType = dis.readUTF();
                tmpLength = dis.readInt();
                tmpIsUnique = dis.readBoolean();
                tmpNumType = NumType.valueOf(tmpType);
                tmpAttributeVector.addElement(new Attribute(tmpAttributeName, tmpNumType, tmpLength, tmpIsUnique));
            }
            tables.put(tmpTableName, new Table(tmpTableName, tmpPrimaryKey, tmpAttributeVector, tmpIndexVector, tmpRowNum));
        }
        dis.close();
    }

    private static void initialIndex() throws IOException {
        File file = new File(indexFilename);
        if (!file.exists()) return;
        FileInputStream fis = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fis);
        String tmpIndexName, tmpTableName, tmpAttributeName;
        int tmpBlockNum, tmpRootNum;
        while (dis.available() > 0) {
            tmpIndexName = dis.readUTF();
            tmpTableName = dis.readUTF();
            tmpAttributeName = dis.readUTF();
            tmpBlockNum = dis.readInt();
            tmpRootNum = dis.readInt();
            indexes.put(tmpIndexName, new Index(tmpIndexName, tmpTableName, tmpAttributeName, tmpBlockNum, tmpRootNum));
        }
        dis.close();
    }

    public static void storeCatalog() throws IOException {
        storeTable();
        storeIndex();
    }

    private static void storeTable() throws IOException {
        File file = new File(tableFilename);
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        Table tmpTable;
        Iterator<Map.Entry<String, Table>> iter = tables.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpTable = (Table) entry.getValue();
            dos.writeUTF(tmpTable.tableName);
            dos.writeUTF(tmpTable.primaryKey);
            dos.writeInt(tmpTable.rowNum);
            dos.writeInt(tmpTable.indexNum);
            for (int i = 0; i < tmpTable.indexNum; i++) {
                Index tmpIndex = tmpTable.indexVector.get(i);
                dos.writeUTF(tmpIndex.indexName);
                dos.writeUTF(tmpIndex.attributeName);
            }
            dos.writeInt(tmpTable.attributeNum);
            for (int i = 0; i < tmpTable.attributeNum; i++) {
                Attribute tmpAttribute = tmpTable.attributeVector.get(i);
                dos.writeUTF(tmpAttribute.attributeName);
                dos.writeUTF(tmpAttribute.type.get_type().name());
                dos.writeInt(tmpAttribute.type.getLength());
                dos.writeBoolean(tmpAttribute.isUnique);
            }
        }
        dos.close();
    }

    private static void storeIndex() throws IOException {
        File file = new File(indexFilename);
        if (file.exists()) file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        Index tmpIndex;
        //Enumeration<Index> en = indexes.elements();
        Iterator<Map.Entry<String, Index>> iter = indexes.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpIndex = (Index) entry.getValue();
            //tmpIndex = en.nextElement();
            dos.writeUTF(tmpIndex.indexName);
            dos.writeUTF(tmpIndex.tableName);
            dos.writeUTF(tmpIndex.attributeName);
            dos.writeInt(tmpIndex.blockNum);
            dos.writeInt(tmpIndex.rootNum);
        }
        dos.close();
    }

    public static void showCatalog() {
        showTable();
        System.out.println();
        showIndex();
    }

    public static void showIndex() {
        Index tmpIndex;
        Iterator<Map.Entry<String, Index>> iter = indexes.entrySet().iterator();
        int idx = 5, tab = 5, attr = 9;
        //System.out.println("There are " + indexes.size() + " indexes in the database: ");
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpIndex = (Index) entry.getValue();
            idx = tmpIndex.indexName.length() > idx ? tmpIndex.indexName.length() : idx;
            tab = tmpIndex.tableName.length() > tab ? tmpIndex.tableName.length() : tab;
            attr = tmpIndex.attributeName.length() > attr ? tmpIndex.attributeName.length() : attr;
        }
        String format = "|%-" + idx + "s|%-" + tab + "s|%-" + attr + "s|\n";
        iter = indexes.entrySet().iterator();
        System.out.printf(format, "INDEX", "TABLE", "ATTRIBUTE");
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpIndex = (Index) entry.getValue();
            System.out.printf(format, tmpIndex.indexName, tmpIndex.tableName, tmpIndex.attributeName);
        }

    }

    public static int getMaxAttrLength(Table tab) {
        int len = 9;//the size of "ATTRIBUTE"
        for (int i = 0; i < tab.attributeVector.size(); i++) {
            int v = tab.attributeVector.get(i).attributeName.length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static void showTable() {
        Table tmpTable;
        Attribute tmpAttribute;
        Iterator<Map.Entry<String, Table>> iter = tables.entrySet().iterator();
        //System.out.println("There are " + tables.size() + " tables in the database: ");
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpTable = (Table) entry.getValue();
            System.out.println("[TABLE] " + tmpTable.tableName);
            String format = "|%-" + getMaxAttrLength(tmpTable) + "s";
            format += "|%-5s|%-6s|%-6s|\n";
            System.out.printf(format, "ATTRIBUTE", "TYPE", "LENGTH", "UNIQUE");
            for (int i = 0; i < tmpTable.attributeNum; i++) {
                tmpAttribute = tmpTable.attributeVector.get(i);
                System.out.printf(format, tmpAttribute.attributeName, tmpAttribute.type.get_type(), tmpAttribute.type.getLength(), tmpAttribute.isUnique);
            }
            if (iter.hasNext()) System.out.println("--------------------------------");
        }
    }

    public static Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public static Index getIndex(String indexName) {
        return indexes.get(indexName);
    }

    public static String getPrimaryKey(String tableName) {
        return getTable(tableName).primaryKey;
    }

    public static int getRowLength(String tableName) {
        return getTable(tableName).rowLength;
    }

    public static int getAttributeNum(String tableName) {
        return getTable(tableName).attributeNum;
    }

    public static int getRowNum(String tableName) {
        return getTable(tableName).rowNum;
    }

    //check
    public static boolean isPrimaryKey(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = getTable(tableName);
            return tmpTable.primaryKey.equals(attributeName);
        } else {
            System.out.println("The table " + tableName + " doesn't exist");
            return false;
        }
    }

    public static boolean isUnique(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = getTable(tableName);
            for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
                Attribute tmpAttribute = tmpTable.attributeVector.get(i);
                if (tmpAttribute.attributeName.equals(attributeName)) {
                    return tmpAttribute.isUnique;
                }
            }
            //if (i >= tmpTable.attributeVector.size()) {
            System.out.println("The attribute " + attributeName + " doesn't exist");
            return false;
            //}
        }
        System.out.println("The table " + tableName + " doesn't exist");
        return false;

    }

    public static boolean isIndexKey(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = getTable(tableName);
            if (isAttributeExist(tableName, attributeName)) {
                for (int i = 0; i < tmpTable.indexVector.size(); i++) {
                    if (tmpTable.indexVector.get(i).attributeName.equals(attributeName))
                        return true;
                }
            } else {
                System.out.println("The attribute " + attributeName + " doesn't exist");
            }
        } else
            System.out.println("The table " + tableName + " doesn't exist");
        return false;
    }

    private static boolean isIndexExist(String indexName) {
        return indexes.containsKey(indexName);
    }

    private static boolean isAttributeExist(String tableName, String attributeName) {
        Table tmpTable = getTable(tableName);
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            if (tmpTable.attributeVector.get(i).attributeName.equals(attributeName))
                return true;
        }
        return false;
    }

    public static String getIndexName(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = getTable(tableName);
            if (isAttributeExist(tableName, attributeName)) {
                for (int i = 0; i < tmpTable.indexVector.size(); i++) {
                    if (tmpTable.indexVector.get(i).attributeName.equals(attributeName))
                        return tmpTable.indexVector.get(i).indexName;
                }
            } else {
                System.out.println("The attribute " + attributeName + " doesn't exist");
            }
        } else
            System.out.println("The table " + tableName + " doesn't exist");
        return null;
    }

    public static String getAttributeName(String tableName, int i) {
        return tables.get(tableName).attributeVector.get(i).attributeName;
    }

    public static int getAttributeIndex(String tableName, String attributeName) {
        Table tmpTable = tables.get(tableName);
        Attribute tmpAttribute;
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            tmpAttribute = tmpTable.attributeVector.get(i);
            if (tmpAttribute.attributeName.equals(attributeName))
                return i;
        }
        System.out.println("The attribute " + attributeName + " doesn't exist");
        return -1;
    }

    public static FieldType getAttributeType(String tableName, String attributeName) {
        Table tmpTable = tables.get(tableName);
        Attribute tmpAttribute;
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            tmpAttribute = tmpTable.attributeVector.get(i);
            if (tmpAttribute.attributeName.equals(attributeName))
                return tmpAttribute.type;
        }
        System.out.println("The attribute " + attributeName + " doesn't exist");
        return null;
    }

    public static int getLength(String tableName, String attributeName) {
        Table tmpTable = tables.get(tableName);
        Attribute tmpAttribute;
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            tmpAttribute = tmpTable.attributeVector.get(i);
            if (tmpAttribute.attributeName.equals(attributeName))
                return tmpAttribute.type.getLength();
        }
        System.out.println("The attribute " + attributeName + " doesn't exist");
        return -1;
    }

    public static String getType(String tableName, int i) {
        //Table tmpTable=tables.get(tableName);
        return tables.get(tableName).attributeVector.get(i).type.get_type().name();
    }

    public static int getLength(String tableName, int i) {
        //table tmpTable=tables.get(tableName);
        return tables.get(tableName).attributeVector.get(i).type.getLength();
    }

    public static void addRowNum(String tableName) {
        tables.get(tableName).rowNum++;
    }

    public static void deleteRowNum(String tableName, int num) {
        tables.get(tableName).rowNum -= num;
    }

    public static boolean updateIndexTable(String indexName, Index tmpIndex) {
        indexes.replace(indexName, tmpIndex);
        return true;
    }

    public static boolean isAttributeExist(Vector<Attribute> attributeVector, String attributeName) {
        for (int i = 0; i < attributeVector.size(); i++) {
            if (attributeVector.get(i).attributeName.equals(attributeName))
                return true;
        }
        return false;
    }

    //Interface
    public static boolean createTable(Table newTable) throws NullPointerException{
        tables.put(newTable.tableName, newTable);
        //indexes.put(newTable.indexes.firstElement().indexName, newTable.indexes.firstElement());
        return true;
    }

    public static boolean dropTable(String tableName) throws NullPointerException{
        Table tmpTable = tables.get(tableName);
        for (int i = 0; i < tmpTable.indexVector.size(); i++) {
            indexes.remove(tmpTable.indexVector.get(i).indexName);
        }
        tables.remove(tableName);
        return true;
    }

    public static boolean createIndex(Index newIndex) throws NullPointerException{
        Table tmpTable = getTable(newIndex.tableName);
        tmpTable.indexVector.addElement(newIndex);
        tmpTable.indexNum = tmpTable.indexVector.size();
        indexes.put(newIndex.indexName, newIndex);
        return true;
    }

    public static boolean dropIndex(String indexName) throws NullPointerException{
        Index tmpIndex = getIndex(indexName);
        Table tmpTable = getTable(tmpIndex.tableName);
        tmpTable.indexVector.remove(tmpIndex);
        tmpTable.indexNum = tmpTable.indexVector.size();
        indexes.remove(indexName);
        return true;
    }

}
