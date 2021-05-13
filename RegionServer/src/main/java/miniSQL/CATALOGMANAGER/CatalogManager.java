package miniSQL.CATALOGMANAGER;

import miniSQL.INDEXMANAGER.Index;

import java.io.*;
import java.util.*;

public class CatalogManager {

    private static LinkedHashMap<String, Table> tables = new LinkedHashMap<>();
    private static LinkedHashMap<String, Index> indexes = new LinkedHashMap<>();
    private static String tableFilename = "table_catalog";
    private static String indexFilename = "index_catalog";

    public static void initial_catalog() throws IOException {
        initial_table();
        initial_index();
    }

    private static void initial_table() throws IOException {
        File file = new File(tableFilename);
        if (!file.exists()) return;
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

    private static void initial_index() throws IOException {
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

    public static void store_catalog() throws IOException {
        store_table();
        store_index();
    }

    private static void store_table() throws IOException {
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
                dos.writeInt(tmpAttribute.type.get_length());
                dos.writeBoolean(tmpAttribute.isUnique);
            }
        }
        dos.close();
    }

    private static void store_index() throws IOException {
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

    public static void show_catalog() {
        show_table();
        System.out.println();
        show_index();
    }

    public static void show_index() {
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

    public static int get_max_attr_length(Table tab) {
        int len = 9;//the size of "ATTRIBUTE"
        for (int i = 0; i < tab.attributeVector.size(); i++) {
            int v = tab.attributeVector.get(i).attributeName.length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static void show_table() {
        Table tmpTable;
        Attribute tmpAttribute;
        Iterator<Map.Entry<String, Table>> iter = tables.entrySet().iterator();
        //System.out.println("There are " + tables.size() + " tables in the database: ");
        while (iter.hasNext()) {
            Map.Entry entry = iter.next();
            tmpTable = (Table) entry.getValue();
            System.out.println("[TABLE] " + tmpTable.tableName);
            String format = "|%-" + get_max_attr_length(tmpTable) + "s";
            format += "|%-5s|%-6s|%-6s|\n";
            System.out.printf(format, "ATTRIBUTE", "TYPE", "LENGTH", "UNIQUE");
            for (int i = 0; i < tmpTable.attributeNum; i++) {
                tmpAttribute = tmpTable.attributeVector.get(i);
                System.out.printf(format, tmpAttribute.attributeName, tmpAttribute.type.get_type(), tmpAttribute.type.get_length(), tmpAttribute.isUnique);
            }
            if (iter.hasNext()) System.out.println("--------------------------------");
        }
    }

    public static Table get_table(String tableName) {
        return tables.get(tableName);
    }

    public static Index get_index(String indexName) {
        return indexes.get(indexName);
    }

    public static String get_primary_key(String tableName) {
        return get_table(tableName).primaryKey;
    }

    public static int get_row_length(String tableName) {
        return get_table(tableName).rowLength;
    }

    public static int get_attribute_num(String tableName) {
        return get_table(tableName).attributeNum;
    }

    public static int get_row_num(String tableName) {
        return get_table(tableName).rowNum;
    }

    //check
    public static boolean is_primary_key(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = get_table(tableName);
            return tmpTable.primaryKey.equals(attributeName);
        } else {
            System.out.println("The table " + tableName + " doesn't exist");
            return false;
        }
    }

    public static boolean is_unique(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = get_table(tableName);
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

    public static boolean is_index_key(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = get_table(tableName);
            if (is_attribute_exist(tableName, attributeName)) {
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

    private static boolean is_index_exist(String indexName) {
        return indexes.containsKey(indexName);
    }

    private static boolean is_attribute_exist(String tableName, String attributeName) {
        Table tmpTable = get_table(tableName);
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            if (tmpTable.attributeVector.get(i).attributeName.equals(attributeName))
                return true;
        }
        return false;
    }

    public static String get_index_name(String tableName, String attributeName) {
        if (tables.containsKey(tableName)) {
            Table tmpTable = get_table(tableName);
            if (is_attribute_exist(tableName, attributeName)) {
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

    public static String get_attribute_name(String tableName, int i) {
        return tables.get(tableName).attributeVector.get(i).attributeName;
    }

    public static int get_attribute_index(String tableName, String attributeName) {
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

    public static FieldType get_attribute_type(String tableName, String attributeName) {
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

    public static int get_length(String tableName, String attributeName) {
        Table tmpTable = tables.get(tableName);
        Attribute tmpAttribute;
        for (int i = 0; i < tmpTable.attributeVector.size(); i++) {
            tmpAttribute = tmpTable.attributeVector.get(i);
            if (tmpAttribute.attributeName.equals(attributeName))
                return tmpAttribute.type.get_length();
        }
        System.out.println("The attribute " + attributeName + " doesn't exist");
        return -1;
    }

    public static String get_type(String tableName, int i) {
        //Table tmpTable=tables.get(tableName);
        return tables.get(tableName).attributeVector.get(i).type.get_type().name();
    }

    public static int get_length(String tableName, int i) {
        //table tmpTable=tables.get(tableName);
        return tables.get(tableName).attributeVector.get(i).type.get_length();
    }

    public static void add_row_num(String tableName) {
        tables.get(tableName).rowNum++;
    }

    public static void delete_row_num(String tableName, int num) {
        tables.get(tableName).rowNum -= num;
    }

    public static boolean update_index_table(String indexName, Index tmpIndex) {
        indexes.replace(indexName, tmpIndex);
        return true;
    }

    public static boolean is_attribute_exist(Vector<Attribute> attributeVector, String attributeName) {
        for (int i = 0; i < attributeVector.size(); i++) {
            if (attributeVector.get(i).attributeName.equals(attributeName))
                return true;
        }
        return false;
    }

    //Interface
    public static boolean create_table(Table newTable) throws NullPointerException{
        tables.put(newTable.tableName, newTable);
        //indexes.put(newTable.indexes.firstElement().indexName, newTable.indexes.firstElement());
        return true;
    }

    public static boolean drop_table(String tableName) throws NullPointerException{
        Table tmpTable = tables.get(tableName);
        for (int i = 0; i < tmpTable.indexVector.size(); i++) {
            indexes.remove(tmpTable.indexVector.get(i).indexName);
        }
        tables.remove(tableName);
        return true;
    }

    public static boolean create_index(Index newIndex) throws NullPointerException{
        Table tmpTable = get_table(newIndex.tableName);
        tmpTable.indexVector.addElement(newIndex);
        tmpTable.indexNum = tmpTable.indexVector.size();
        indexes.put(newIndex.indexName, newIndex);
        return true;
    }

    public static boolean drop_index(String indexName) throws NullPointerException{
        Index tmpIndex = get_index(indexName);
        Table tmpTable = get_table(tmpIndex.tableName);
        tmpTable.indexVector.remove(tmpIndex);
        tmpTable.indexNum = tmpTable.indexVector.size();
        indexes.remove(indexName);
        return true;
    }

}
