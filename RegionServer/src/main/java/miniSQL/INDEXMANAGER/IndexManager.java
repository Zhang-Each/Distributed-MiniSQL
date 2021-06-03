package miniSQL.INDEXMANAGER;

import miniSQL.CATALOGMANAGER.*;
import miniSQL.BUFFERMANAGER.*;
import miniSQL.RECORDMANAGER.*;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Vector;


public class IndexManager {

    private static LinkedHashMap<String, BPTree<Integer, Address>> intTreeMap = new LinkedHashMap<>();
    private static LinkedHashMap<String, BPTree<String, Address>> charTreeMap = new LinkedHashMap<>();
    private static LinkedHashMap<String, BPTree<Float, Address>> floatTreeMap = new LinkedHashMap<>();

    public IndexManager() {
        //Nothing
    }

    public static Vector<Address> select(Index idx, Condition cond) throws IllegalArgumentException {
        String tableName = idx.tableName;
        String attributeName = idx.attributeName;
        int index = CatalogManager.getAttributeIndex(tableName, attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(tableName, index));

        BPTree<Integer, Address> intTree;
        BPTree<String, Address> charTree;
        BPTree<Float, Address> floatTree;

        switch (type) {
            case INT:
                intTree = intTreeMap.get(idx.indexName);
                return IndexManager.<Integer>satisfiesCond(intTree, cond.getOperator(), Integer.parseInt(cond.getValue()));
            case FLOAT:
                floatTree = floatTreeMap.get(idx.indexName);
                return IndexManager.<Float>satisfiesCond(floatTree, cond.getOperator(), Float.parseFloat(cond.getValue()));
            case CHAR:
                charTree = charTreeMap.get(idx.indexName);
                return IndexManager.<String>satisfiesCond(charTree, cond.getOperator(), cond.getValue());
        }
        return null;
    }

    public static void delete(Index idx, String key) throws IllegalArgumentException {
        String tableName = idx.tableName;
        String attributeName = idx.attributeName;
        int index = CatalogManager.getAttributeIndex(tableName, attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(tableName, index));

        BPTree<Integer, Address> intTree;
        BPTree<String, Address> charTree;
        BPTree<Float, Address> floatTree;

        switch(type) {
        case INT:
            intTree = intTreeMap.get(idx.indexName);
            intTree.delete(Integer.parseInt(key));
            break;
        case FLOAT:
            floatTree = floatTreeMap.get(idx.indexName);
            floatTree.delete(Float.parseFloat(key));
            break;
        case CHAR:
            charTree = charTreeMap.get(idx.indexName);
            charTree.delete(key);
            break;
        }
    }

    public static void insert(Index idx, String key, Address value) throws IllegalArgumentException {
        String tableName = idx.tableName;
        String attributeName = idx.attributeName;
        int index = CatalogManager.getAttributeIndex(tableName, attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(tableName, index));

        BPTree<Integer, Address> intTree;
        BPTree<String, Address> charTree;
        BPTree<Float, Address> floatTree;

        switch(type) {
        case INT:
            intTree = intTreeMap.get(idx.indexName);
            intTree.insert(Integer.parseInt(key), value);
            break;
        case FLOAT:
            floatTree = floatTreeMap.get(idx.indexName);
            floatTree.insert(Float.parseFloat(key), value);
            break;
        case CHAR:
            charTree = charTreeMap.get(idx.indexName);
            charTree.insert(key, value);
            break;
        }
    }

    public static void update(Index idx, String key, Address value) throws IllegalArgumentException {
        String tableName = idx.tableName;
        String attributeName = idx.attributeName;
        int index = CatalogManager.getAttributeIndex(tableName, attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(tableName, index));

        BPTree<Integer, Address> intTree;
        BPTree<String, Address> charTree;
        BPTree<Float, Address> floatTree;

        switch(type) {
        case INT:
            intTree = intTreeMap.get(idx.indexName);
            intTree.update(Integer.parseInt(key), value);
            break;
        case FLOAT:
            floatTree = floatTreeMap.get(idx.indexName);
            floatTree.update(Float.parseFloat(key), value);
            break;
        case CHAR:
            charTree = charTreeMap.get(idx.indexName);
            charTree.update(key, value);
            break;
        }
    }

    public static void initialIndex() throws IOException {
        String fileName = "index_catalog";
        File file = new File(fileName);
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
            createIndex(new Index(tmpIndexName, tmpTableName, tmpAttributeName, tmpBlockNum, tmpRootNum));
        }
        dis.close();
    }

    public static boolean createIndex(Index idx) throws IOException, IllegalArgumentException, RuntimeException {
        String fileName = idx.indexName + ".index";
        buildIndex(idx);
        //把idx的信息写入到硬盘中
        File file = new File(fileName);
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeUTF(idx.indexName);
        dos.writeUTF(idx.tableName);
        dos.writeUTF(idx.attributeName);
        dos.writeInt(idx.blockNum);
        dos.writeInt(idx.rootNum);
        dos.close();
        return true; //文件读写失败返回false
    }

    public static boolean dropIndex(Index idx) {
        String filename = idx.indexName + ".index";
        File file = new File(filename);
        if (file.exists()) file.delete();
        int index = CatalogManager.getAttributeIndex(idx.tableName, idx.attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(idx.tableName, index));
        switch (type) {
            case INT:
                intTreeMap.remove(idx.indexName);
                break;
            case CHAR:
                charTreeMap.remove(idx.indexName);
                break;
            case FLOAT:
                floatTreeMap.remove(idx.indexName);
                break;
        }
        return true;
    }

    private static void buildIndex(Index idx) throws IllegalArgumentException, RuntimeException {
        String tableName = idx.tableName;
        String attributeName = idx.attributeName;
        int tupleNum = CatalogManager.getRowNum(tableName);
        int storeLen = IndexManager.getStoreLength(tableName);
        int byteOffset = FieldType.INTSIZE;
        int blockOffset = 0;
        int processNum = 0;
        int index = CatalogManager.getAttributeIndex(tableName, attributeName);
        NumType type = NumType.valueOf(CatalogManager.getType(tableName, index));

        Block block = BufferManager.readBlockFromDiskQuote(tableName, 0);

        BPTree<Integer, Address> intTree = new BPTree<>(4);
        BPTree<String, Address> charTree = new BPTree<>(4);
        BPTree<Float, Address> floatTree = new BPTree<>(4);

        switch (type) {
            case INT:
                while (processNum < tupleNum) {
                    if (byteOffset + storeLen >= Block.BLOCKSIZE) { //find next block
                        blockOffset++;
                        byteOffset = 0; //reset byte offset
                        block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read next block
                        if (block == null) { //can't get from buffer
                            throw new RuntimeException();
                        }
                    }
                    if (block.readInteger(byteOffset) < 0) { //tuple is valid
                        Address value = new Address(tableName, blockOffset, byteOffset);
                        TableRow row = IndexManager.getTuple(tableName, block, byteOffset);
                        Integer key = Integer.parseInt(row.getAttributeValue(index));
                        intTree.insert(key, value);
                        processNum++; //update processed tuple number
                    }
                    byteOffset += storeLen; //update byte offset
                }
                intTreeMap.put(idx.indexName, intTree);
                break;
            case CHAR:
                while (processNum < tupleNum) {
                    if (byteOffset + storeLen >= Block.BLOCKSIZE) { //find next block
                        blockOffset++;
                        byteOffset = 0; //reset byte offset
                        block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read next block
                        if (block == null) { //can't get from buffer
                            throw new RuntimeException();
                        }
                    }
                    if (block.readInteger(byteOffset) < 0) { //tuple is valid
                        Address value = new Address(tableName, blockOffset, byteOffset);
                        TableRow row = IndexManager.getTuple(tableName, block, byteOffset);
                        String key = row.getAttributeValue(index);
                        charTree.insert(key, value);
                        processNum++; //update processed tuple number
                    }
                    byteOffset += storeLen; //update byte offset
                }
                charTreeMap.put(idx.indexName, charTree);
                break;
            case FLOAT:
                while (processNum < tupleNum) {
                    if (byteOffset + storeLen >= Block.BLOCKSIZE) { //find next block
                        blockOffset++;
                        byteOffset = 0; //reset byte offset
                        block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read next block
                        if (block == null) { //can't get from buffer
                            throw new RuntimeException();
                        }
                    }
                    if (block.readInteger(byteOffset) < 0) { //tuple is valid
                        Address value = new Address(tableName, blockOffset, byteOffset);
                        TableRow row = IndexManager.getTuple(tableName, block, byteOffset);
                        Float key = Float.parseFloat(row.getAttributeValue(index));
                        floatTree.insert(key, value);
                        processNum++; //update processed tuple number
                    }
                    byteOffset += storeLen; //update byte offset
                }
                floatTreeMap.put(idx.indexName, floatTree);
                break;
        }
    }
    
    //returns a vector of addresses which satisfy the condition
    private static <K extends Comparable<? super K>> Vector<Address> satisfiesCond(BPTree<K, Address> tree, String operator, K key) throws IllegalArgumentException {
        if (operator.equals("=")) {
            return tree.findEq(key);
        } else if (operator.equals("<>")) {
            return tree.findNeq(key);
        } else if (operator.equals(">")) {
            return tree.findGreater(key);
        } else if (operator.equals("<")) {
            return tree.findLess(key);
        } else if (operator.equals(">=")) {
            return tree.findGeq(key);
        } else if (operator.equals("<=")) {
            return tree.findLeq(key);
        } else { //undefined operator
            throw new IllegalArgumentException();
        }
    }

    //get the length for one tuple to store in given table
    private static int getStoreLength(String tableName) {
        int rowLen = CatalogManager.getRowLength(tableName); //actual length
        if (rowLen > FieldType.INTSIZE) { //add a valid byte in head
            return rowLen + FieldType.CHARSIZE;
        } else { //empty address pointer + valid byte
            return FieldType.INTSIZE + FieldType.CHARSIZE;
        }
    }

    //get the tuple from given table according to stored block and start byte offset
    private static TableRow getTuple(String tableName, Block block, int offset) {
        int attributeNum = CatalogManager.getAttributeNum(tableName); //number of attribute
        String attributeValue = null;
        TableRow result = new TableRow();

        offset++; //skip first valid flag

        for (int i = 0; i < attributeNum; i++) { //for each attribute
            int length = CatalogManager.getLength(tableName, i); //get length
            String type = CatalogManager.getType(tableName, i); //get type
            if (type.equals("CHAR")) { //char type
                int first;
                attributeValue = block.readString(offset, length);
                first = attributeValue.indexOf(0);
                first = first == -1 ? attributeValue.length() : first;
                attributeValue = attributeValue.substring(0, first); //filter '\0'
            } else if (type.equals("INT")) { //integer type
                attributeValue = String.valueOf(block.readInteger(offset));
            } else if (type.equals("FLOAT")) { //float type
                attributeValue = String.valueOf(block.readFloat(offset));
            }
            offset += length;
            result.addAttributeValue(attributeValue); //add attribute to row
        }
        return result;
    }

}
