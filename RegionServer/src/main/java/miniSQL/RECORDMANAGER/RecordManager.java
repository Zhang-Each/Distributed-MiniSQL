package miniSQL.RECORDMANAGER;

import miniSQL.BUFFERMANAGER.Block;
import miniSQL.BUFFERMANAGER.BufferManager;
import miniSQL.CATALOGMANAGER.Address;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.FieldType;
import miniSQL.INDEXMANAGER.Index;
import miniSQL.INDEXMANAGER.IndexManager;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

public class RecordManager {

    //create a file for new table, return true if success, otherwise return false
    public static boolean createTable(String tableName) throws Exception{
        File file =new File(tableName);
        if (!file.createNewFile()) //file already exists
            throw new NullPointerException();
        Block block = BufferManager.readBlockFromDiskQuote(tableName, 0); //read first block from file
        if(block == null) { //can't get from buffer
            throw new NullPointerException();
        } else {
            block.writeInteger(0, -1); //write to free list head, -1 means no free space
            return true;
        }
    }

    //delete the file of given table, return true if success, otherwise return false
    public static boolean dropTable(String tableName) throws Exception {
        File file =new File(tableName);
        if(file.delete()) { //delete the file
            BufferManager.makeInvalid(tableName); // set the block invalid
            return true;
        } else {
            throw new NullPointerException();
        }
    }

    //select tuples from given table according to conditions, return result tuples
    public static Vector<TableRow> select(String tableName, Vector<Condition> conditions) throws Exception{
        int tupleNum = CatalogManager.getRowNum(tableName);
        int storeLen = getStoreLength(tableName);

        int processNum = 0; //number of processed tuples
        int byteOffset = FieldType.INTSIZE; //byte offset in block, skip file header
        int blockOffset = 0; //block offset in file
        Vector<TableRow> result = new Vector<>(); //table row result

        Block block = BufferManager.readBlockFromDiskQuote(tableName, 0); //get first block
        if(block == null)  //can't get from buffer
            throw new NullPointerException();
        if(!checkCondition(tableName, conditions))  //check condition
            return result;

        while(processNum < tupleNum) { //scan the block in sequence
            if (byteOffset + storeLen >= Block.BLOCKSIZE) { //find next block
                blockOffset++;
                byteOffset = 0; //reset byte offset
                block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read next block
                if(block == null) { //can't get from buffer
                    return result;
                }
            }
            if(block.readInteger(byteOffset) < 0) { //tuple is valid
                int i;
                TableRow newRow = getTuple(tableName, block, byteOffset);
                for(i = 0;i < conditions.size();i++) { //check all conditions
                    if(!conditions.get(i).satisfy(tableName, newRow))
                        break;
                }
                if(i == conditions.size()) { //if satisfy all conditions
                    result.add(newRow); //add new row to result
                }
                processNum++; //update processed tuple number
            }
            byteOffset += storeLen; //update byte offset
        }
        return result;
    }

    //insert the tuple in given table, return the inserted address
    public static Address insert(String tableName, TableRow data) throws Exception{
        int tupleNum = CatalogManager.getRowNum(tableName);
        Block headBlock = BufferManager.readBlockFromDiskQuote(tableName, 0); //get first block

        if(headBlock == null) //can't get from buffer
            throw new NullPointerException();
        if(!checkRow(tableName, data)) // illegal
            return null;

        headBlock.lock(true); //lock first block for later write

        int freeOffset = headBlock.readInteger(0); //read the first free offset in file header
        int tupleOffset;

        if(freeOffset < 0) { //no free space
            tupleOffset = tupleNum; //add to tail of the file
        } else {
            tupleOffset = freeOffset; //add to free offset
        }

        int blockOffset = getBlockOffset(tableName, tupleOffset); //block offset of tuple
        int byteOffset = getByteOffset(tableName, tupleOffset); //byte offset of tuple
        Block insertBlock = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read the block for inserting

        if(insertBlock == null) { //can't get from buffer
            headBlock.lock(false);
            return null;
        }

        if(freeOffset >= 0) { //if head has free offset, update it
            freeOffset = insertBlock.readInteger(byteOffset + 1); //get next free address
            headBlock.writeInteger(0, freeOffset); //write new free offset to head
        }

        headBlock.lock(false); //unlock head block
        writeTuple(tableName, data, insertBlock, byteOffset); //write data to insert block
        return new Address(tableName, blockOffset, byteOffset); //return insert address
    }

    //delete the condition-satisfied tuples from given table, return number of deleted tuples
    public static int delete(String tableName, Vector<Condition> conditions) throws Exception{
        int tupleNum = CatalogManager.getRowNum(tableName);
        int storeLen = getStoreLength(tableName);

        int processNum = 0; //number of processed tuples
        int byteOffset = FieldType.INTSIZE; //byte offset in block, skip file header
        int blockOffset = 0; //block offset in file
        int deleteNum = 0; // number of delete tuples

        Block headBlock = BufferManager.readBlockFromDiskQuote(tableName, 0); //get first block
        Block laterBlock = headBlock; //block for sequently scanning

        if(headBlock == null)  //can't get from buffer
            throw new NullPointerException();
        if(!checkCondition(tableName, conditions))  //check condition
            return 0;

        headBlock.lock(true); //lock head block for free list update

        for(int currentNum = 0;processNum < tupleNum; currentNum++) { //scan the block in sequence
            if (byteOffset + storeLen >= Block.BLOCKSIZE) { //byte overflow, find next block
                blockOffset++;
                byteOffset = 0; //reset byte offset
                laterBlock = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); //read next block
                if(laterBlock == null) { //can't get from buffer
                    headBlock.lock(false);
                    return deleteNum;
                }
            }
            if(laterBlock.readInteger(byteOffset) < 0) { //tuple is valid
                int i;
                TableRow newRow = getTuple(tableName, laterBlock, byteOffset); //get current tuple
                for(i = 0;i < conditions.size();i++) { //check all conditions
                    if(!conditions.get(i).satisfy(tableName, newRow))
                        break;
                }
                if(i == conditions.size()) { //if satisfy all conditions, delete the tuple
                    laterBlock.writeInteger(byteOffset, 0); //set vaild byte to 0
                    laterBlock.writeInteger(byteOffset + 1, headBlock.readInteger(0)); //set free offset
                    headBlock.writeInteger(0, currentNum); //write deleted offset to head pointer
                    deleteNum++;
                    for(int j = 0; j < newRow.getAttributeSize(); j++) { //delete index
                        String attrName = CatalogManager.getAttributeName(tableName, j);
                        if (CatalogManager.isIndexKey(tableName, attrName)) {
                            String indexName = CatalogManager.getIndexName(tableName, attrName);
                            Index index = CatalogManager.getIndex(indexName);
                            IndexManager.delete(index, newRow.getAttributeValue(j));
                        }
                    }
                }
                processNum++; //update processed tuple number
            }
            byteOffset += storeLen; //update byte offset
        }

        headBlock.lock(false);
        return deleteNum;
    }

    //select the tuple from given list of address on one table with conditions, return result list of tuples
    public static Vector<TableRow> select(Vector<Address> address, Vector<Condition> conditions) throws Exception{
        if(address.size() == 0) //empty address
            return new Vector<>();
        Collections.sort(address); //sort address
        String tableName = address.get(0).getFileName(); //get table name
        int blockOffset = 0, blockOffsetPre = -1; //current and previous block offset
        int byteOffset = 0; //current byte offset

        Block block = null;
        Vector<TableRow> result = new Vector<>();

        if(!checkCondition(tableName, conditions))  //check condition
            return result;

        for(int i = 0;i < address.size(); i++) { //for each later address
            blockOffset = address.get(i).getBlockOffset(); //read block and byte offset
            byteOffset = address.get(i).getByteOffset();
            if (i == 0 || blockOffset != blockOffsetPre) { //not in same block as previous
                block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); // read a new block
                if(block == null) {
                    if (i == 0)
                        throw new NullPointerException();
                }
            }
            if (block.readInteger(byteOffset) < 0) { //tuple is valid
                int j;
                TableRow newRow = getTuple(tableName, block, byteOffset);
                for(j = 0;j < conditions.size();j++) { //check all conditions
                    if(!conditions.get(j).satisfy(tableName,newRow))
                        break;
                }
                if(j == conditions.size()) { //all satisfy
                    result.add(newRow); //add tuple
                }
            }
            blockOffsetPre = blockOffset;
        }
        return result;
    }

    //delete the tuples from given address on one table with conditions, return the number of delete tuples
    public static int delete(Vector<Address> address, Vector<Condition> conditions) throws Exception {
        if(address.size() == 0) //empty address
            return 0;

        Collections.sort(address); //sort address
        String tableName = address.get(0).getFileName(); //get table name

        int blockOffset = 0,blockOffsetPre = -1; //current and previous block offset
        int byteOffset = 0; //current byte offset
        int tupleOffset = 0; //tuple offset in file

        Block headBlock = BufferManager.readBlockFromDiskQuote(tableName, 0); //get head block
        Block deleteBlock = null;

        if(headBlock == null)  //can't get from buffer
            throw new NullPointerException();
        if(!checkCondition(tableName, conditions))  //check condition
            return 0;

        headBlock.lock(true); //lock head block for free list update

        int deleteNum = 0; // number of delete tuple
        for(int i = 0;i < address.size();i++) { //for each address
            blockOffset = address.get(i).getBlockOffset(); //read block and byte offset
            byteOffset = address.get(i).getByteOffset();
            tupleOffset = getTupleOffset(tableName, blockOffset, byteOffset);

            if(i == 0 || blockOffset != blockOffsetPre) { //not in same block
                deleteBlock = BufferManager.readBlockFromDiskQuote(tableName, blockOffset); // read a new block
                if(deleteBlock == null) { //can't get from buffer
                    headBlock.lock(false);
                    return deleteNum;
                }
            }

            if (deleteBlock.readInteger(byteOffset) < 0) { //tuple is valid
                int j;
                TableRow newRow = getTuple(tableName, deleteBlock, byteOffset);
                for(j = 0;j < conditions.size();j++) { //check all conditions
                    if(!conditions.get(j).satisfy(tableName, newRow))
                        break;
                }
                if(j == conditions.size()) { //all satisfy
                    deleteBlock.writeInteger(byteOffset, 0); //set valid byte to 0
                    deleteBlock.writeInteger(byteOffset + 1, headBlock.readInteger(0)); //set free address
                    headBlock.writeInteger(0, tupleOffset); //write delete offset to head
                    deleteNum++;
                    for(int k = 0; k < newRow.getAttributeSize(); k++) { //delete index
                        String attrName = CatalogManager.getAttributeName(tableName, k);
                        if (CatalogManager.isIndexKey(tableName, attrName)) {
                            String indexName = CatalogManager.getIndexName(tableName, attrName);
                            Index index = CatalogManager.getIndex(indexName);
                            IndexManager.delete(index, newRow.getAttributeValue(k));
                        }
                    }
                }
            }
            blockOffsetPre = blockOffset;
        }
        headBlock.lock(false); //unlock head block
        return deleteNum;
    }

    //do projection on given result and projected attribute name in given table, return the projection result
    public static Vector<TableRow> project(String tableName, Vector<TableRow> result, Vector<String> projectName) throws Exception{
        int attributeNum = CatalogManager.getAttributeNum(tableName);
        Vector<TableRow> projectResult = new Vector<>();
        for(int i = 0;i < result.size();i++) { //for each tuple in result
            TableRow newRow = new TableRow();
            for(int j = 0;j < projectName.size();j++) { //for each project attribute name
                int index = CatalogManager.getAttributeIndex(tableName, projectName.get(j)); //get index
                if (index == -1) {
                    throw new IllegalArgumentException("Can't not find attribute " + projectName.get(j));
                } else {
                    newRow.addAttributeValue(result.get(i).getAttributeValue(index)); //set attribute to tuple
                }
            }
            projectResult.add(newRow);
        }

        return projectResult;
    }

    //store the record from buffer to file
    public static void storeRecord() {
        BufferManager.destructBufferManager();
    }

    //get the length for one tuple to store in given table
    private static int getStoreLength(String tableName) {
        int rowLen = CatalogManager.getRowLength(tableName); //actual length
        if(rowLen > FieldType.INTSIZE) { //add a valid byte in head
            return rowLen + FieldType.CHARSIZE;
        } else { //empty address pointer + valid byte
            return FieldType.INTSIZE + FieldType.CHARSIZE;
        }
    }

    //get the block offset of given table and tuple offset
    private static int getBlockOffset(String tableName, int tupleOffset) {
        int storeLen = getStoreLength(tableName);
        int tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) / storeLen; //number of tuples in first block
        int tupleInNext = Block.BLOCKSIZE / storeLen; //number of tuples in later block

        if(tupleOffset < tupleInFirst) { //in first block
            return 0;
        } else { //in later block
            return (tupleOffset - tupleInFirst) / tupleInNext + 1;
        }
    }

    //get the byte offset of given table and tuple offset
    private static int getByteOffset(String tableName, int tupleOffset) {
        int storeLen = getStoreLength(tableName);
        int tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) / storeLen; //number of tuples in first block
        int tupleInNext = Block.BLOCKSIZE / storeLen; //number of tuples in later block

        int blockOffset = getBlockOffset(tableName, tupleOffset);
        if(blockOffset == 0) { //in first block
            return tupleOffset * storeLen + FieldType.INTSIZE;
        } else { //in later block
            return (tupleOffset - tupleInFirst - (blockOffset - 1) * tupleInNext) * storeLen;
        }
    }

    //get the tuple offset of given table, block offset and byte offset
    private static int getTupleOffset(String tableName, int blockOffset, int byteOffset) {
        int storeLen = getStoreLength(tableName);
        int tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) / storeLen; //number of tuples in first block
        int tupleInNext = Block.BLOCKSIZE / storeLen; //number of tuples in later block

        if(blockOffset == 0) { //in first block
            return (byteOffset - FieldType.INTSIZE) / storeLen;
        } else { //in later block
            return tupleInFirst + (blockOffset - 1) * tupleInNext + byteOffset / storeLen;
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

    //write a tuple to given table according to stored block and start byte offset
    private static void writeTuple(String tableName, TableRow data, Block block, int offset) {
        int attributeNum = CatalogManager.getAttributeNum(tableName); //number of attribute

        block.writeInteger(offset,-1); //set valid byte to 11111111
        offset++; //skip first valid flag

        for (int i = 0; i < attributeNum; i++) { //for each attribute
            int length = CatalogManager.getLength(tableName, i); //get length
            String type = CatalogManager.getType(tableName, i); //get type
            if (type.equals("CHAR")) { //char type
                byte[] reset = new byte[length];
                Arrays.fill(reset, (byte) 0);
                block.writeData(offset, reset);
                block.writeString(offset,data.getAttributeValue(i));
            } else if (type.equals("INT")) { //integer type
                block.writeInteger(offset, Integer.parseInt(data.getAttributeValue(i)));
            } else if (type.equals("FLOAT")) { //float type
                block.writeFloat(offset, Float.parseFloat(data.getAttributeValue(i)));
            }
            offset += length;
        }
    }

    //check whether the tuple statisfy the table attribute definition
    private static boolean checkRow(String tableName, TableRow data) throws Exception{
        if (CatalogManager.getAttributeNum(tableName) != data.getAttributeSize())
            throw new IllegalArgumentException("Attribute number doesn't match");

        for (int i = 0; i < data.getAttributeSize(); i++) {
            String type = CatalogManager.getType(tableName, i);
            int length = CatalogManager.getLength(tableName, i);
            if (!checkType(type, length, data.getAttributeValue(i)))
                return false;
        }
        return true;
    }

    //check whether the condition statisfy the table attribute definition
    private static boolean checkCondition(String tableName, Vector<Condition> conditions) throws Exception{
        for(int i = 0;i <conditions.size();i++) {
            int index = CatalogManager.getAttributeIndex(tableName, conditions.get(i).getName());
            if(index == -1)
                throw new IllegalArgumentException("Can't not find attribute " + conditions.get(i).getName());
            String type = CatalogManager.getType(tableName, index);
            int length = CatalogManager.getLength(tableName ,index);
            if (!checkType(type, length, conditions.get(i).getValue()))
                return false;
        }
        return true;
    }

    //check whether the type correspond the attribute value
    private static boolean checkType(String type, int length, String value) throws Exception{
        switch (type) { //check type
            case "INT":
                try {
                    Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(value + " dosen't match int type or overflow");
                }
                break;
            case "FLOAT":
                try {
                    Float.parseFloat(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(value + " dosen't match float type or overflow");
                }
                break;
            case "CHAR":
                if(length < value.length())
                    throw new IllegalArgumentException("The char number " + value + " must be limited in " + length + " bytes");
                break;
            default:
                throw new IllegalArgumentException("Undefined type of " + type);
        }
        return true;
    }
}
