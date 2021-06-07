package miniSQL;

import miniSQL.BUFFERMANAGER.BufferManager;
import miniSQL.CATALOGMANAGER.*;
import miniSQL.INDEXMANAGER.Index;
import miniSQL.INDEXMANAGER.IndexManager;
import miniSQL.RECORDMANAGER.Condition;
import miniSQL.RECORDMANAGER.RecordManager;
import miniSQL.RECORDMANAGER.TableRow;

import java.io.IOException;
import java.util.Vector;

public class API {

    public static void initial() throws Exception {
	    try {
		    BufferManager.initialBuffer();  //init Buffer Manager
		    CatalogManager.initialCatalog();  //init Catalog Manager
		    IndexManager.initialIndex(); //init Index Manager
	    } catch (Exception e) {
		    throw new QException(1, 500, "Failed to initialize API!");
	    }
    }

    public static void store() throws Exception {
	    CatalogManager.storeCatalog();
	    RecordManager.storeRecord();
    }

    public static boolean  createTable(String tabName, Table tab) throws Exception {
        try {
            if (RecordManager.createTable(tabName) && CatalogManager.createTable(tab)) {
                String indexName = tabName + "_index";  //refactor index name
                Index index = new Index(indexName, tabName, CatalogManager.getPrimaryKey(tabName));
                IndexManager.createIndex(index);  //create index on Index Manager
                CatalogManager.createIndex(index); //create index on Catalog Manager
                return true;
            }
        } catch (NullPointerException e) {
            throw new QException(1, 501, "Table " + tabName + " already exist!");
        } catch (IOException e) {
            throw new QException(1, 502, "Failed to create an index on table " + tabName);
        }
        throw new QException(1, 503, "Failed to create table " + tabName);
    }

    public static boolean dropTable(String tabName) throws Exception {
        try {
            for (int i = 0; i < CatalogManager.getAttributeNum(tabName); i++) {
                String attrName = CatalogManager.getAttributeName(tabName, i);
                String indexName = CatalogManager.getIndexName(tabName, attrName);  //find index if exists
                if (indexName != null) {
                    IndexManager.dropIndex(CatalogManager.getIndex(indexName)); //drop index at Index Manager
                }
            }
            if (CatalogManager.dropTable(tabName) && RecordManager.dropTable(tabName)) return true;
        } catch (NullPointerException e) {
            throw new QException(1, 504, "Table " + tabName + " does not exist!");
        }
        throw new QException(1, 505, "Failed to drop table " + tabName);
    }

    public static boolean createIndex(Index index) throws Exception {
        if (IndexManager.createIndex(index) && CatalogManager.createIndex(index)) return true;
        throw new QException(1, 506, "Failed to create index " + index.attributeName + " on table " + index.tableName);
    }

    public static boolean dropIndex(String indexName) throws Exception {
        Index index = CatalogManager.getIndex(indexName);
        if (IndexManager.dropIndex(index) && CatalogManager.dropIndex(indexName)) return true;
        throw new QException(1, 507, "Failed to drop index " + index.attributeName + " on table " + index.tableName);
    }

    public static boolean insertRow(String tabName, TableRow row) throws Exception {
        try {
            Address recordAddr = RecordManager.insert(tabName, row);  //insert and get return address
            int attrNum = CatalogManager.getAttributeNum(tabName);  //get the number of attribute
            for (int i = 0; i < attrNum; i++) {
                String attrName = CatalogManager.getAttributeName(tabName, i);
                String indexName = CatalogManager.getIndexName(tabName, attrName);  //find index if exists
                if (indexName != null) {  //index exists, then need to insert the key to BPTree
                    Index index = CatalogManager.getIndex(indexName); //get index
                    String key = row.getAttributeValue(i);  //get value of the key
                    IndexManager.insert(index, key, recordAddr);  //insert to index manager
                    CatalogManager.updateIndexTable(indexName, index); //update index
                }
            }
            CatalogManager.addRowNum(tabName);  //update number of records in catalog        return true;
            return true;
        } catch (NullPointerException e){
	        throw new QException(1, 508, "Table " + tabName + " does not exist!");
        } catch (IllegalArgumentException e) {
        	throw new QException(1, 509, e.getMessage());
        } catch (Exception e) {
            throw new QException(1, 510, "Failed to insert a row on table " + tabName);
        }
    }

    public static int deleteRow(String tabName, Vector<Condition> conditions) throws Exception {
        Condition condition = API.findIndexCondition(tabName, conditions);
        int numberOfRecords = 0;
        if (condition != null) {
            try {
                String indexName = CatalogManager.getIndexName(tabName, condition.getName());
                Index idx = CatalogManager.getIndex(indexName);
                Vector<Address> addresses = IndexManager.select(idx, condition);
                if (addresses != null) {
                    numberOfRecords = RecordManager.delete(addresses, conditions);
                }
            } catch (NullPointerException e) {
	            throw new QException(1, 511, "Table " + tabName + " does not exist!");
            } catch (IllegalArgumentException e) {
	            throw new QException(1, 512, e.getMessage());
            } catch (Exception e) {
                throw new QException(1, 513, "Failed to delete on table " + tabName);
            }
        } else {
            try {
            	numberOfRecords = RecordManager.delete(tabName, conditions);
            }  catch (NullPointerException e) {
	            throw new QException(1, 514, "Table " + tabName + " does not exist!");
            } catch (IllegalArgumentException e) {
	            throw new QException(1, 515, e.getMessage());
            }
        }
        CatalogManager.deleteRowNum(tabName, numberOfRecords);
        return numberOfRecords;
    }

    public static Vector<TableRow> select(String tabName, Vector<String> attriName, Vector<Condition> conditions) throws Exception {
	    Vector<TableRow> resultSet = new Vector<>();
	    Condition condition = API.findIndexCondition(tabName, conditions);
	    if (condition != null) {
		    try {
			    String indexName = CatalogManager.getIndexName(tabName, condition.getName());
			    Index idx = CatalogManager.getIndex(indexName);
			    Vector<Address> addresses = IndexManager.select(idx, condition);
			    if (addresses != null) {
				    resultSet = RecordManager.select(addresses, conditions);
			    }
		    } catch (NullPointerException e) {
			    throw new QException(1, 516, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 517, e.getMessage());
		    } catch (Exception e) {
			    throw new QException(1, 518, "Failed to select from table " + tabName);
		    }
	    } else {
		    try {
			    resultSet = RecordManager.select(tabName, conditions);
		    } catch (NullPointerException e) {
			    throw new QException(1, 519, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 520, e.getMessage());
		    }
	    }

	    if (!attriName.isEmpty()) {
		    try {
			    return RecordManager.project(tabName, resultSet, attriName);
		    } catch (NullPointerException e) {
			    throw new QException(1, 521, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 522, e.getMessage());
		    }
	    } else {
		    return resultSet;
	    }

    }

    private static Condition findIndexCondition(String tabName, Vector<Condition> conditions) throws Exception {
        Condition condition = null;
        for (int i = 0; i < conditions.size(); i++) {
            if (CatalogManager.getIndexName(tabName, conditions.get(i).getName()) != null) {
                condition = conditions.get(i);
                conditions.remove(condition);
                break;
            }
        }
        return condition;
    }


}
