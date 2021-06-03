package RegionManagers;

import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.Table;
import miniSQL.INDEXMANAGER.Index;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;


public class DataBaseManager {
    private LinkedHashMap<String, Table> tables;
    private LinkedHashMap<String, Index> indices;

    public DataBaseManager() throws IOException {
        CatalogManager.initialCatalog();
        this.tables = CatalogManager.getTables();
        this.indices = CatalogManager.getIndex();
    }

    /**
     * 获取当前从节点中所有数据表和索引的信息，用于后续发送给主节点，并进行查询
     */
    public String getMetaInfo() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Table> stringTableEntry : tables.entrySet()) {
            //System.out.println(((Map.Entry) stringTableEntry).getKey());
            result.append(((Map.Entry) stringTableEntry).getKey()).append(" ");
        }
        return result.toString();
    }

}
