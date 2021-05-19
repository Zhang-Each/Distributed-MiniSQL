package miniSQL;

import miniSQL.BUFFERMANAGER.BufferManager;
import miniSQL.CATALOGMANAGER.NumType;
import miniSQL.INDEXMANAGER.Index;
import miniSQL.CATALOGMANAGER.Attribute;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.Table;

import java.util.Vector;

public class Main {

    public static void main(String[] args) {
        API api = new API();
        // buffer_unit_test(); //Buffer Manager test function
        catalog_unit_test1();
        catalog_unit_test2();
    }

    public static void buffer_unit_test() {
        String buffer_test_file_name = "buffer_test";
        try {
            BufferManager m = new BufferManager();
            m.testInterface();
            int bid = m.readBlockFromDisk(buffer_test_file_name, 15);
            buffer_print(m,bid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void buffer_print(BufferManager m, int bid) {
        System.out.println(bid);
        System.out.println("isLock = " + m.buffer[bid].lock());
        System.out.println("isDirty = " + m.buffer[bid].dirty());
        System.out.println("isValid = " + m.buffer[bid].valid());
        System.out.println(m.buffer[bid].readInteger(1200));
        System.out.println(m.buffer[bid].readFloat(76));
        System.out.println(m.buffer[bid].readString(492, 6));
        m.buffer[bid].writeInteger(128, -23333);
        System.out.println("isLock = " + m.buffer[bid].lock());
        System.out.println("isDirty = " + m.buffer[bid].dirty());
        System.out.println("isValid = " + m.buffer[bid].valid());
        System.out.println("LRUCnt = " + m.buffer[bid].getLRU());
        System.out.println(m.buffer[bid].readInteger(128));
    }

    private static void catalog_unit_test2() {
        try {
            CatalogManager.initialCatalog();
            CatalogManager.showCatalog();
            CatalogManager.storeCatalog();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void catalog_unit_test1() {
        try {
            CatalogManager.initialCatalog();
            Attribute tmpAttribute1 = new Attribute("id", NumType.valueOf("INT"), true);
            Attribute tmpAttribute2 = new Attribute("name", NumType.valueOf("CHAR"), 12, true);
            Attribute tmpAttribute3 = new Attribute("category", NumType.valueOf("CHAR"), 20, true);
            Vector<Attribute> tmpAttributeVector = new Vector<>();
            tmpAttributeVector.addElement(tmpAttribute1);
            tmpAttributeVector.addElement(tmpAttribute2);
            Table tmpTable1 = new Table("students", "id", tmpAttributeVector);
            CatalogManager.createTable(tmpTable1);
            CatalogManager.showCatalog();
            Index tmpIndex1 = new Index("idIndex", "students", "id");
            CatalogManager.createIndex(tmpIndex1);
            CatalogManager.showCatalog();
            tmpAttributeVector.addElement(tmpAttribute3);
            Table tmpTable2 = new Table("book", "name", tmpAttributeVector);
            CatalogManager.createTable(tmpTable2);
            CatalogManager.showCatalog();
            //CatalogManager.drop_table("students");
            //CatalogManager.show_catalog();
            //CatalogManager.drop_index("idIndex");
            Index tmpIndex2 = new Index("categoryIndex", "book", "category");
            CatalogManager.createIndex(tmpIndex2);
            CatalogManager.showCatalog();
            CatalogManager.storeCatalog();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
