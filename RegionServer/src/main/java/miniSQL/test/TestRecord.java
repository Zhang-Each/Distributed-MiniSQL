/*package test;

import BUFFERMANAGER.BufferManager;
import CATALOGMANAGER.*;
import RECORDMANAGER.Condition;
import RECORDMANAGER.RecordManager;
import RECORDMANAGER.TableRow;

import java.util.Vector;

public class TestRecord {
    public static void main(String[] args) {
        try {
            CatalogManager.initial_catalog(); //initialize
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
        BufferManager.initial_buffer();
        Vector<Attribute> attributes = new Vector<>(); //create table
        Attribute attribute;
        attribute = new Attribute("id", NumType.valueOf("INT"), true);
        attributes.add(attribute);
        attribute = new Attribute("name", NumType.valueOf("CHAR"), 20, false);
        attributes.add(attribute);
        attribute = new Attribute("price", NumType.valueOf("FLOAT"), false);
        attributes.add(attribute);

        Table table = new Table("Goods", "id", attributes);
        if(RecordManager.create_table("Goods")) { //if create successfully
            CatalogManager.create_table(table);
            Vector<Condition> conditions;
            Vector<TableRow> tableRows;

            Vector<Address> addresses;
            insertTuples(0,1000);
            conditions = new Vector<>();
            selectTuples(conditions);

            conditions = new Vector<>();
            conditions.add(new Condition("id", "<>", "8"));
            conditions.add(new Condition("name", "<", "8"));
            conditions.add(new Condition("price", "<", "60000"));
            deleteTuples(conditions);

            insertTuples(1001,1100);
            conditions = new Vector<>();
            selectTuples(conditions);

           try {
               CatalogManager.store_catalog();
               RecordManager.store_record();
           } catch(Exception e) {
               System.out.println(e.getMessage());
           }

        }
    }

    private static void printResult(String tableName, Vector<TableRow> tableRows) {
        System.out.println("Select result:\n");
        int attributeNum = CatalogManager.get_attribute_num(tableName);
        for(int i = 0;i < tableRows.size();i++) {
            for(int j = 0; j < attributeNum;j++) {
                System.out.println(tableRows.get(i).get_attribute_value(j)+" ");
            }
            System.out.println("\n");
        }
    }
    private static Vector<Address> insertTuples(int start, int end) {
        Vector<Address> addresses = new Vector<>();
        System.out.println("Insert start.....\n");
        for(int i = start;i <= end;i++) {
            TableRow newRow = new TableRow();
            newRow.add_attribute_value(String.valueOf(i));
            newRow.add_attribute_value(i+"Food"+i);
            newRow.add_attribute_value(i+"12.34"+i);
            Address address = RecordManager.insert("Goods",newRow);
            System.out.println("Address: "+address.get_file_name()
                                +" "+address.get_block_offset()
                                +" "+address.get_byte_offset()+"\n");
            CatalogManager.add_row_num("Goods");
            addresses.add(address);
        }
        System.out.println("Insert end.....\n");
        return addresses;
    }
    private static void deleteTuples(Vector<Condition> conditions) {
        System.out.println("Delete start.....\n");
        int num = RecordManager.delete("Goods",conditions);
        CatalogManager.delete_row_num("Goods",num);
        System.out.println("Delete number: "+num+"\n");
        System.out.println("Delete end.....\n");
    }
    private static void selectTuples(Vector<Condition> conditions) {
        System.out.println("Select start.....\n");
        Vector<TableRow> tableRows = RecordManager.select("Goods",conditions);
        printResult("Goods", tableRows);
        System.out.println("Select end.....\n");

    }
    private static void deleteTuplesInAddress(Vector<Address> addresses) {
        Vector<Condition> contidions = new Vector<>();
        System.out.println("Delete address start.....\n");
        int num = RecordManager.delete(addresses, contidions);
        CatalogManager.delete_row_num("Goods",num);
        System.out.println("Delete number: "+num+"\n");
        System.out.println("Delete address end.....\n");
    }
    private static void selectTuplesInAddress(Vector<Address> addresses) {
        Vector<Condition> contidions = new Vector<>();
        System.out.println("Select address start.....\n");
        Vector<TableRow> tableRows = RecordManager.select(addresses, contidions);
        printResult("Goods", tableRows);
        System.out.println("Select address end.....\n");
    }

}*/
