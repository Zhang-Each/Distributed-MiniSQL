package miniSQL.RECORDMANAGER;

import java.util.Vector;

public class TableRow {

    private Vector<String> attributeValue;

    public TableRow() {
        attributeValue = new Vector<>();
    }

    public TableRow(Vector<String> attributeValue) {
        this.attributeValue = new Vector<String>(attributeValue);
    }

    //add one new attribute value in table row
    public void add_attribute_value(String attributeValue) {
        this.attributeValue.add(attributeValue);
    }

    public String get_attribute_value(int index) {
        return attributeValue.get(index);
    }

    public int get_attribute_size() {
        return attributeValue.size();
    }
}
