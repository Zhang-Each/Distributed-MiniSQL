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
    public void addAttributeValue(String attributeValue) {
        this.attributeValue.add(attributeValue);
    }

    public String getAttributeValue(int index) {
        return attributeValue.get(index);
    }

    public int getAttributeSize() {
        return attributeValue.size();
    }
}
