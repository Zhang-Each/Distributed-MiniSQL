package miniSQL.CATALOGMANAGER;

public class Attribute {

    public String attributeName;
    public FieldType type;
    public boolean isUnique;

    public Attribute(String attributeName, NumType type, int length, boolean isUnique) {
        this.attributeName = attributeName;
        this.type = new FieldType(type, length);
        this.isUnique = isUnique;
    }

    public Attribute(String attributeName, NumType type, boolean isUnique) {
        this.attributeName = attributeName;
        this.type = new FieldType(type);
        this.isUnique = isUnique;
    }

}