package miniSQL.CATALOGMANAGER;

public class FieldType {

    public static final int CHARSIZE = 1;  //1 byte for a char
    public static final int INTSIZE = 4;   //4 bytes for an integer
    public static final int FLOATSIZE = 4; //4 bytes for a float number

    private NumType type; //type of number
    private int length; //length of char type

    FieldType() {
        //do noting
    }

    FieldType(NumType type) {
        this.type = type; //set type ( for integer and float number )
        this.length = 1;
    }

    FieldType(NumType type, int length) {
        this.type = type; //set type and length ( for char )
        this.length = length;
    }

    NumType get_type() {
        return this.type;
    }

    int getLength() {
        switch(this.type) {
            case CHAR:
                return this.length * CHARSIZE;
            case INT:
                return INTSIZE;
            case FLOAT:
                return FLOATSIZE;
            default: //undefined type
                return 0;
        }
    }

    void setType(NumType type) {
        this.type = type;
    }
    void setLength(int length) {
        this.length = length;
    }
}
