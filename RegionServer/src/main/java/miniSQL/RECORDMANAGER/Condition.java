package miniSQL.RECORDMANAGER;

import miniSQL.CATALOGMANAGER.CatalogManager;

public class Condition {
    private String name;  //attribute name in condition
    private String value; //attribute value in condition
    private String operator; //condition operator

    public Condition() {
        //do nothing
    }

    public Condition(String name, String operator, String value) {
        this.name = name; //initialize name, operator and value
        this.operator = operator;
        this.value = value;
    }

    //if the data in table satisfy the condition, return true, else return false 
    public boolean satisfy(String tableName, TableRow data) {
        int index = CatalogManager.getAttributeIndex(tableName, this.name); //get attribute index
        String type = CatalogManager.getType(tableName, index); //get type

        if (type.equals("CHAR")) { //char type
            String cmpObject = data.getAttributeValue(index);
            String cmpValue = this.value;

            if (this.operator.equals("=")) {
                return cmpObject.compareTo(cmpValue) == 0;
            } else if (this.operator.equals("<>")) {
                return cmpObject.compareTo(cmpValue) != 0;
            } else if (this.operator.equals(">")) {
                return cmpObject.compareTo(cmpValue) > 0;
            } else if (this.operator.equals("<")) {
                return cmpObject.compareTo(cmpValue) < 0;
            } else if (this.operator.equals(">=")) {
                return cmpObject.compareTo(cmpValue) >= 0;
            } else if (this.operator.equals("<=")) {
                return cmpObject.compareTo(cmpValue) <= 0;
            } else { //undefined operator
                return false;
            }
        } else if (type.equals("INT")) { //integer type
            int cmpObject = Integer.parseInt(data.getAttributeValue(index));
            int cmpValue = Integer.parseInt(this.value);
            switch (this.operator) {
                case "=":
                    return cmpObject == cmpValue;
                case "<>":
                    return cmpObject != cmpValue;
                case ">":
                    return cmpObject > cmpValue;
                case "<":
                    return cmpObject < cmpValue;
                case ">=":
                    return cmpObject >= cmpValue;
                case "<=":
                    return cmpObject <= cmpValue;
                default:
                    return false;
            }
        } else if (type.equals("FLOAT")) { //float type
            float cmpObject = Float.parseFloat(data.getAttributeValue(index));
            float cmpValue = Float.parseFloat(this.value);
            if (this.operator.equals("=")) {
                return cmpObject == cmpValue;
            } else if (this.operator.equals("<>")) {
                return cmpObject != cmpValue;
            } else if (this.operator.equals(">")) {
                return cmpObject > cmpValue;
            } else if (this.operator.equals("<")) {
                return cmpObject < cmpValue;
            } else if (this.operator.equals(">=")) {
                return cmpObject >= cmpValue;
            } else if (this.operator.equals("<=")) {
                return cmpObject <= cmpValue;
            } else { //undefined operator
                return false;
            }
        } else { //undefined type
            return false;
        }
    }


    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }

    public String getOperator() {
        return this.operator;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
