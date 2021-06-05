package miniSQL;

import miniSQL.CATALOGMANAGER.Attribute;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.NumType;
import miniSQL.CATALOGMANAGER.Table;
import miniSQL.INDEXMANAGER.Index;
import miniSQL.INDEXMANAGER.IndexManager;
import miniSQL.RECORDMANAGER.Condition;
import miniSQL.RECORDMANAGER.RecordManager;
import miniSQL.RECORDMANAGER.TableRow;

import java.io.*;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interpreter {

    private static boolean nestLock = false; //not permit to use nesting sql file execution
    private static int execFile = 0;

    public static void main(String[] args) {
        try {
            API.initial();
            System.out.println("Welcome to minisql~");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String res = interpret("select * from stu2;;");
            System.out.println("收到的结果：\n" + res);

        } catch (IOException e) {
            System.out.println("101 Run time error : IO exception occurs");
        } catch (Exception e) {
            System.out.println("Default error: " + e.getMessage());
        }

    }

    public static String interpret(String sql) throws IOException {
        // 去掉最后的分号，否则有bug
        sql = sql.substring(0, sql.length() - 2);
        String resultValue = "";

        String result = sql.trim().replaceAll("\\s+", " ");
        String[] tokens = result.split(" ");

        try {
            if (tokens.length == 1 && tokens[0].equals(""))
                throw new QException(0, 200, "No statement specified");
            switch (tokens[0]) { //match keyword
                case "create":
                    if (tokens.length == 1)
                        throw new QException(0, 201, "Can't find create object");
                    switch (tokens[1]) {
                        case "table":
                            resultValue += parseCreateTable(result);
                            break;
                        case "index":
                            resultValue += parseCreateIndex(result);
                            break;
                        default:
                            throw new QException(0, 202, "Can't identify " + tokens[1]);
                    }
                    break;
                case "drop":
                    if (tokens.length == 1)
                        throw new QException(0, 203, "Can't find drop object");
                    switch (tokens[1]) {
                        case "table":
                            resultValue += parseDropTable(result);
                            break;
                        case "index":
                            resultValue += parseDropIndex(result);
                            break;
                        default:
                            throw new QException(0, 204, "Can't identify " + tokens[1]);
                    }
                    break;
                case "select":
                    resultValue += parseSelect(result);
                    break;
                case "insert":
                    resultValue += parseInsert(result);
                    break;
                case "delete":
                    resultValue += parseDelete(result);
                    break;
                case "execfile":
                    parseSqlFile(result);
                    break;
                case "show":
                    parseShow(result);
                    break;
                default:
                    throw new QException(0, 205, "Can't identify " + tokens[0]);
            }
        } catch (QException e) {
            System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
        } catch (Exception e) {
            System.out.println("Default error: " + e.getMessage());
        }
        return resultValue;
    }

    private static String interpret(BufferedReader reader) throws IOException {
        String restState = "";
        //rest statement after ';' in last line



        while (true) {
            StringBuilder returnValue = new StringBuilder();
            //read for each statement
            int index;
            String line;
            StringBuilder statement = new StringBuilder();
            if (restState.contains(";")) { // resetLine contains whole statement
                index = restState.indexOf(";");
                statement.append(restState.substring(0, index));
                restState = restState.substring(index + 1);
            } else {
                statement.append(restState); //add rest line
                statement.append(" ");
                if (execFile == 0)
                    System.out.print("MiniSQL-->");
//                System.out.print("-->");
                while (true) {  //read whole statement until ';'
                    line = reader.readLine();
                    if (line == null) { //read the file tail
                        reader.close();
                        break;
                        // return "";
                    } else if (line.contains(";")) { //last line
                        index = line.indexOf(";");
                        statement.append(line.substring(0, index));
                        restState = line.substring(index + 1); //set reset statement
                        break;
                    } else {
                        statement.append(line);
                        statement.append(" ");
                        if (execFile == 0)
                            System.out.print("MiniSQL-->");
//                        System.out.print("-->"); //next line
                    }
                }
            }

            //after get the whole statement
            String result = statement.toString().trim().replaceAll("\\s+", " ");
            String[] tokens = result.split(" ");

            try {
                if (tokens.length == 1 && tokens[0].equals(""))
                    throw new QException(0, 200, "No statement specified");
                switch (tokens[0]) { //match keyword
                    case "create":
                        if (tokens.length == 1)
                            throw new QException(0, 201, "Can't find create object");
                        switch (tokens[1]) {
                            case "table":
                                parseCreateTable(result);
                                break;
                            case "index":
                                parseCreateIndex(result);
                                break;
                            default:
                                throw new QException(0, 202, "Can't identify " + tokens[1]);
                        }
                        break;
                    case "drop":
                        if (tokens.length == 1)
                            throw new QException(0, 203, "Can't find drop object");
                        switch (tokens[1]) {
                            case "table":
                                parseDropTable(result);
                                break;
                            case "index":
                                parseDropIndex(result);
                                break;
                            default:
                                throw new QException(0, 204, "Can't identify " + tokens[1]);
                        }
                        break;
                    case "select":
                        returnValue.append(parseSelect(result));
                        break;
                    case "insert":
                        parseInsert(result);
                        break;
                    case "delete":
                        parseDelete(result);
                        break;
                    case "quit":
                        parseQuit(result, reader);
                        break;
                    case "execfile":
                        parseSqlFile(result);
                        break;
                    case "show":
                        parseShow(result);
                        break;
                    default:
                        throw new QException(0, 205, "Can't identify " + tokens[0]);
                }
            } catch (QException e) {
                System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            } catch (Exception e) {
                System.out.println("Default error: " + e.getMessage());
            }
            return returnValue.toString();
        }

    }

    private static void parseShow(String statement) throws Exception {
        String type = Utils.substring(statement, "show ", "").trim();
        if (type.equals("tables")) {
            CatalogManager.showTable();
        } else if (type.equals("indexes")) {
            CatalogManager.showIndex();
        } else throw new QException(0, 323, "Can not find valid key word after 'show'!");
    }

    private static String parseCreateTable(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^create table", "").trim(); //skip create table keyword

        StringBuilder result = new StringBuilder();
        int startIndex, endIndex;
        if (statement.equals("")) //no statement after create table
            throw new QException(0, 401, "Must specify a table name");

        endIndex = statement.indexOf(" ");
        if (endIndex == -1)  //no statement after create table xxx
            throw new QException(0, 402, "Can't find attribute definition");

        String tableName = statement.substring(0, endIndex); //get table name
        startIndex = endIndex + 1; //start index of '('
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            throw new QException(0, 403, "Can't not find the definition brackets in table " + tableName);

        int length;
        String[] attrParas, attrsDefine;
        String attrName, attrType, attrLength = "", primaryName = "";
        boolean attrUnique;
        Attribute attribute;
        Vector<Attribute> attrVec = new Vector<>();

        attrsDefine = statement.substring(startIndex + 1).split(","); //get each attribute definition
        for (int i = 0; i < attrsDefine.length; i++) { //for each attribute
            if (i == attrsDefine.length - 1) { //last line
                attrParas = attrsDefine[i].trim().substring(0, attrsDefine[i].length() - 1).split(" "); //remove last ')'
            } else {
                attrParas = attrsDefine[i].trim().split(" ");
            } //split each attribute in parameters: name, type,（length) (unique)

            if (attrParas[0].equals("")) { //empty
                throw new QException(0, 404, "Empty attribute in table " + tableName);
            } else if (attrParas[0].equals("primary")) { //primary key definition
                if (attrParas.length != 3 || !attrParas[1].equals("key"))  //not as primary key xxxx
                    throw new QException(0, 405, "Error definition of primary key in table " + tableName);
                if (!attrParas[2].matches("^\\(.*\\)$"))  //not as primary key (xxxx)
                    throw new QException(0, 406, "Error definition of primary key in table " + tableName);
                if (!primaryName.equals("")) //already set primary key
                    throw new QException(0, 407, "Redefinition of primary key in table " + tableName);

                primaryName = attrParas[2].substring(1, attrParas[2].length() - 1); //set primary key
            } else { //ordinary definition
                if (attrParas.length == 1)  //only attribute name
                    throw new QException(0, 408, "Incomplete definition in attribute " + attrParas[0]);
                attrName = attrParas[0]; //get attribute name
                attrType = attrParas[1]; //get attribute type
                for (int j = 0; j < attrVec.size(); j++) { //check whether name redefines
                    if (attrName.equals(attrVec.get(j).attributeName))
                        throw new QException(0, 409, "Redefinition in attribute " + attrParas[0]);
                }
                if (attrType.equals("int") || attrType.equals("float")) { //check type
                    endIndex = 2; //expected end index
                } else if (attrType.equals("char")) {
                    if (attrParas.length == 2)  //no char length
                        throw new QException(0, 410, "ust specify char length in " + attrParas[0]);
                    if (!attrParas[2].matches("^\\(.*\\)$"))  //not in char (x) form
                        throw new QException(0, 411, "Wrong definition of char length in " + attrParas[0]);

                    attrLength = attrParas[2].substring(1, attrParas[2].length() - 1); //get length
                    try {
                        length = Integer.parseInt(attrLength); //check the length
                    } catch (NumberFormatException e) {
                        throw new QException(0, 412, "The char length in " + attrParas[0] + " dosen't match a int type or overflow");
                    }
                    if (length < 1 || length > 255)
                        throw new QException(0, 413, "The char length in " + attrParas[0] + " must be in [1,255] ");
                    endIndex = 3; //expected end index
                } else { //unmatched type
                    throw new QException(0, 414, "Error attribute type " + attrType + " in " + attrParas[0]);
                }

                if (attrParas.length == endIndex) { //check unique constraint
                    attrUnique = false;
                } else if (attrParas.length == endIndex + 1 && attrParas[endIndex].equals("unique")) {  //unique
                    attrUnique = true;
                } else { //wrong definition
                    throw new QException(0, 415, "Error constraint definition in " + attrParas[0]);
                }

                if (attrType.equals("char")) { //generate attribute
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), Integer.parseInt(attrLength), attrUnique);
                } else {
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), attrUnique);
                }
                attrVec.add(attribute);
            }
        }

        if (primaryName.equals(""))  //check whether set the primary key
            throw new QException(0, 416, "Not specified primary key in table " + tableName);

        Table table = new Table(tableName, primaryName, attrVec); // create table
        API.createTable(tableName, table);
        System.out.println("-->Create table " + tableName + " successfully");
        result.append("-->Create table " + tableName + " successfully!");
        return result.toString();
    }

    private static String parseDropTable(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 601, "Not specify table name");
        if (tokens.length != 3)
            throw new QException(0, 602, "Extra parameters in drop table");

        String tableName = tokens[2]; //get table name
        API.dropTable(tableName);
        System.out.println("-->Drop table " + tableName + " successfully");
        return "-->Drop table " + tableName + " successfully!";
    }

    private static String parseCreateIndex(String statement) throws Exception {
        statement = statement.replaceAll("\\s+", " ");
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.trim();

        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 701, "Not specify index name");

        String indexName = tokens[2]; //get index name
        if (tokens.length == 3 || !tokens[3].equals("on"))
            throw new QException(0, 702, "Must add keyword 'on' after index name " + indexName);
        if (tokens.length == 4)
            throw new QException(0, 703, "Not specify table name");

        String tableName = tokens[4]; //get table name
        if (tokens.length == 5)
            throw new QException(0, 704, "Not specify attribute name in table " + tableName);

        String attrName = tokens[5];
        if (!attrName.matches("^\\(.*\\)$"))  //not as (xxx) form
            throw new QException(0, 705, "Error in specifiy attribute name " + attrName);

        attrName = attrName.substring(1, attrName.length() - 1); //extract attribute name
        if (tokens.length != 6)
            throw new QException(0, 706, "Extra parameters in create index");
        if (!CatalogManager.isUnique(tableName, attrName))
            throw new QException(1, 707, "Not a unique attribute");

        Index index = new Index(indexName, tableName, attrName);
        API.createIndex(index);
        System.out.println("-->Create index " + indexName + " successfully");
        return "-->Create index " + indexName + " successfully!";
    }

    private static String  parseDropIndex(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 801, "Not specify index name");
        if (tokens.length != 3)
            throw new QException(0, 802, "Extra parameters in drop index");

        String indexName = tokens[2]; //get table name
        API.dropIndex(indexName);
        System.out.println("-->Drop index " + indexName + " successfully");
        return "-->Drop index " + indexName + " successfully!";
    }

    private static String parseSelect(String statement) throws Exception {
        //select ... from ... where ...
        String attrStr = Utils.substring(statement, "select ", " from");
        String tabStr = Utils.substring(statement, "from ", " where");
        String conStr = Utils.substring(statement, "where ", "");
        Vector<Condition> conditions;
        Vector<String> attrNames;
        long startTime, endTime;
        startTime = System.currentTimeMillis();

        String result = "";

        if (attrStr.equals("")) {
            return "Can not find key word 'from' or lack of blank before from!";
        }
            //throw new QException(0, 250, );
        if (attrStr.trim().equals("*")) {
            //select all attributes
            if (tabStr.equals("")) {  // select * from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), new Vector<>());
                endTime = System.currentTimeMillis();
                result += Utils.printRows(ret, tabStr);
            } else { //select * from [] where [];
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = Utils.createConditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), conditions);
                endTime = System.currentTimeMillis();
                result += Utils.printRows(ret, tabStr);
            }
        } else {
            attrNames = Utils.convert(attrStr.split(" *, *")); //get attributes list
            if (tabStr.equals("")) {  //select [attr] from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, attrNames, new Vector<>());
                endTime = System.currentTimeMillis();
                result += Utils.printRows(ret, tabStr);
            } else { //select [attr] from [table] where
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = Utils.createConditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, attrNames, conditions);
                endTime = System.currentTimeMillis();
                result += Utils.printRows(ret, tabStr);
            }
        }
        double usedTime = (endTime - startTime) / 1000.0;
        System.out.println("Finished in " + usedTime + " s");
        result += "\nFinished in " + usedTime + " s";
        return result;
    }

    private static String parseInsert(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^insert", "").trim();  //skip insert keyword

        StringBuilder result = new StringBuilder();
        int startIndex, endIndex;
        if (statement.equals("")) {
            result.append("Must add keyword 'into' after insert.");
        }
        endIndex = statement.indexOf(" "); //check into keyword
        if (endIndex == -1)
            throw new QException(0, 902, "Not specify the table name");
        if (!statement.substring(0, endIndex).equals("into"))
            throw new QException(0, 903, "Must add keyword 'into' after insert");

        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check table name
        if (endIndex == -1)
            throw new QException(0, 904, "Not specify the insert value");

        String tableName = statement.substring(startIndex, endIndex); //get table name
        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check values keyword
        if (endIndex == -1)
            throw new QException(0, 905, "Syntax error: Not specify the insert value");

        if (!statement.substring(startIndex, endIndex).equals("values"))
            throw new QException(0, 906, "Must add keyword 'values' after table " + tableName);

        startIndex = endIndex + 1;
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            throw new QException(0, 907, "Can't not find the insert brackets in table " + tableName);

        String[] valueParas = statement.substring(startIndex + 1).split(","); //get attribute tokens
        TableRow tableRow = new TableRow();

        for (int i = 0; i < valueParas.length; i++) {
            if (i == valueParas.length - 1)  //last attribute
                valueParas[i] = valueParas[i].substring(0, valueParas[i].length() - 1);
            if (valueParas[i].equals("")) //empty attribute
                throw new QException(0, 908, "Empty attribute value in insert value");
            if (valueParas[i].matches("^\".*\"$") || valueParas[i].matches("^\'.*\'$"))  // extract from '' or " "
                valueParas[i] = valueParas[i].substring(1, valueParas[i].length() - 1);
            tableRow.addAttributeValue(valueParas[i]); //add to table row
        }

        //Check unique attributes
        if (tableRow.getAttributeSize() != CatalogManager.getAttributeNum(tableName))
            throw new QException(1, 909, "Attribute number doesn't match");
        Vector<Attribute> attributes = CatalogManager.getTable(tableName).attributeVector;
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.isUnique) {
                Condition cond = new Condition(attr.attributeName, "=", valueParas[i]);
                if (CatalogManager.isIndexKey(tableName, attr.attributeName)) {
                    Index idx = CatalogManager.getIndex(CatalogManager.getIndexName(tableName, attr.attributeName));
                    if (IndexManager.select(idx, cond).isEmpty())
                        continue;
                } else {
                    Vector<Condition> conditions = new Vector<>();
                    conditions.add(cond);
                    Vector<TableRow> res = RecordManager.select(tableName, conditions); //Supposed to be empty
                    if (res.isEmpty())
                        continue;
                }
                throw new QException(1, 910, "Duplicate unique key: " + attr.attributeName);
            }
        }

        API.insertRow(tableName, tableRow);
        System.out.println("");
        result.append("-->Insert successfully");
        return result.toString();
    }

    private static String parseDelete(String statement) throws Exception {
        //delete from [tabName] where []
        int num;
        StringBuilder result = new StringBuilder();
        String tabStr = Utils.substring(statement, "from ", " where").trim();
        String conStr = Utils.substring(statement, "where ", "").trim();
        Vector<Condition> conditions;
        Vector<String> attrNames;
        if (tabStr.equals("")) {  //delete from ...
            tabStr = Utils.substring(statement, "from ", "").trim();
            num = API.deleteRow(tabStr, new Vector<>());
            System.out.println("-->Delete " + num + " row(s).");
            result.append("-->Delete " + num + " row(s).");
        } else {  //delete from ... where ...
            String[] conSet = conStr.split(" *and *");
            //get condition vector
            conditions = Utils.createConditon(conSet);
            num = API.deleteRow(tabStr, conditions);
            System.out.println("-->Delete " + num + " row(s).");
            result.append("-->Delete " + num + " row(s).");
        }
        return result.toString();
    }

    private static void parseQuit(String statement, BufferedReader reader) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length != 1)
            throw new QException(0, 1001, "Extra parameters in quit");

        API.store();
        reader.close();
        System.out.println("Bye");
        System.exit(0);
    }

    private static void parseSqlFile(String statement) throws Exception {
        execFile++;
        String[] tokens = statement.split(" ");
        if (tokens.length != 2)
            throw new QException(0, 1101, "Extra parameters in sql file execution");

        String fileName = tokens[1];
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(fileName));
//            if (nestLock)  //first enter in sql file execution
//                throw new QException(0, 1102, "Can't use nested file execution");
//            nestLock = true; //lock, avoid nested execution
            interpret(fileReader);
        } catch (FileNotFoundException e) {
            throw new QException(1, 1103, "Can't find the file");
        } catch (IOException e) {
            throw new QException(1, 1104, "IO exception occurs");
        } finally {
            execFile--;
//            nestLock = false; //unlock
        }
    }
}

class Utils {

    public static final int NONEXIST = -1;
    public static final String[] OPERATOR = {"<>", "<=", ">=", "=", "<", ">"};

    public static String substring(String str, String start, String end) {
        String regex = start + "(.*)" + end;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) return matcher.group(1);
        else return "";
    }

    public static <T> Vector<T> convert(T[] array) {
        Vector<T> v = new Vector<>();
        for (int i = 0; i < array.length; i++) v.add(array[i]);
        return v;
    }

    //ab <> 'c' | cab ="fabd"  | k=5  | char= '53' | int = 2
    public static Vector<Condition> createConditon(String[] conSet) throws Exception {
        Vector<Condition> c = new Vector<>();
        for (int i = 0; i < conSet.length; i++) {
            int index = contains(conSet[i], OPERATOR);
            if (index == NONEXIST) throw new Exception("Syntax error: Invalid conditions " + conSet[i]);
            String attr = substring(conSet[i], "", OPERATOR[index]).trim();
            String value = substring(conSet[i], OPERATOR[index], "").trim().replace("\'", "").replace("\"", "");
            c.add(new Condition(attr, OPERATOR[index], value));
        }
        return c;
    }

    public static boolean checkType(String attr, boolean flag) {
        return true;
    }

    public static int contains(String str, String[] reg) {
        for (int i = 0; i < reg.length; i++) {
            if (str.contains(reg[i])) return i;
        }
        return NONEXIST;
    }

    public static void printRow(TableRow row) {
        for (int i = 0; i < row.getAttributeSize(); i++) {
            System.out.print(row.getAttributeValue(i) + "\t");
        }
        System.out.println();
    }

    public static int getMaxAttrLength(Vector<TableRow> tab, int index) {
        int len = 0;
        for (int i = 0; i < tab.size(); i++) {
            int v = tab.get(i).getAttributeValue(index).length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static String printRows(Vector<TableRow> tab, String tabName) {
        // 该函数经过修改，将所有的输出结果转化成了String返回
        StringBuilder result = new StringBuilder();
        if (tab.size() == 0) {
            System.out.println();
            return "-->Query ok! 0 rows are selected\n";
        }
        int attrSize = tab.get(0).getAttributeSize();
        int cnt = 0;
        Vector<Integer> v = new Vector<>(attrSize);
        for (int j = 0; j < attrSize; j++) {
            int len = getMaxAttrLength(tab, j);
            String attrName = CatalogManager.getAttributeName(tabName, j);
            if (attrName.length() > len) len = attrName.length();
            v.add(len);
            String format = "|%-" + len + "s";

            System.out.printf(format, attrName);
            result.append(String.format(format, attrName));
            //result.append(format).append(attrName);
            cnt = cnt + len + 1;
        }
        cnt++;

        System.out.println("|");
        result.append("|\n");

        for (int i = 0; i < cnt; i++) {
            System.out.print("-");
            result.append("-");
        }
        System.out.println();
        result.append("\n");
        for (int i = 0; i < tab.size(); i++) {
            TableRow row = tab.get(i);
            for (int j = 0; j < attrSize; j++) {
                String format = "|%-" + v.get(j) + "s";
                System.out.printf(format, row.getAttributeValue(j));
                // result.append(format).append(row.getAttributeValue(j));
                result.append(String.format(format, row.getAttributeValue(j)));
            }
            System.out.println("|");
            result.append("|\n");
        }
        System.out.println("-->Query ok! " + tab.size() + " rows are selected");
        result.append("-->Query ok! ").append(tab.size()).append(" rows are selected");
        return result.toString();
    }
}
