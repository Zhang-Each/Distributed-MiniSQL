package miniSQL;

public class QException extends Exception {

    public int status; //status code
    public int type; //exception type: 0 for 'syntax error' and 1 for 'rn time error'
    public String msg; //exception message
    public static final String[] ex = {"Syntax error ", "Run time error "};

    QException(int type, int status, String msg) {
        this.type = (type >= 0 && type <= ex.length) ? type : 0;
        this.status = status;
        this.msg = msg;
    }

    @Override
    public String getMessage() {
        return ex[type] + status + ": " + msg;
    }

    public void printMsg() {
        System.out.println(ex[type] + status + ": " + msg);
    }

}
