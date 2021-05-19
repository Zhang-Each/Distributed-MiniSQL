import java.io.IOException;

public class RegionServer {
    public static void main(String[] args) throws IOException {
        DataBaseManager dataBaseManager = new DataBaseManager();
        dataBaseManager.showMetaInfo();
    }
}
