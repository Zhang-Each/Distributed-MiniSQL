import ClientManagers.ClientManager;

import java.io.IOException;

public class Client {

    public static void main(String[] args) throws IOException {
        ClientManager clientManager = new ClientManager();
        clientManager.run();
    }

}
