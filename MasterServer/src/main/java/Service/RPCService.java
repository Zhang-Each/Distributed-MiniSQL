package Service;

import org.apache.thrift.TException;


public class RPCService implements ServerService.Iface {

    @Override
    public String getServerName(int id) throws TException {
        return "MasterServer";
    }

    @Override
    public boolean isAlive(String name) throws TException {
        return false;
    }
}
