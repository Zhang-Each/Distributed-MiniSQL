namespace java org.example.thrift

service ServerService {
    string getServerName(1: i32 id)
    bool isAlive(1: string name)
}