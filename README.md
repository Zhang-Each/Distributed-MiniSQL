# 分布式MiniSQL

## 项目介绍

- 2020-2021春夏学期《大规模信息系统构建技术导论》课程project，一个分布式的关系型数据库引擎Distributed-MiniSQL
- **详细设计情况可以参考最终的设计文档**，目前该项目只实现了多台本地计算机的运行，至少需要开启一个Client，一个Master和一个Region才能正常使用，**运行之前请将SocketManager等相关位置的ip地址改成对应的**ip
- 比较naive，谢绝抄袭，仅供参考

## 项目文件结构

```bash
├─Client
│  │  pom.xml
│  │  
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  │  Client.java
│  │  │  │  │  
│  │  │  │  └─ClientManagers
│  │  │  │      │  CacheManager.java
│  │  │  │      │  ClientManager.java
│  │  │  │      │  
│  │  │  │      └─SocketManager
│  │  │  │              MasterSocketManager.java
│  │  │  │              RegionSocketManager.java
│  │  │  │              
│  │  │  └─resources
│  │  └─test
│  │      └─java
│  └─target
|
├─MasterServer
│  │  pom.xml
│  │  
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  │  MasterServer.java
│  │  │  │  │  
│  │  │  │  └─MasterManagers
│  │  │  │      │  MasterManager.java
│  │  │  │      │  TableManger.java
│  │  │  │      │  ZookeeperManager.java
│  │  │  │      │  
│  │  │  │      ├─SocketManager
│  │  │  │      │      ClientProcessor.java
│  │  │  │      │      RegionProcessor.java
│  │  │  │      │      SocketManager.java
│  │  │  │      │      SocketThread.java
│  │  │  │      │      
│  │  │  │      └─utils
│  │  │  │              CuratorHolder.java
│  │  │  │              ServiceMonitor.java
│  │  │  │              ServiceStrategyExecutor.java
│  │  │  │              SocketUtils.java
│  │  │  │              StrategyTypeEnum.java
│  │  │  │              
│  │  │  └─resources
│  │  │          log4j.properties
│  │  │          
│  │  └─test
│  │      └─java
│  └─target
│      
├─RegionServer
│  │  pom.xml
│  │  
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  │  RegionServer.java
│  │  │  │  │  
│  │  │  │  ├─miniSQL
│  │  │  │  │  │  API.java
│  │  │  │  │  │  Interpreter.java
│  │  │  │  │  │  Main.java
│  │  │  │  │  │  QException.java
│  │  │  │  │  │  
│  │  │  │  │  ├─BUFFERMANAGER
│  │  │  │  │  │      Block.java
│  │  │  │  │  │      BufferManager.java
│  │  │  │  │  │      
│  │  │  │  │  ├─CATALOGMANAGER
│  │  │  │  │  │      Address.java
│  │  │  │  │  │      Attribute.java
│  │  │  │  │  │      CatalogManager.java
│  │  │  │  │  │      FieldType.java
│  │  │  │  │  │      NumType.java
│  │  │  │  │  │      Table.java
│  │  │  │  │  │      
│  │  │  │  │  ├─INDEXMANAGER
│  │  │  │  │  │      BPTree.java
│  │  │  │  │  │      Index.java
│  │  │  │  │  │      IndexManager.java
│  │  │  │  │  │      
│  │  │  │  │  ├─RECORDMANAGER
│  │  │  │  │  │      Condition.java
│  │  │  │  │  │      RecordManager.java
│  │  │  │  │  │      TableRow.java
│  │  │  │  │  │      
│  │  │  │  │  └─test
│  │  │  │  │          TestRecord.java
│  │  │  │  │          
│  │  │  │  └─RegionManagers
│  │  │  │      │  DataBaseManager.java
│  │  │  │      │  RegionManager.java
│  │  │  │      │  zkServiceManager.java
│  │  │  │      │  
│  │  │  │      └─SocketManager
│  │  │  │              ClientSocketManager.java
│  │  │  │              ClientThread.java
│  │  │  │              FtpUtils.java
│  │  │  │              MasterSocketManager.java
│  │  │  │              
│  │  │  └─resources
│  │  └─test
│  │      └─java
│  └─target
│
└─Report&PPT
        初期设计报告.pdf
        最终设计报告.pdf
        答辩ppt.pptx

```

