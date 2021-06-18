# 分布式MiniSQL

## 项目介绍

- 2020-2021春夏学期《大规模信息系统构建技术导论》课程project，一个分布式的关系型数据库引擎Distributed-MiniSQL
- **详细设计情况可以参考最终的设计文档**，目前该项目只实现了多台本地计算机的运行，至少需要开启一个Client，一个Master和一个Region才能正常使用，**运行之前请将SocketManager等相关位置的ip地址改成对应的**ip
- 比较naive，谢绝抄袭，仅供参考

## 项目文件结构

```bash
├─Client
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  └─ClientManagers
│  │  │  │      └─SocketManager
│  │  │  └─resources
│  │  └─test
│  │      └─java
│  └─target
│      ├─classes
│      │  ├─ClientManagers
│      │  │  └─SocketManager
│      │  └─META-INF
│      └─generated-sources
│          └─annotations
├─MasterServer
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  └─MasterManagers
│  │  │  │      ├─SocketManager
│  │  │  │      └─utils
│  │  │  └─resources
│  │  └─test
│  │      └─java
│  └─target
│      ├─classes
│      │  ├─MasterManagers
│      │  │  ├─SocketManager
│      │  │  └─utils
│      │  └─META-INF
│      └─generated-sources
│          └─annotations
├─RegionServer
│  ├─src
│  │  ├─main
│  │  │  ├─java
│  │  │  │  ├─miniSQL
│  │  │  │  │  ├─BUFFERMANAGER
│  │  │  │  │  ├─CATALOGMANAGER
│  │  │  │  │  ├─INDEXMANAGER
│  │  │  │  │  ├─RECORDMANAGER
│  │  │  │  │  └─test
│  │  │  │  └─RegionManagers
│  │  │  │      └─SocketManager
│  │  │  └─resources
│  │  └─test
│  │      └─java
│  └─target
│      ├─classes
│      │  ├─META-INF
│      │  ├─miniSQL
│      │  │  ├─BUFFERMANAGER
│      │  │  ├─CATALOGMANAGER
│      │  │  ├─INDEXMANAGER
│      │  │  └─RECORDMANAGER
│      │  └─RegionManagers
│      │      └─SocketManager
│      └─generated-sources
│          └─annotations
└─Report&PPT

```

