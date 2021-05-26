# 分布式MiniSQL

- 2021春大规模信息系统构建技术导论课程项目
- 重要功能模块建议命名为xxxManager，与minisql保持一致，方法名采用小驼峰命名法
- 项目进度：
  - 仓库建立，移植minisql
  - 完成了minisql的重构和一些基本功能模块的搭建
  - 添加了Client模块中的解释器和cache查询等基本功能，后续需要完成socketManager中的socket连接建立等功能
  - 添加了主服务器中的zookeeper模块用于查询信息
  - 添加了client和master的socket通信连接，具体的消息处理框架需要进一步完善

- 注意看注释，不然可能会感觉模型奇妙。