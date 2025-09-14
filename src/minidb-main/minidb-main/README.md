# MiniDB (Java)

一个简化版的数据库管理系统（教学版），支持：

- SQL 子集：`CREATE TABLE`、`INSERT INTO ... VALUES`、`SELECT ... FROM ... [WHERE ...]`，支持 `*`、列投影、`=` `<` `>` 条件
- 执行引擎：`SeqScan` + `Filter` + `Project`
- 存储系统：基于 **4KB 页** 的堆表，LRU 缓存（BufferPool），文件持久化
- 目录（Catalog）：表与模式持久化
- 界面：Swing 可视化输入与结果表格展示；也可 `--cli` 运行命令行
- 不包含：事务、并发控制、查询优化等高级特性

## 运行

```bash
mvn -q -e -DskipTests package
java -jar target/mini-db-0.1.0.jar
# 或
java -jar target/mini-db-0.1.0.jar --cli
```

示例 SQL：

```sql
CREATE TABLE users(id INT, name TEXT);
INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
SELECT * FROM users WHERE id > 1;
```

## 项目结构（Maven）

```
mini-db
├── pom.xml
├── src
│   ├── main/java/com/minidb
│   │   ├── cli/Main.java         # 程序入口：Swing 或 CLI
│   │   ├── gui/Gui.java          # Swing 界面
│   │   ├── sql/                  # 词法/语法分析 + AST
│   │   ├── engine/               # 执行器与结果结构
│   │   ├── storage/              # 页式存储、BufferPool、HeapFile
│   │   ├── catalog/              # 表目录与模式
│   │   └── utils/                # 常量、异常
│   └── test/java/com/minidb/BasicTest.java
└── README.md
```

## 设计说明

- **页式存储**：每页布局为 `nRecords | slotOffsets[] | free | records`（可追加写入）。记录采用简单的变长编码（支持 `INT`、`TEXT`、`NULL`）。
- **BufferPool**：基于 `LinkedHashMap` 的 LRU；简化实现，无脏页回写标记（写后立刻落盘）。
- **Catalog**：使用 Java 对象序列化持久化表信息与模式。
- **SQL 编译器**：手写词法/语法；错误信息简化。
- **执行模型**：顺序扫描 -> 可选过滤 -> 投影；单表查询。

> 提示：示例实现面向教学与作业；如需扩展，请在 `sql`、`engine`、`storage` 模块中继续完善（如：类型系统、更丰富表达式、删除/更新、索引、Join 等）。
