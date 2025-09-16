package com.minidb.utils;

/**
 * 数据库系统常量定义
 * 
 * 包含系统中使用的各种常量值，如页大小、目录路径等
 */
public final class Constants {
    
    // 私有构造函数，防止实例化
    private Constants() {
        throw new UnsupportedOperationException("Constants class cannot be instantiated");
    }
    
    // ========== 存储相关常量 ==========
    
    /** 页大小：4KB（4096字节） */
    public static final int PAGE_SIZE = 4096;
    
    /** 数据库数据目录名称 */
    public static final String DB_DIR = "data";
    
    /** 索引目录名称 */
    public static final String INDEX_DIR = "indexes";
    
    /** 日志目录名称 */
    public static final String LOG_DIR = "logs";
    
    // ========== 缓存相关常量 ==========
    
    /** 默认缓存池大小 */
    public static final int DEFAULT_BUFFER_POOL_SIZE = 64;
    
    /** 最小缓存池大小 */
    public static final int MIN_BUFFER_POOL_SIZE = 8;
    
    /** 最大缓存池大小 */
    public static final int MAX_BUFFER_POOL_SIZE = 1024;
    
    // ========== B+树相关常量 ==========
    
    /** B+树的默认阶数 */
    public static final int DEFAULT_BPLUS_TREE_ORDER = 4;
    
    /** B+树的最小阶数 */
    public static final int MIN_BPLUS_TREE_ORDER = 3;
    
    /** B+树的最大阶数 */
    public static final int MAX_BPLUS_TREE_ORDER = 10;
    
    // ========== 事务相关常量 ==========
    
    /** 事务超时时间（毫秒） */
    public static final long TRANSACTION_TIMEOUT_MS = 30000; // 30秒
    
    /** 最大并发事务数 */
    public static final int MAX_CONCURRENT_TRANSACTIONS = 100;
    
    // ========== 查询相关常量 ==========
    
    /** 最大查询结果行数 */
    public static final int MAX_QUERY_RESULT_ROWS = 10000;
    
    /** 最大JOIN表数量 */
    public static final int MAX_JOIN_TABLES = 10;
    
    // ========== 字符串相关常量 ==========
    
    /** 最大字符串长度 */
    public static final int MAX_STRING_LENGTH = 1000;
    
    /** 最大标识符长度 */
    public static final int MAX_IDENTIFIER_LENGTH = 64;
    
    // ========== 文件相关常量 ==========
    
    /** 表文件扩展名 */
    public static final String TABLE_FILE_EXTENSION = ".dat";
    
    /** 元数据文件扩展名 */
    public static final String METADATA_FILE_EXTENSION = ".meta";
    
    /** 索引文件扩展名 */
    public static final String INDEX_FILE_EXTENSION = ".idx";
    
    // ========== 错误消息常量 ==========
    
    /** 通用错误消息前缀 */
    public static final String ERROR_PREFIX = "MiniDB Error";
    
    /** 警告消息前缀 */
    public static final String WARNING_PREFIX = "MiniDB Warning";
    
    /** 信息消息前缀 */
    public static final String INFO_PREFIX = "MiniDB Info";
}
