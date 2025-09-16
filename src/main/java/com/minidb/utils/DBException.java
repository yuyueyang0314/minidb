package com.minidb.utils;

/**
 * 数据库系统异常
 * 
 * 数据库操作过程中可能抛出的各种异常的基类
 * 包含错误消息和可选的异常原因
 */
public class DBException extends RuntimeException {
    
    /**
     * 创建数据库异常
     * 
     * @param message 错误消息
     */
    public DBException(String message) { 
        super(message); 
    }
    
    /**
     * 创建数据库异常
     * 
     * @param message 错误消息
     * @param cause 异常原因
     */
    public DBException(String message, Throwable cause) { 
        super(message, cause); 
    }
    
    /**
     * 创建数据库异常（带格式化消息）
     * 
     * @param format 消息格式字符串
     * @param args 格式参数
     */
    public DBException(String format, Object... args) {
        super(String.format(format, args));
    }
    
    /**
     * 创建数据库异常（带格式化消息和原因）
     * 
     * @param cause 异常原因
     * @param format 消息格式字符串
     * @param args 格式参数
     */
    public DBException(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }
    
    /**
     * 获取格式化的错误消息
     * 
     * @return 包含错误前缀的格式化消息
     */
    @Override
    public String getMessage() {
        return Constants.ERROR_PREFIX + ": " + super.getMessage();
    }
    
    /**
     * 创建表不存在异常
     * 
     * @param tableName 表名
     * @return 表不存在异常
     */
    public static DBException tableNotFound(String tableName) {
        return new DBException("Table '%s' does not exist", tableName);
    }
    
    /**
     * 创建列不存在异常
     * 
     * @param columnName 列名
     * @return 列不存在异常
     */
    public static DBException columnNotFound(String columnName) {
        return new DBException("Column '%s' does not exist", columnName);
    }
    
    /**
     * 创建索引不存在异常
     * 
     * @param indexName 索引名
     * @return 索引不存在异常
     */
    public static DBException indexNotFound(String indexName) {
        return new DBException("Index '%s' does not exist", indexName);
    }
    
    /**
     * 创建类型不匹配异常
     * 
     * @param expectedType 期望类型
     * @param actualType 实际类型
     * @return 类型不匹配异常
     */
    public static DBException typeMismatch(String expectedType, String actualType) {
        return new DBException("Type mismatch: expected %s, got %s", expectedType, actualType);
    }
    
    /**
     * 创建语法错误异常
     * 
     * @param message 错误消息
     * @return 语法错误异常
     */
    public static DBException syntaxError(String message) {
        return new DBException("Syntax error: %s", message);
    }
    
    /**
     * 创建语义错误异常
     * 
     * @param message 错误消息
     * @return 语义错误异常
     */
    public static DBException semanticError(String message) {
        return new DBException("Semantic error: %s", message);
    }
}
