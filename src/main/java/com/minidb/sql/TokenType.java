package com.minidb.sql;

/**
 * SQL词法分析器支持的Token类型枚举
 * 
 * 按照功能分组：
 * - 字面量：标识符、数字、字符串
 * - 分隔符：逗号、括号、分号等
 * - 关键字：SQL语句关键字
 * - 操作符：比较、逻辑操作符
 * - 特殊：EOF标记
 */
public enum TokenType {
    // ========== 字面量类型 ==========
    /** 标识符（表名、列名、变量名等） */
    IDENT,
    /** 数字字面量 */
    NUMBER,
    /** 字符串字面量 */
    STRING,
    
    // ========== 分隔符 ==========
    /** 逗号 */
    COMMA,
    /** 左括号 */
    LPAREN,
    /** 右括号 */
    RPAREN,
    /** 星号（用于SELECT *） */
    STAR,
    /** 分号（语句结束符） */
    SEMI,
    
    // ========== DDL关键字 ==========
    /** CREATE关键字 */
    CREATE,
    /** TABLE关键字 */
    TABLE,
    /** DROP关键字 */
    DROP,
    
    // ========== DML关键字 ==========
    /** INSERT关键字 */
    INSERT,
    /** INTO关键字 */
    INTO,
    /** VALUES关键字 */
    VALUES,
    /** SELECT关键字 */
    SELECT,
    /** FROM关键字 */
    FROM,
    /** WHERE关键字 */
    WHERE,
    /** UPDATE关键字 */
    UPDATE,
    /** SET关键字 */
    SET,
    /** DELETE关键字 */
    DELETE,
    
    // ========== 数据类型关键字 ==========
    /** INT数据类型 */
    INT,
    /** TEXT数据类型 */
    TEXT,
    
    // ========== 比较操作符 ==========
    /** 等于 (=) */
    EQ,
    /** 小于 (<) */
    LT,
    /** 大于 (>) */
    GT,
    /** 小于等于 (<=) */
    LE,
    /** 大于等于 (>=) */
    GE,
    /** 不等于 (!=) */
    NE,
    
    // ========== 逻辑操作符 ==========
    /** LIKE操作符 */
    LIKE,
    /** AND逻辑操作符 */
    AND,
    /** OR逻辑操作符 */
    OR,
    /** NOT逻辑操作符 */
    NOT,
    /** IS关键字 */
    IS,
    /** NULL关键字 */
    NULL,
    
    // ========== JOIN操作关键字 ==========
    /** JOIN关键字 */
    JOIN,
    /** INNER JOIN */
    INNER,
    /** LEFT JOIN */
    LEFT,
    /** RIGHT JOIN */
    RIGHT,
    /** FULL JOIN */
    FULL,
    /** ON关键字（JOIN条件） */
    ON,
    
    // ========== 索引操作关键字 ==========
    /** INDEX关键字 */
    INDEX,
    /** CREATE INDEX */
    CREATE_INDEX,
    /** DROP INDEX */
    DROP_INDEX,
    
    // ========== 事务操作关键字 ==========
    /** BEGIN关键字（开始事务） */
    BEGIN,
    /** COMMIT关键字（提交事务） */
    COMMIT,
    /** ROLLBACK关键字（回滚事务） */
    ROLLBACK,
    
    // ========== 特殊标记 ==========
    /** 文件结束标记 */
    EOF;
    
    /**
     * 检查当前Token类型是否为关键字
     * 
     * @return 如果是关键字返回true，否则返回false
     */
    public boolean isKeyword() {
        return switch (this) {
            case CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, WHERE,
                 INT, TEXT, LIKE, AND, OR, NOT, IS, NULL, DELETE, DROP, UPDATE, SET,
                 JOIN, INNER, LEFT, RIGHT, FULL, ON, INDEX, BEGIN, COMMIT, ROLLBACK -> true;
            default -> false;
        };
    }
    
    /**
     * 检查当前Token类型是否为操作符
     * 
     * @return 如果是操作符返回true，否则返回false
     */
    public boolean isOperator() {
        return switch (this) {
            case EQ, LT, GT, LE, GE, NE, LIKE, AND, OR, NOT -> true;
            default -> false;
        };
    }
    
    /**
     * 检查当前Token类型是否为分隔符
     * 
     * @return 如果是分隔符返回true，否则返回false
     */
    public boolean isDelimiter() {
        return switch (this) {
            case COMMA, LPAREN, RPAREN, STAR, SEMI -> true;
            default -> false;
        };
    }
    
    /**
     * 检查当前Token类型是否为字面量
     * 
     * @return 如果是字面量返回true，否则返回false
     */
    public boolean isLiteral() {
        return switch (this) {
            case IDENT, NUMBER, STRING -> true;
            default -> false;
        };
    }
}