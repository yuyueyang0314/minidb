package com.minidb.sql;

/**
 * 词法分析器异常
 * 
 * 当词法分析过程中遇到错误时抛出，包含详细的错误位置信息
 */
public class LexerException extends RuntimeException {
    /** 错误发生的行号（从1开始） */
    private final int line;
    /** 错误发生的列号（从1开始） */
    private final int column;
    
    /**
     * 创建词法分析器异常
     * 
     * @param message 错误消息
     * @param line 错误行号
     * @param column 错误列号
     */
    public LexerException(String message, int line, int column) {
        super(String.format("Lexical error at line %d, column %d: %s", line, column, message));
        this.line = line;
        this.column = column;
    }
    
    /**
     * 获取错误行号
     * 
     * @return 错误行号
     */
    public int getLine() { 
        return line; 
    }
    
    /**
     * 获取错误列号
     * 
     * @return 错误列号
     */
    public int getColumn() { 
        return column; 
    }
    
    /**
     * 获取格式化的位置信息
     * 
     * @return 位置信息字符串，如"line 5, column 10"
     */
    public String getPositionString() {
        return String.format("line %d, column %d", line, column);
    }
    
    @Override
    public String toString() {
        return String.format("LexerException: %s at %s", getMessage(), getPositionString());
    }
}
