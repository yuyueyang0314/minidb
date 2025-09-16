package com.minidb.sql;

/**
 * 表示SQL词法分析过程中的一个Token
 * 
 * @param type Token的类型（关键字、标识符、操作符等）
 * @param text Token的原始文本内容
 * @param line Token在源代码中的行号（从1开始）
 * @param column Token在源代码中的列号（从1开始）
 */
public record Token(TokenType type, String text, int line, int column) {
    
    /**
     * 检查当前Token是否为指定类型
     * 
     * @param expectedType 期望的Token类型
     * @return 如果类型匹配返回true，否则返回false
     */
    public boolean isType(TokenType expectedType) {
        return this.type == expectedType;
    }
    
    /**
     * 检查当前Token是否为关键字
     * 
     * @return 如果是关键字返回true，否则返回false
     */
    public boolean isKeyword() {
        return switch (type) {
            case CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, WHERE,
                 INT, TEXT, LIKE, AND, OR, NOT, IS, NULL, DELETE, DROP, UPDATE, SET,
                 JOIN, INNER, LEFT, RIGHT, FULL, ON, INDEX, BEGIN, COMMIT, ROLLBACK -> true;
            default -> false;
        };
    }
    
    /**
     * 检查当前Token是否为操作符
     * 
     * @return 如果是操作符返回true，否则返回false
     */
    public boolean isOperator() {
        return switch (type) {
            case EQ, LT, GT, LE, GE, NE, LIKE, AND, OR, NOT -> true;
            default -> false;
        };
    }
    
    /**
     * 获取Token的位置信息字符串
     * 
     * @return 格式化的位置信息，如"line 5, column 10"
     */
    public String getPositionString() {
        return String.format("line %d, column %d", line, column);
    }
    
    @Override
    public String toString() {
        return String.format("Token{type=%s, text='%s', position=%s}", 
                           type, text, getPositionString());
    }
}
