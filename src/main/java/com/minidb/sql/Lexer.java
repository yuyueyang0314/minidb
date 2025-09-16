package com.minidb.sql;

import java.util.*;

/**
 * SQL词法分析器
 * 
 * 将SQL字符串分解为Token序列，支持：
 * - 关键字识别（CREATE, SELECT, INSERT等）
 * - 标识符识别（表名、列名等）
 * - 字面量识别（数字、字符串）
 * - 操作符识别（=, <, >, LIKE等）
 * - 位置跟踪（行号、列号）
 * - 错误处理（未终止字符串、未知字符等）
 */
public class Lexer {
    /** 待分析的SQL字符串 */
    private final String input;
    /** 当前字符索引 */
    private int currentIndex = 0;
    /** 当前行号（从1开始） */
    private int currentLine = 1;
    /** 当前列号（从1开始） */
    private int currentColumn = 1;
    
    /**
     * 创建词法分析器
     * 
     * @param input 待分析的SQL字符串
     */
    public Lexer(String input) { 
        this.input = input; 
    }

    /**
     * 检查是否已到达输入字符串末尾
     * 
     * @return 如果已到达末尾返回true，否则返回false
     */
    private boolean isEndOfInput() { 
        return currentIndex >= input.length(); 
    }
    
    /**
     * 查看当前字符但不移动索引
     * 
     * @return 当前字符
     * @throws IndexOutOfBoundsException 如果已到达末尾
     */
    private char peekCurrentChar() { 
        return input.charAt(currentIndex); 
    }
    
    /**
     * 跳过空白字符（空格、制表符、换行符等）
     * 同时更新行号和列号
     */
    private void skipWhitespace() { 
        while (!isEndOfInput() && Character.isWhitespace(peekCurrentChar())) {
            if (peekCurrentChar() == '\n') {
                currentLine++;
                currentColumn = 1;
            } else {
                currentColumn++;
            }
            currentIndex++;
        }
    }
    
    /**
     * 向前移动一个字符
     * 同时更新行号和列号
     */
    private void advance() {
        if (!isEndOfInput()) {
            if (peekCurrentChar() == '\n') {
                currentLine++;
                currentColumn = 1;
            } else {
                currentColumn++;
            }
            currentIndex++;
        }
    }
    
    /**
     * 获取当前位置信息
     * 
     * @return 当前位置的行号和列号
     */
    private Position getCurrentPosition() {
        return new Position(currentLine, currentColumn);
    }
    
    /**
     * 位置信息记录类
     */
    private record Position(int line, int column) {
        @Override
        public String toString() {
            return String.format("line %d, column %d", line, column);
        }
    }

    /**
     * 执行词法分析，将SQL字符串分解为Token序列
     * 
     * @return Token列表
     * @throws LexerException 如果遇到词法错误
     */
    public List<Token> lex() {
        List<Token> tokens = new ArrayList<>();
        
        while (true) {
            // 跳过空白字符
            skipWhitespace();
            
            // 检查是否到达输入末尾
            if (isEndOfInput()) {
                tokens.add(new Token(TokenType.EOF, "", currentLine, currentColumn));
                break;
            }
            
            char currentChar = peekCurrentChar();
            Position startPosition = getCurrentPosition();
            
            // 根据字符类型进行不同的处理
            if (Character.isJavaIdentifierStart(currentChar)) {
                tokens.add(parseIdentifierOrKeyword(startPosition));
            } else if (Character.isDigit(currentChar)) {
                tokens.add(parseNumber(startPosition));
            } else if (currentChar == '\'') {
                tokens.add(parseString(startPosition));
            } else {
                tokens.add(parseOperatorOrDelimiter(startPosition));
            }
        }
        
        return tokens;
    }
    
    /**
     * 解析标识符或关键字
     * 
     * @param startPosition 开始位置
     * @return 解析得到的Token
     */
    private Token parseIdentifierOrKeyword(Position startPosition) {
        int startIndex = currentIndex;
        advance(); // 跳过第一个字符
        
        // 继续读取标识符的其余部分
        while (!isEndOfInput() && Character.isJavaIdentifierPart(peekCurrentChar())) {
            advance();
        }
        
        String text = input.substring(startIndex, currentIndex);
        String upperText = text.toUpperCase();
        
        // 根据文本内容确定Token类型
        TokenType type = getKeywordType(upperText);
        return new Token(type, text, startPosition.line(), startPosition.column());
    }
    
    /**
     * 根据关键字文本获取对应的TokenType
     * 
     * @param keyword 关键字文本（已转换为大写）
     * @return 对应的TokenType，如果不是关键字则返回IDENT
     */
    private TokenType getKeywordType(String keyword) {
        return switch (keyword) {
            case "CREATE" -> TokenType.CREATE;
            case "TABLE" -> TokenType.TABLE;
            case "INSERT" -> TokenType.INSERT;
            case "INTO" -> TokenType.INTO;
            case "VALUES" -> TokenType.VALUES;
            case "SELECT" -> TokenType.SELECT;
            case "FROM" -> TokenType.FROM;
            case "WHERE" -> TokenType.WHERE;
            case "INT" -> TokenType.INT;
            case "TEXT" -> TokenType.TEXT;
            case "LIKE" -> TokenType.LIKE;
            case "AND" -> TokenType.AND;
            case "OR" -> TokenType.OR;
            case "NOT" -> TokenType.NOT;
            case "IS" -> TokenType.IS;
            case "NULL" -> TokenType.NULL;
            case "DELETE" -> TokenType.DELETE;
            case "DROP" -> TokenType.DROP;
            case "UPDATE" -> TokenType.UPDATE;
            case "SET" -> TokenType.SET;
            case "JOIN" -> TokenType.JOIN;
            case "INNER" -> TokenType.INNER;
            case "LEFT" -> TokenType.LEFT;
            case "RIGHT" -> TokenType.RIGHT;
            case "FULL" -> TokenType.FULL;
            case "ON" -> TokenType.ON;
            case "INDEX" -> TokenType.INDEX;
            case "BEGIN" -> TokenType.BEGIN;
            case "COMMIT" -> TokenType.COMMIT;
            case "ROLLBACK" -> TokenType.ROLLBACK;
            default -> TokenType.IDENT;
        };
    }
    
    /**
     * 解析数字字面量
     * 
     * @param startPosition 开始位置
     * @return 解析得到的Token
     */
    private Token parseNumber(Position startPosition) {
        int startIndex = currentIndex;
        advance(); // 跳过第一个数字
        
        // 继续读取数字的其余部分
        while (!isEndOfInput() && Character.isDigit(peekCurrentChar())) {
            advance();
        }
        
        String text = input.substring(startIndex, currentIndex);
        return new Token(TokenType.NUMBER, text, startPosition.line(), startPosition.column());
    }
    
    /**
     * 解析字符串字面量
     * 
     * @param startPosition 开始位置
     * @return 解析得到的Token
     * @throws LexerException 如果字符串未正确终止
     */
    private Token parseString(Position startPosition) {
        advance(); // 跳过开始引号
        int startIndex = currentIndex;
        
        // 查找结束引号
        while (!isEndOfInput() && peekCurrentChar() != '\'') {
            if (peekCurrentChar() == '\n') {
                throw new LexerException("Unterminated string literal", currentLine, currentColumn);
            }
            advance();
        }
        
        if (isEndOfInput()) {
            throw new LexerException("Unterminated string literal", startPosition.line(), startPosition.column());
        }
        
        String text = input.substring(startIndex, currentIndex);
        advance(); // 跳过结束引号
        
        return new Token(TokenType.STRING, text, startPosition.line(), startPosition.column());
    }
    
    /**
     * 解析操作符或分隔符
     * 
     * @param startPosition 开始位置
     * @return 解析得到的Token
     * @throws LexerException 如果遇到未知字符
     */
    private Token parseOperatorOrDelimiter(Position startPosition) {
        char currentChar = peekCurrentChar();
        advance(); // 跳过当前字符
        
        return switch (currentChar) {
            case '=' -> new Token(TokenType.EQ, "=", startPosition.line(), startPosition.column());
            case '<' -> parseLessThanOperator(startPosition);
            case '>' -> parseGreaterThanOperator(startPosition);
            case '!' -> parseNotEqualOperator(startPosition);
            case ',' -> new Token(TokenType.COMMA, ",", startPosition.line(), startPosition.column());
            case '(' -> new Token(TokenType.LPAREN, "(", startPosition.line(), startPosition.column());
            case ')' -> new Token(TokenType.RPAREN, ")", startPosition.line(), startPosition.column());
            case '*' -> new Token(TokenType.STAR, "*", startPosition.line(), startPosition.column());
            case ';' -> new Token(TokenType.SEMI, ";", startPosition.line(), startPosition.column());
            default -> throw new LexerException("Unknown character: '" + currentChar + "'", 
                                              startPosition.line(), startPosition.column());
        };
    }
    
    /**
     * 解析小于操作符（< 或 <=）
     */
    private Token parseLessThanOperator(Position startPosition) {
        if (!isEndOfInput() && peekCurrentChar() == '=') {
            advance();
            return new Token(TokenType.LE, "<=", startPosition.line(), startPosition.column());
        } else {
            return new Token(TokenType.LT, "<", startPosition.line(), startPosition.column());
        }
    }
    
    /**
     * 解析大于操作符（> 或 >=）
     */
    private Token parseGreaterThanOperator(Position startPosition) {
        if (!isEndOfInput() && peekCurrentChar() == '=') {
            advance();
            return new Token(TokenType.GE, ">=", startPosition.line(), startPosition.column());
        } else {
            return new Token(TokenType.GT, ">", startPosition.line(), startPosition.column());
        }
    }
    
    /**
     * 解析不等于操作符（!=）
     */
    private Token parseNotEqualOperator(Position startPosition) {
        if (!isEndOfInput() && peekCurrentChar() == '=') {
            advance();
            return new Token(TokenType.NE, "!=", startPosition.line(), startPosition.column());
        } else {
            throw new LexerException("Unexpected character '!'", startPosition.line(), startPosition.column());
        }
    }
}
