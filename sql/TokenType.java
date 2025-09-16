package com.minidb.sql;

public enum TokenType {
    IDENT, NUMBER, STRING, COMMA, LPAREN, RPAREN, STAR,
    CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, WHERE,
    INT, TEXT,
    EQ, LT, GT, LE, GE, NE,          // =  <  >  <=  >=  !=
    LIKE, AND, OR, NOT, IS, NULL,    // 逻辑/空值
    DELETE, DROP, UPDATE, SET,       // DDL/DML
    SEMI, EOF
}