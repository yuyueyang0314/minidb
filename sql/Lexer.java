package com.minidb.sql;

import java.util.*;

public class Lexer {
    private final String s; private int i=0;
    public Lexer(String s){ this.s=s; }

    private boolean eof(){ return i>=s.length(); }
    private char peek(){ return s.charAt(i); }
    private void skipws(){ while(!eof() && Character.isWhitespace(peek())) i++; }

    public List<Token> lex(){
        List<Token> out = new ArrayList<>();
        while(true){
            skipws();
            if (eof()){ out.add(new Token(TokenType.EOF,"")); break; }
            char c = peek();

            if (Character.isJavaIdentifierStart(c)){
                int j=i++; while(!eof() && Character.isJavaIdentifierPart(peek())) i++;
                String t = s.substring(j,i);
                String u = t.toUpperCase();
                switch(u){
                    case "CREATE" -> out.add(new Token(TokenType.CREATE,t));
                    case "TABLE"  -> out.add(new Token(TokenType.TABLE,t));
                    case "INSERT" -> out.add(new Token(TokenType.INSERT,t));
                    case "INTO"   -> out.add(new Token(TokenType.INTO,t));
                    case "VALUES" -> out.add(new Token(TokenType.VALUES,t));
                    case "SELECT" -> out.add(new Token(TokenType.SELECT,t));
                    case "FROM"   -> out.add(new Token(TokenType.FROM,t));
                    case "WHERE"  -> out.add(new Token(TokenType.WHERE,t));
                    case "INT"    -> out.add(new Token(TokenType.INT,t));
                    case "TEXT"   -> out.add(new Token(TokenType.TEXT,t));
                    case "LIKE"   -> out.add(new Token(TokenType.LIKE,t));
                    case "AND"    -> out.add(new Token(TokenType.AND,t));
                    case "OR"     -> out.add(new Token(TokenType.OR,t));
                    case "NOT"    -> out.add(new Token(TokenType.NOT,t));
                    case "IS"     -> out.add(new Token(TokenType.IS,t));
                    case "NULL"   -> out.add(new Token(TokenType.NULL,t));
                    case "DELETE" -> out.add(new Token(TokenType.DELETE,t));
                    case "DROP"   -> out.add(new Token(TokenType.DROP,t));
                    case "UPDATE" -> out.add(new Token(TokenType.UPDATE,t));
                    case "SET"    -> out.add(new Token(TokenType.SET,t));
                    default       -> out.add(new Token(TokenType.IDENT,t));
                }
            } else if (Character.isDigit(c)){
                int j=i++; while(!eof() && Character.isDigit(peek())) i++;
                out.add(new Token(TokenType.NUMBER, s.substring(j,i)));
            } else if (c=='\''){
                int j=++i; while(!eof() && peek()!='\'') i++;
                out.add(new Token(TokenType.STRING, s.substring(j,i))); i++;
            } else {
                i++;
                switch(c){
                    case '=' -> out.add(new Token(TokenType.EQ,"="));
                    case '<' -> {
                        if (!eof() && peek()=='='){ i++; out.add(new Token(TokenType.LE,"<=")); }
                        else out.add(new Token(TokenType.LT,"<"));
                    }
                    case '>' -> {
                        if (!eof() && peek()=='='){ i++; out.add(new Token(TokenType.GE,">=")); }
                        else out.add(new Token(TokenType.GT,">"));
                    }
                    case '!' -> {
                        if (!eof() && peek()=='='){ i++; out.add(new Token(TokenType.NE,"!=")); }
                        else throw new RuntimeException("Unexpected '!'");
                    }
                    case ',' -> out.add(new Token(TokenType.COMMA, ","));
                    case '(' -> out.add(new Token(TokenType.LPAREN, "("));
                    case ')' -> out.add(new Token(TokenType.RPAREN, ")"));
                    case '*' -> out.add(new Token(TokenType.STAR, "*"));
                    case ';' -> out.add(new Token(TokenType.SEMI, ";"));
                    default  -> throw new RuntimeException("Unknown char: "+c);
                }
            }
        }
        return out;
    }
}
