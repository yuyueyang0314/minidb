package com.minidb.sql;
import java.util.*;
import static com.minidb.sql.TokenType.*;
public class Parser {
    private final List<Token> t; private int i=0;
    public Parser(List<Token> t){ this.t=t; }
    private Token la(){ return t.get(i); }
    private boolean eat(TokenType ty){ if (la().type()==ty){ i++; return true; } return false; }
    private Token req(TokenType ty){ Token x=la(); if (x.type()!=ty) throw new RuntimeException("Expected "+ty+" but got "+x.type()); i++; return x; }
    public ast.Stmt parseStmt(){
        TokenType k = la().type();
        if (k==CREATE) return parseCreate();
        if (k==INSERT) return parseInsert();
        if (k==SELECT) return parseSelect();
        throw new RuntimeException("Unknown start token: "+k);
    }
    private ast.CreateTable parseCreate(){
        req(CREATE); req(TABLE);
        String name = req(IDENT).text();
        req(LPAREN);
        List<ast.CreateTable.Col> cols = new ArrayList<>();
        do {
            String cn = req(IDENT).text();
            Token tt = la();
            if (tt.type()!=INT && tt.type()!=TEXT) throw new RuntimeException("Type expected");
            i++;
            cols.add(new ast.CreateTable.Col(cn, tt.type().name()));
        } while (eat(COMMA));
        req(RPAREN); eat(SEMI);
        return new ast.CreateTable(name, cols);
    }
    private ast.Insert parseInsert(){
        req(INSERT); req(INTO);
        String name = req(IDENT).text();
        req(VALUES);
        List<List<Object>> rows = new ArrayList<>();
        do {
            req(LPAREN);
            List<Object> vs = new ArrayList<>();
            do {
                Token x = la(); i++;
                if (x.type()==NUMBER) vs.add(Integer.parseInt(x.text()));
                else if (x.type()==STRING) vs.add(x.text());
                else throw new RuntimeException("literal expected");
            } while (eat(COMMA));
            req(RPAREN);
            rows.add(vs);
        } while (eat(COMMA));
        eat(SEMI);
        return new ast.Insert(name, rows);
    }
    private ast.Select parseSelect(){
        req(SELECT);
        List<String> cols = new ArrayList<>();
        if (eat(STAR)){ cols.add("*"); }
        else {
            do { cols.add(req(IDENT).text()); } while (eat(COMMA));
        }
        req(FROM);
        String table = req(IDENT).text();
        ast.Cond cond = null;
        if (eat(WHERE)){
            String c = req(IDENT).text();
            Token op = la(); i++;
            if (op.type()!=EQ && op.type()!=LT && op.type()!=GT) throw new RuntimeException("op");
            Token lit = la(); i++;
            Object v = switch(lit.type()){
                case NUMBER -> Integer.parseInt(lit.text());
                case STRING -> lit.text();
                default -> throw new RuntimeException("literal");
            };
            cond = new ast.Cond(c, op.text(), v);
        }
        eat(SEMI);
        return new ast.Select(table, cols, cond);
    }
}
