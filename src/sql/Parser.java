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
        if (k==DELETE) return parseDelete();
        if (k==DROP)   return parseDrop();
        if (k==UPDATE) return parseUpdate();
        throw new RuntimeException("Unknown start token: "+k);
    }

    private ast.CreateTable parseCreate(){
        req(CREATE); req(TABLE);
        String name = req(IDENT).text();
        req(LPAREN);
        List<ast.CreateTable.Col> cols = new ArrayList<>();
        do {
            String cn = req(IDENT).text();
            Token tt = la(); if (tt.type()!=INT && tt.type()!=TEXT) throw new RuntimeException("Type expected");
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
        if (eat(STAR)) cols.add("*"); else { do { cols.add(req(IDENT).text()); } while (eat(COMMA)); }
        req(FROM);
        String table = req(IDENT).text();
        ast.Expr cond = null;
        if (eat(WHERE)) cond = parseOr();
        eat(SEMI);
        return new ast.Select(table, cols, cond);
    }

    private ast.Delete parseDelete(){
        req(DELETE); req(FROM);
        String table = req(IDENT).text();
        ast.Expr cond = null;
        if (eat(WHERE)) cond = parseOr();
        eat(SEMI);
        return new ast.Delete(table, cond);
    }

    private ast.DropTable parseDrop(){
        req(DROP); req(TABLE);
        String name = req(IDENT).text();
        eat(SEMI);
        return new ast.DropTable(name);
    }

    private ast.Update parseUpdate(){
        req(UPDATE);
        String table = req(IDENT).text();
        req(SET);
        List<ast.Update.SetClause> sets = new ArrayList<>();
        do {
            String col = req(IDENT).text();
            req(EQ);
            ast.Expr e = parseOr();
            sets.add(new ast.Update.SetClause(col, e));
        } while (eat(COMMA));
        ast.Expr where = null;
        if (eat(WHERE)) where = parseOr();
        eat(SEMI);
        return new ast.Update(table, sets, where);
    }

    // --------- expression precedence: OR > AND > NOT > cmp ----------
    private ast.Expr parseOr(){
        ast.Expr e = parseAnd();
        while (eat(OR)) e = new ast.Or(e, parseAnd());
        return e;
    }
    private ast.Expr parseAnd(){
        ast.Expr e = parseNot();
        while (eat(AND)) e = new ast.And(e, parseNot());
        return e;
    }
    private ast.Expr parseNot(){
        if (eat(NOT)) return new ast.Not(parseNot());
        return parseCmp();
    }
    private ast.Expr parseCmp(){
        ast.Expr left = parsePrimary();
        Token op = la();
        if (op.type()==EQ || op.type()==LT || op.type()==GT || op.type()==LE || op.type()==GE || op.type()==NE || op.type()==LIKE){
            i++;
            ast.Expr right = parsePrimary();
            if (op.type()==LIKE) return new ast.Like(left, right);
            return new ast.Compare(op.text(), left, right);
        } else if (op.type()==IS){
            i++;
            if (eat(NULL)) return new ast.IsNull(left);
            if (eat(NOT) && eat(NULL)) return new ast.IsNotNull(left);
            throw new RuntimeException("Expected NULL or NOT NULL after IS");
        }
        return left;
    }
    private ast.Expr parsePrimary(){
        Token x = la(); i++;
        return switch(x.type()){
            case IDENT -> new ast.ColRef(x.text());
            case NUMBER -> new ast.Literal(Integer.parseInt(x.text()));
            case STRING -> new ast.Literal(x.text());
            case LPAREN -> { ast.Expr e = parseOr(); req(RPAREN); yield e; }
            default -> throw new RuntimeException("bad expr token: "+x.type());
        };
    }
}
