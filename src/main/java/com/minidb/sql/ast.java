package com.minidb.sql;
import java.util.*;

public class ast {
    public interface Stmt {}
    public interface Expr {}

    // ----- DDL/DML -----
    public static class CreateTable implements Stmt {
        public final String name; public final List<Col> cols;
        public CreateTable(String name, List<Col> cols){ this.name=name; this.cols=cols; }
        public static class Col { public final String name,type; public Col(String n,String t){ name=n; type=t; } }
    }
    public static class DropTable implements Stmt {
        public final String table; public DropTable(String table){ this.table=table; }
    }
    public static class Insert implements Stmt {
        public final String table; public final List<List<Object>> rows;
        public Insert(String table, List<List<Object>> rows){ this.table=table; this.rows=rows; }
    }
    public static class Update implements Stmt {
        public final String table; public final List<SetClause> sets; public final Expr where;
        public Update(String table, List<SetClause> sets, Expr where){ this.table=table; this.sets=sets; this.where=where; }
        public static class SetClause { public final String col; public final Expr expr; public SetClause(String c, Expr e){ col=c; expr=e; } }
    }
    public static class Delete implements Stmt {
        public final String table; public final Expr where;
        public Delete(String table, Expr where){ this.table=table; this.where=where; }
    }
    public static class Select implements Stmt {
        public final String table; public final List<String> cols; public final Expr where;
        public final List<JoinClause> joins;
        public Select(String table, List<String> cols, Expr where, List<JoinClause> joins){ 
            this.table=table; this.cols=cols; this.where=where; this.joins=joins; 
        }
        public Select(String table, List<String> cols, Expr where){ 
            this(table, cols, where, List.of()); 
        }
    }
    
    public static class JoinClause {
        public enum Type { INNER, LEFT, RIGHT, FULL }
        public final Type type;
        public final String table;
        public final Expr condition;
        public JoinClause(Type type, String table, Expr condition) {
            this.type = type; this.table = table; this.condition = condition;
        }
    }
    
    public static class CreateIndex implements Stmt {
        public final String indexName;
        public final String tableName;
        public final String columnName;
        public CreateIndex(String indexName, String tableName, String columnName) {
            this.indexName = indexName; this.tableName = tableName; this.columnName = columnName;
        }
    }
    
    public static class DropIndex implements Stmt {
        public final String indexName;
        public DropIndex(String indexName) { this.indexName = indexName; }
    }
    
    public static class BeginTransaction implements Stmt {
        public BeginTransaction() {}
    }
    
    public static class CommitTransaction implements Stmt {
        public CommitTransaction() {}
    }
    
    public static class RollbackTransaction implements Stmt {
        public RollbackTransaction() {}
    }

    // ----- Expr -----
    public static class ColRef implements Expr { public final String name; public ColRef(String n){ name=n; } }
    public static class Literal implements Expr { public final Object v; public Literal(Object v){ this.v=v; } }
    public static class Compare implements Expr { public final String op; public final Expr left,right; public Compare(String op, Expr l, Expr r){ this.op=op; this.left=l; this.right=r; } }
    public static class And implements Expr { public final Expr l,r; public And(Expr l, Expr r){ this.l=l; this.r=r; } }
    public static class Or  implements Expr { public final Expr l,r; public Or (Expr l, Expr r){ this.l=l; this.r=r; } }
    public static class Not implements Expr { public final Expr e; public Not(Expr e){ this.e=e; } }
    public static class Like implements Expr { public final Expr l,r; public Like(Expr l, Expr r){ this.l=l; this.r=r; } }
    public static class IsNull implements Expr { public final Expr e; public IsNull(Expr e){ this.e=e; } }
    public static class IsNotNull implements Expr { public final Expr e; public IsNotNull(Expr e){ this.e=e; } }
}
