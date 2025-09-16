package com.minidb.sql;

import com.minidb.catalog.*;
import com.minidb.utils.*;
import java.util.*;

public class SemanticAnalyzer {
    private final Catalog catalog;
    
    public SemanticAnalyzer(Catalog catalog) {
        this.catalog = catalog;
    }
    
    public void analyze(ast.Stmt stmt) {
        if (stmt instanceof ast.CreateTable ct) analyzeCreateTable(ct);
        else if (stmt instanceof ast.Insert ins) analyzeInsert(ins);
        else if (stmt instanceof ast.Select sel) analyzeSelect(sel);
        else if (stmt instanceof ast.Delete del) analyzeDelete(del);
        else if (stmt instanceof ast.Update upd) analyzeUpdate(upd);
        else if (stmt instanceof ast.DropTable dt) analyzeDropTable(dt);
    }
    
    private void analyzeCreateTable(ast.CreateTable ct) {
        // Check if table already exists
        try {
            catalog.getTable(ct.name);
            throw new SemanticException("Table already exists", ct.name, 0, 0);
        } catch (DBException e) {
            // Table doesn't exist, which is what we want
        }
        
        // Check column names are unique
        Set<String> columnNames = new HashSet<>();
        for (var col : ct.cols) {
            if (!columnNames.add(col.name.toLowerCase())) {
                throw new SemanticException("Duplicate column name", col.name, 0, 0);
            }
        }
    }
    
    private void analyzeInsert(ast.Insert ins) {
        TableInfo table = getTableOrThrow(ins.table, 0, 0);
        
        // Check column count matches
        for (var row : ins.rows) {
            if (row.size() != table.schema.size()) {
                throw new SemanticException(
                    String.format("Column count mismatch: expected %d, got %d", 
                        table.schema.size(), row.size()),
                    ins.table, 0, 0
                );
            }
        }
        
        // Check type consistency
        for (var row : ins.rows) {
            for (int i = 0; i < row.size(); i++) {
                Object value = row.get(i);
                Column.Type expectedType = table.schema.get(i).type;
                if (value != null && !isTypeCompatible(value, expectedType)) {
                    throw new SemanticException(
                        String.format("Type mismatch: column %s expects %s, got %s",
                            table.schema.get(i).name, expectedType, getValueType(value)),
                        ins.table, 0, 0
                    );
                }
            }
        }
    }
    
    private void analyzeSelect(ast.Select sel) {
        TableInfo table = getTableOrThrow(sel.table, 0, 0);
        
        // Check column references
        if (!sel.cols.contains("*")) {
            for (String colName : sel.cols) {
                if (table.schema.indexOf(colName).isEmpty()) {
                    throw new SemanticException("Unknown column", colName, 0, 0);
                }
            }
        }
        
        // Analyze WHERE clause
        if (sel.where != null) {
            analyzeExpression(sel.where, table.schema);
        }
    }
    
    private void analyzeDelete(ast.Delete del) {
        TableInfo table = getTableOrThrow(del.table, 0, 0);
        
        // Analyze WHERE clause
        if (del.where != null) {
            analyzeExpression(del.where, table.schema);
        }
    }
    
    private void analyzeUpdate(ast.Update upd) {
        TableInfo table = getTableOrThrow(upd.table, 0, 0);
        
        // Check column references in SET clauses
        for (var setClause : upd.sets) {
            if (table.schema.indexOf(setClause.col).isEmpty()) {
                throw new SemanticException("Unknown column", setClause.col, 0, 0);
            }
        }
        
        // Analyze WHERE clause
        if (upd.where != null) {
            analyzeExpression(upd.where, table.schema);
        }
    }
    
    private void analyzeDropTable(ast.DropTable dt) {
        getTableOrThrow(dt.table, 0, 0); // Just check if table exists
    }
    
    private void analyzeExpression(ast.Expr expr, Schema schema) {
        if (expr instanceof ast.ColRef cr) {
            if (schema.indexOf(cr.name).isEmpty()) {
                throw new SemanticException("Unknown column", cr.name, 0, 0);
            }
        } else if (expr instanceof ast.Compare cmp) {
            analyzeExpression(cmp.left, schema);
            analyzeExpression(cmp.right, schema);
        } else if (expr instanceof ast.And and) {
            analyzeExpression(and.l, schema);
            analyzeExpression(and.r, schema);
        } else if (expr instanceof ast.Or or) {
            analyzeExpression(or.l, schema);
            analyzeExpression(or.r, schema);
        } else if (expr instanceof ast.Not not) {
            analyzeExpression(not.e, schema);
        } else if (expr instanceof ast.Like like) {
            analyzeExpression(like.l, schema);
            analyzeExpression(like.r, schema);
        } else if (expr instanceof ast.IsNull isNull) {
            analyzeExpression(isNull.e, schema);
        } else if (expr instanceof ast.IsNotNull isNotNull) {
            analyzeExpression(isNotNull.e, schema);
        }
        // Literal expressions don't need analysis
    }
    
    private TableInfo getTableOrThrow(String tableName, int line, int column) {
        try {
            return catalog.getTable(tableName);
        } catch (DBException e) {
            throw new SemanticException("Table does not exist", tableName, line, column);
        }
    }
    
    private boolean isTypeCompatible(Object value, Column.Type expectedType) {
        if (value == null) return true;
        
        switch (expectedType) {
            case INT:
                return value instanceof Number;
            case TEXT:
                return true; // Everything can be converted to text
            default:
                return false;
        }
    }
    
    private String getValueType(Object value) {
        if (value == null) return "NULL";
        if (value instanceof Number) return "INT";
        return "TEXT";
    }
    
    public static class SemanticException extends RuntimeException {
        private final String column;
        private final int line;
        private final int columnPos;
        
        public SemanticException(String message, String column, int line, int columnPos) {
            super(String.format("Semantic error: %s [%s] at line %d, column %d", 
                message, column, line, columnPos));
            this.column = column;
            this.line = line;
            this.columnPos = columnPos;
        }
        
        public String getColumn() { return column; }
        public int getLine() { return line; }
        public int getColumnPos() { return columnPos; }
    }
}
