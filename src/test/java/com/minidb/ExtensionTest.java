package com.minidb;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.nio.file.*;
import com.minidb.catalog.*;
import com.minidb.engine.Executor;
import com.minidb.engine.Planner;
import com.minidb.sql.*;
import com.minidb.storage.*;
import com.minidb.utils.*;

public class ExtensionTest {
    
    @Test
    public void testJoinOperations() {
        Path dbDir = Paths.get("testdb_join");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // 创建测试表
        exec.exec(new Parser(new Lexer("CREATE TABLE employees(id INT, name TEXT, dept_id INT);").lex()).parseStmt());
        exec.exec(new Parser(new Lexer("CREATE TABLE departments(id INT, name TEXT);").lex()).parseStmt());
        
        // 插入测试数据
        exec.exec(new Parser(new Lexer("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1);").lex()).parseStmt());
        exec.exec(new Parser(new Lexer("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'HR');").lex()).parseStmt());
        
        // 测试INNER JOIN
        var result1 = exec.exec(new Parser(new Lexer("SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.TABLE, result1.kind);
        assertEquals(3, result1.rows.size());
        
        // 测试LEFT JOIN
        var result2 = exec.exec(new Parser(new Lexer("SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.TABLE, result2.kind);
        assertEquals(3, result2.rows.size());
    }
    
    @Test
    public void testIndexOperations() {
        Path dbDir = Paths.get("testdb_index");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // 创建测试表
        exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT, name TEXT);").lex()).parseStmt());
        exec.exec(new Parser(new Lexer("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');").lex()).parseStmt());
        
        // 创建索引
        var result1 = exec.exec(new Parser(new Lexer("CREATE INDEX idx_id ON test(id);").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, result1.kind);
        assertTrue(result1.message.contains("Index created"));
        
        // 删除索引
        var result2 = exec.exec(new Parser(new Lexer("DROP INDEX idx_id;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, result2.kind);
        assertTrue(result2.message.contains("Index dropped"));
    }
    
    @Test
    public void testTransactionOperations() {
        Path dbDir = Paths.get("testdb_transaction");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // 创建测试表
        exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT, name TEXT);").lex()).parseStmt());
        
        // 开始事务
        var result1 = exec.exec(new Parser(new Lexer("BEGIN;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, result1.kind);
        assertTrue(result1.message.contains("Transaction started"));
        
        // 插入数据
        exec.exec(new Parser(new Lexer("INSERT INTO test VALUES (1, 'Alice');").lex()).parseStmt());
        
        // 提交事务
        var result2 = exec.exec(new Parser(new Lexer("COMMIT;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, result2.kind);
        assertTrue(result2.message.contains("Transaction committed"));
        
        // 验证数据
        var result3 = exec.exec(new Parser(new Lexer("SELECT * FROM test;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.TABLE, result3.kind);
        assertEquals(1, result3.rows.size());
    }
    
    @Test
    public void testTransactionRollback() {
        Path dbDir = Paths.get("testdb_rollback");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // 创建测试表
        exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT, name TEXT);").lex()).parseStmt());
        
        // 开始事务
        exec.exec(new Parser(new Lexer("BEGIN;").lex()).parseStmt());
        
        // 插入数据
        exec.exec(new Parser(new Lexer("INSERT INTO test VALUES (1, 'Alice');").lex()).parseStmt());
        
        // 回滚事务
        var result1 = exec.exec(new Parser(new Lexer("ROLLBACK;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, result1.kind);
        assertTrue(result1.message.contains("Transaction rolled back"));
        
        // 验证数据未插入
        var result2 = exec.exec(new Parser(new Lexer("SELECT * FROM test;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.TABLE, result2.kind);
        assertEquals(0, result2.rows.size());
    }
    
    @Test
    public void testExecutionPlanFormats() {
        Path dbDir = Paths.get("testdb_plan_ext");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // 创建测试表
        exec.exec(new Parser(new Lexer("CREATE TABLE employees(id INT, name TEXT);").lex()).parseStmt());
        exec.exec(new Parser(new Lexer("CREATE TABLE departments(id INT, name TEXT);").lex()).parseStmt());
        
        // 测试JOIN查询的执行计划
        var select = new Parser(new Lexer("SELECT e.name FROM employees e JOIN departments d ON e.id = d.id;").lex()).parseStmt();
        var plan = Planner.plan((ast.Select) select);
        
        // 测试树形格式
        String tree = Planner.toTree(plan);
        assertTrue(tree.contains("Join"));
        assertTrue(tree.contains("SeqScan"));
        
        // 测试JSON格式
        String json = Planner.toJSON(plan);
        assertTrue(json.contains("\"type\": \"Join\""));
        assertTrue(json.contains("\"type\": \"SeqScan\""));
        
        // 测试S表达式格式
        String sexpr = Planner.toSExpression(plan);
        assertTrue(sexpr.contains("(Join"));
        assertTrue(sexpr.contains("(SeqScan"));
    }
}
