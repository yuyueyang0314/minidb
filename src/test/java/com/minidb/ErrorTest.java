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

public class ErrorTest {
    
    @Test
    public void testLexicalErrors() {
        // Test unterminated string
        assertThrows(LexerException.class, () -> {
            new Lexer("SELECT 'unterminated string").lex();
        });
        
        // Test unknown character
        assertThrows(LexerException.class, () -> {
            new Lexer("SELECT @ unknown").lex();
        });
    }
    
    @Test
    public void testSyntaxErrors() {
        // Test missing semicolon
        assertThrows(ParserException.class, () -> {
            new Parser(new Lexer("CREATE TABLE test(id INT)").lex()).parseStmt();
        });
        
        // Test missing closing parenthesis
        assertThrows(ParserException.class, () -> {
            new Parser(new Lexer("CREATE TABLE test(id INT").lex()).parseStmt();
        });
        
        // Test invalid type
        assertThrows(ParserException.class, () -> {
            new Parser(new Lexer("CREATE TABLE test(id FLOAT);").lex()).parseStmt();
        });
    }
    
    @Test
    public void testSemanticErrors() {
        Path dbDir = Paths.get("testdb_errors");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // Create a test table first
        exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT, name TEXT);").lex()).parseStmt());
        
        // Test table already exists
        var result1 = exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT);").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.ERROR, result1.kind);
        assertTrue(result1.message.contains("already exists"));
        
        // Test unknown table
        var result2 = exec.exec(new Parser(new Lexer("SELECT * FROM unknown_table;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.ERROR, result2.kind);
        assertTrue(result2.message.contains("does not exist"));
        
        // Test unknown column
        var result3 = exec.exec(new Parser(new Lexer("SELECT unknown_col FROM test;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.ERROR, result3.kind);
        assertTrue(result3.message.contains("Unknown column"));
        
        // Test column count mismatch
        var result4 = exec.exec(new Parser(new Lexer("INSERT INTO test VALUES (1);").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.ERROR, result4.kind);
        assertTrue(result4.message.contains("Column count mismatch"));
        
        // Test type mismatch
        var result5 = exec.exec(new Parser(new Lexer("INSERT INTO test VALUES ('text', 123);").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.ERROR, result5.kind);
        assertTrue(result5.message.contains("Type mismatch"));
    }
    
    @Test
    public void testExecutionPlanFormats() {
        Path dbDir = Paths.get("testdb_plan");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        
        // Create test table and data
        exec.exec(new Parser(new Lexer("CREATE TABLE test(id INT, name TEXT);").lex()).parseStmt());
        exec.exec(new Parser(new Lexer("INSERT INTO test VALUES (1, 'test');").lex()).parseStmt());
        
        // Test different plan formats
        var select = new Parser(new Lexer("SELECT name FROM test WHERE id > 0;").lex()).parseStmt();
        var plan = Planner.plan((ast.Select) select);
        
        // Test tree format
        String tree = Planner.toTree(plan);
        assertTrue(tree.contains("Project"));
        assertTrue(tree.contains("Filter"));
        assertTrue(tree.contains("SeqScan"));
        
        // Test JSON format
        String json = Planner.toJSON(plan);
        assertTrue(json.contains("\"type\": \"Project\""));
        assertTrue(json.contains("\"type\": \"Filter\""));
        assertTrue(json.contains("\"type\": \"SeqScan\""));
        
        // Test S-expression format
        String sexpr = Planner.toSExpression(plan);
        assertTrue(sexpr.contains("(Project"));
        assertTrue(sexpr.contains("(Filter"));
        assertTrue(sexpr.contains("(SeqScan"));
    }
}
