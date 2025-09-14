package com.minidb;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.nio.file.*;
import com.minidb.catalog.*;
import com.minidb.engine.Executor;
import com.minidb.sql.*;
import com.minidb.storage.*;
import com.minidb.utils.*;
public class BasicTest {
    @Test
    public void endToEnd() {
        Path dbDir = Paths.get("testdb");
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(8);
        Executor exec = new Executor(catalog, fm, bp);
        var res1 = exec.exec(new Parser(new Lexer("CREATE TABLE t(id INT, name TEXT);").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, res1.kind);
        var res2 = exec.exec(new Parser(new Lexer("INSERT INTO t VALUES (1,'a'),(2,'b');").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.MESSAGE, res2.kind);
        var res3 = exec.exec(new Parser(new Lexer("SELECT id,name FROM t WHERE id > 1;").lex()).parseStmt());
        assertEquals(Executor.Result.Kind.TABLE, res3.kind);
        assertEquals(1, res3.rows.size());
    }
}
