package com.minidb.cli;
import com.minidb.catalog.*;
import com.minidb.engine.Executor;
import com.minidb.sql.*;
import com.minidb.storage.*;
import com.minidb.utils.*;
import java.nio.file.*;
import java.util.*;
public class Main {
    public static void main(String[] args) throws Exception {
        Path dbDir = Paths.get(Constants.DB_DIR);
        Catalog catalog = new Catalog(dbDir);
        FileManager fm = new FileManager(dbDir);
        BufferPool bp = new BufferPool(64);
        Executor exec = new Executor(catalog, fm, bp);
        if (args.length>0 && args[0].equals("--cli")){
            try (Scanner sc = new Scanner(System.in)){
                System.out.println("MiniDB CLI. Type SQL and end with semicolon. Ctrl+C to exit.");
                StringBuilder sb = new StringBuilder();
                while (true){
                    System.out.print("> ");
                    String line = sc.nextLine();
                    sb.append(line).append(" ");
                    if (line.trim().endsWith(";")){
                        run(exec, sb.toString());
                        sb.setLength(0);
                    }
                }
            }
        } else {
            com.minidb.gui.Gui.launch(exec);
        }
    }
    private static void run(Executor exec, String sql){
        try {
            Lexer lx = new Lexer(sql);
            Parser ps = new Parser(lx.lex());
            var stmt = ps.parseStmt();
            var res = exec.exec(stmt);
            if (res.kind==Executor.Result.Kind.MESSAGE){
                System.out.println(res.message);
            } else {
                System.out.println(String.join("\t", res.headers));
                for (var r: res.rows){
                    System.out.println(r.toString());
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
