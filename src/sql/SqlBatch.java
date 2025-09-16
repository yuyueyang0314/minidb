package com.minidb.sql;
import java.util.*;

/** Split SQL text into individual statements by ';', ignoring semicolons inside single quotes. */
public final class SqlBatch {
    private SqlBatch(){}

    public static List<String> splitStatements(String sql){
        ArrayList<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inStr = false;
        for (int i = 0; i < sql.length(); i++){
            char c = sql.charAt(i);
            if (c == '\''){
                inStr = !inStr;
                sb.append(c);
            } else if (c == ';' && !inStr){
                String s = sb.toString().trim();
                if (!s.isEmpty()) out.add(s + ";");
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        String s = sb.toString().trim();
        if (!s.isEmpty()) out.add(s + (s.endsWith(";") ? "" : ";"));
        return out;
    }
}
