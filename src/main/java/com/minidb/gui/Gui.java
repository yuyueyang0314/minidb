package com.minidb.gui;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import com.minidb.engine.Executor;
import com.minidb.sql.*;
public class Gui {
    public static void launch(Executor executor){
        SwingUtilities.invokeLater(()->{
            JFrame f = new JFrame("MiniDB");
            f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            f.setSize(900,600);
            JTextArea input = new JTextArea();
            JButton run = new JButton("Execute");
            JTable table = new JTable();
            JTextArea log = new JTextArea(); log.setEditable(false);
            JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(input), new JScrollPane(table));
            split.setDividerLocation(200);
            f.setLayout(new BorderLayout());
            f.add(split, BorderLayout.CENTER);
            JPanel south = new JPanel(new BorderLayout());
            south.add(run, BorderLayout.WEST);
            south.add(new JScrollPane(log), BorderLayout.CENTER);
            f.add(south, BorderLayout.SOUTH);
            run.addActionListener(e->{
                String sql = input.getText();
                try {
                    Lexer lx = new Lexer(sql);
                    Parser ps = new Parser(lx.lex());
                    var stmt = ps.parseStmt();
                    var res = executor.exec(stmt);
                    if (res.kind==Executor.Result.Kind.MESSAGE){
                        log.append(res.message+" ");
                        table.setModel(new DefaultTableModel());
                    } else {
                        DefaultTableModel model = new DefaultTableModel();
                        for (String h: res.headers) model.addColumn(h);
                        for (var row: res.rows) model.addRow(row.toArray());
                        table.setModel(model);
                        log.append("Fetched "+res.rows.size()+" row(s). ");
                    }
                } catch(Exception ex){
                    log.append("Error: "+ex.getMessage()+" ");
                }
            });
            f.setVisible(true);
        });
    }
}
