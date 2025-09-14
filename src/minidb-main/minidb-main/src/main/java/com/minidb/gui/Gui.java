package com.minidb.gui;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.time.LocalTime;
import java.util.List;
import java.nio.file.*;
import java.util.stream.Collectors;

import com.minidb.engine.Executor;
import com.minidb.sql.*;
import com.minidb.catalog.*;
import com.minidb.utils.*;

public class Gui {
    public static void launch(Executor executor){
        SwingUtilities.invokeLater(() -> {
            JFrame f = new JFrame("MiniDB");
            f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            f.setSize(1100, 700);

            // Fonts
            Font mono = new Font(Font.MONOSPACED, Font.PLAIN, 14);
            Font ui = new Font("SansSerif", Font.PLAIN, 13);

            // Editor
            JTextArea input = new JTextArea();
            input.setFont(mono);
            input.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));

            // Output table
            JTable table = new JTable();
            table.setFillsViewportHeight(true);
            table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

            // Log area
            JTextArea log = new JTextArea();
            log.setEditable(false);
            log.setFont(ui);
            log.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 6));

            // Catalog panel (left)
            DefaultListModel<String> listModel = new DefaultListModel<>();
            JList<String> catalogList = new JList<>(listModel);
            catalogList.setFont(ui);
            JScrollPane catalogPane = new JScrollPane(catalogList);
            catalogPane.setPreferredSize(new Dimension(220, 200));
            catalogPane.setBorder(BorderFactory.createTitledBorder("Tables"));

            // Toolbar
            JToolBar tb = new JToolBar();
            tb.setFloatable(false);
            JButton run = new JButton("Run");
            JButton clear = new JButton("Clear");
            JButton sample = new JButton("Sample");
            JToggleButton dark = new JToggleButton("Dark");
            tb.add(run); tb.add(clear); tb.add(sample); tb.addSeparator(); tb.add(dark);

            // Status bar
            JLabel status = new JLabel(" Ready");
            JPanel statusBar = new JPanel(new BorderLayout());
            statusBar.add(status, BorderLayout.WEST);
            statusBar.setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, Color.LIGHT_GRAY));

            // Split panes: left catalog | right (editor over table)
            JSplitPane vertical = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(input), new JScrollPane(table));
            vertical.setDividerLocation(260);
            JSplitPane mainSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, catalogPane, vertical);
            mainSplit.setDividerLocation(220);

            // Layout
            f.setLayout(new BorderLayout());
            f.add(tb, BorderLayout.NORTH);
            f.add(mainSplit, BorderLayout.CENTER);
            JScrollPane logPane = new JScrollPane(log);
            logPane.setPreferredSize(new Dimension(360, 100));
            f.add(logPane, BorderLayout.EAST);
            f.add(statusBar, BorderLayout.SOUTH);
            f.validate();

            // Actions
            Runnable refreshCatalog = () -> {
                listModel.clear();
                try {
                    Catalog cat = new Catalog(Paths.get(Constants.DB_DIR));
                    for (TableInfo t : cat.allTables()){
                        String cols = t.schema().columns().stream()
                                .map(c -> c.name + ":" + c.type)
                                .collect(Collectors.joining(", "));
                        listModel.addElement(t.name + "  [" + cols + "]");
                    }
                } catch(Exception ignore){}
            };

            Runnable executeBatch = () -> {
                String sql = input.getText();
                List<String> stmts = SqlBatch.splitStatements(sql);
                if (stmts.isEmpty()){ status.setText(" Nothing to run"); return; }
                int totalRowsShown = 0;
                try {
                    for (String s : stmts){
                        Lexer lx = new Lexer(s);
                        Parser ps = new Parser(lx.lex());
                        var stmt = ps.parseStmt();
                        var res = executor.exec(stmt);
                        if (res.kind == Executor.Result.Kind.MESSAGE){
                            log.append(time() + " " + res.message + "\n");
                        } else {
                            DefaultTableModel model = new DefaultTableModel();
                            for (String h : res.headers) model.addColumn(h);
                            for (var row : res.rows) model.addRow(row.toArray());
                            table.setModel(model);
                            totalRowsShown = res.rows.size();
                            log.append(time() + " Fetched " + res.rows.size() + " row(s).\n");
                        }
                    }
                    status.setText(" OK • Executed " + stmts.size() + " statement(s), last result rows=" + totalRowsShown);
                } catch(Exception ex){
                    log.append(time() + " Error: " + ex.getMessage() + "\n");
                    status.setText(" Error");
                }
                refreshCatalog.run();
            };

            run.addActionListener(e -> executeBatch.run());
            clear.addActionListener(e -> { input.setText(""); log.setText(""); status.setText(" Cleared"); });
            sample.addActionListener(e -> input.setText(
                    "CREATE TABLE products(id INT, name TEXT, price INT);\n" +
                            "INSERT INTO products VALUES (1,'Apple',3),(2,'Banana',2),(3,'Carrot',5),(4,'Donut',4);\n" +
                            "SELECT * FROM products;\n" +
                            "SELECT name, price FROM products WHERE price > 3;\n"
            ));

            // Dark mode toggle
            dark.addActionListener(e -> {
                boolean on = dark.isSelected();
                Color bg = on ? new Color(0x1e1e1e) : Color.WHITE;
                Color fg = on ? new Color(0xdddddd) : Color.BLACK;
                input.setBackground(bg); input.setForeground(fg);
                log.setBackground(bg); log.setForeground(fg);
                table.setBackground(on ? new Color(0x252526) : Color.WHITE);
                table.setForeground(fg);
                statusBar.setBackground(on ? new Color(0x2d2d30) : UIManager.getColor("Panel.background"));
                catalogPane.setBackground(bg); catalogList.setBackground(bg); catalogList.setForeground(fg);
            });

            refreshCatalog.run();
            f.setVisible(true);
        });
    }

    private static String time(){
        return "[" + LocalTime.now().withNano(0) + "]";
    }
}
