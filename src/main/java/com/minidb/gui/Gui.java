package com.minidb.gui;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import com.minidb.engine.Executor;
import com.minidb.sql.*;
import com.minidb.catalog.*;
import com.minidb.storage.*;
import com.minidb.utils.*;
import java.nio.file.*;
import java.time.LocalTime;
import java.util.List;

public class Gui {
    public static void launch(Executor executor, BufferPool bp, FileManager fm) {
        SwingUtilities.invokeLater(() -> {
            // Frame Setup
            JFrame f = new JFrame("MiniDB");
            f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            f.setSize(1150, 720);

            // Fonts
            Font mono = new Font(Font.MONOSPACED, Font.PLAIN, 14);
            Font ui = new Font("SansSerif", Font.PLAIN, 13);

            // Editor
            JTextArea input = new JTextArea();
            input.setFont(mono);
            input.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));

            // Tabs for results
            JTabbedPane results = new JTabbedPane();

            // Log area (Initially hidden)
            JTextArea log = new JTextArea();
            log.setEditable(false);
            log.setFont(ui);
            JScrollPane logPane = new JScrollPane(log);
            logPane.setPreferredSize(new Dimension(360, 100));
            logPane.setVisible(false);  // Initially hide log area

            // Toolbar
            JToolBar tb = new JToolBar();
            tb.setFloatable(false);
            JButton run = new JButton("Run");
            JButton clear = new JButton("Clear");
            JButton sample = new JButton("Sample");
            JButton stats = new JButton("Stats");
            JButton toggleLog = new JButton("Toggle Log");  // Button to toggle log visibility
            JToggleButton dark = new JToggleButton("Dark");
            tb.add(run);
            tb.add(clear);
            tb.add(sample);
            tb.add(stats);
            tb.add(toggleLog);  // Add toggle log button
            tb.addSeparator();
            tb.add(dark);

            // Status
            JLabel status = new JLabel(" Ready");
            JPanel statusBar = new JPanel(new BorderLayout());
            statusBar.add(status, BorderLayout.WEST);
            statusBar.setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, Color.LIGHT_GRAY));

            // Left catalog
            DefaultListModel<String> listModel = new DefaultListModel<>();
            JList<String> catalogList = new JList<>(listModel);
            catalogList.setFont(ui);
            JScrollPane catalogPane = new JScrollPane(catalogList);
            catalogPane.setPreferredSize(new Dimension(220, 200));
            catalogPane.setBorder(BorderFactory.createTitledBorder("Tables"));

            // Split panes
            JSplitPane v = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(input), results);
            v.setDividerLocation(260);
            JSplitPane main = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, catalogPane, v);
            main.setDividerLocation(220);

            f.setLayout(new BorderLayout());
            f.add(tb, BorderLayout.NORTH);
            f.add(main, BorderLayout.CENTER);
            f.add(logPane, BorderLayout.EAST); // Place logPane on the right
            f.add(statusBar, BorderLayout.SOUTH);

            // Actions
            Runnable refreshCatalog = () -> {
                listModel.clear();
                Catalog cat = new Catalog(Paths.get(Constants.DB_DIR));
                for (TableInfo t : cat.allTables()) listModel.addElement(t.name);
            };

            run.addActionListener(e -> {
                // Clear previous results
                results.removeAll();
                String sql = input.getText();
                List<String> stmts = SqlBatch.splitStatements(sql);
                if (stmts.isEmpty()) {
                    status.setText(" Nothing to run");
                    return;
                }
                int tabNo = results.getTabCount();
                for (String s : stmts) {
                    try {
                        Lexer lx = new Lexer(s);
                        Parser ps = new Parser(lx.lex());
                        var stmt = ps.parseStmt();
                        var res = executor.exec(stmt);
                        if (res.kind == Executor.Result.Kind.MESSAGE) {
                            log.append("[" + LocalTime.now().withNano(0) + "] " + res.message + "\n");
                        } else {
                            DefaultTableModel model = new DefaultTableModel();
                            for (String h : res.headers) model.addColumn(h);
                            for (var row : res.rows) model.addRow(row.toArray());
                            JTable table = new JTable(model);
                            results.addTab("Result " + (++tabNo), new JScrollPane(table));
                            results.setSelectedIndex(results.getTabCount() - 1);
                            log.append("[" + LocalTime.now().withNano(0) + "] Fetched " + res.rows.size() + " row(s).\n");
                        }
                    } catch (Exception ex) {
                        log.append("[" + LocalTime.now().withNano(0) + "] Error: " + ex.getMessage() + "\n");
                    }
                }
                status.setText(" OK • Executed " + stmts.size() + " statement(s)");
                refreshCatalog.run();
            });

            clear.addActionListener(e -> {
                input.setText("");
                results.removeAll();
                log.setText("");
                status.setText(" Cleared");
            });
            sample.addActionListener(e -> input.setText(
                    "CREATE TABLE emp(id INT, name TEXT, dept TEXT);\n" +
                            "INSERT INTO emp VALUES (1,'Ann','ENG'),(2,'Bob','ENG'),(3,'Cara','HR'),(4,'Dan','FIN');\n" +
                            "SELECT * FROM emp;\n" +
                            "SELECT name FROM emp WHERE NOT (dept = 'HR') AND id >= 2;\n" +
                            "UPDATE emp SET dept = 'ENG' WHERE name != 'Ann' AND dept IS NOT NULL;\n" +
                            "DELETE FROM emp WHERE dept IS NULL OR name LIKE 'A';\n"));
            stats.addActionListener(e -> {
                String msg = String.format(
                        "Cache policy: %s\nCache size: %d\nData dir: %s",
                        bp.policy(), bp.size(), Constants.DB_DIR);
                JOptionPane.showMessageDialog(f, msg, "Runtime Stats", JOptionPane.INFORMATION_MESSAGE);
            });
            dark.addActionListener(e -> {
                boolean on = dark.isSelected();
                Color bg = on ? new Color(0x1e1e1e) : Color.WHITE;
                Color fg = on ? new Color(0xdddddd) : Color.BLACK;
                input.setBackground(bg);
                input.setForeground(fg);
                log.setBackground(bg);
                log.setForeground(fg);
                catalogList.setBackground(bg);
                catalogList.setForeground(fg);
                statusBar.setBackground(on ? new Color(0x2d2d30) : UIManager.getColor("Panel.background"));
            });

            // Toggle log visibility
            toggleLog.addActionListener(e -> {
                boolean isLogVisible = logPane.isVisible();
                logPane.setVisible(!isLogVisible);  // Toggle visibility
                f.revalidate();
                f.repaint();
            });

            // 双击表名：插入 SELECT *
            catalogList.addMouseListener(new java.awt.event.MouseAdapter() {
                public void mouseClicked(java.awt.event.MouseEvent e) {
                    if (e.getClickCount() == 2) {
                        String t = catalogList.getSelectedValue();
                        if (t != null) {
                            input.insert("SELECT * FROM " + t + ";\n", input.getCaretPosition());
                        }
                    }
                }
            });

            refreshCatalog.run();
            f.setVisible(true);
        });
    }
}