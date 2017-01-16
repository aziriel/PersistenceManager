/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package persistence.sql;

import com.sun.media.jfxmedia.logging.Logger;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import persistence.DataClass;
import persistence.PersistenceManager;

/**
 *
 * @author Jens Baetens @ JBProductions
 */
public class SqlManager extends PersistenceManager {

    protected Connection m_conn;

    protected static final HashMap<Class, String> java_sql_type_mapping;

    static {
        java_sql_type_mapping = new HashMap<>();
        java_sql_type_mapping.put(String.class, "VARCHAR");
        java_sql_type_mapping.put(java.math.BigDecimal.class, "NUMERIC");
        java_sql_type_mapping.put(boolean.class, "BIT");
        java_sql_type_mapping.put(byte.class, "TINYINT");
        java_sql_type_mapping.put(short.class, "SMALLINT");
        java_sql_type_mapping.put(int.class, "INTEGER");
        java_sql_type_mapping.put(long.class, "BIGINT");
        java_sql_type_mapping.put(float.class, "REAL");
        java_sql_type_mapping.put(double.class, "DOUBLE");
        java_sql_type_mapping.put(byte[].class, "VARBINARY");
        java_sql_type_mapping.put(java.sql.Date.class, "DATE");
        java_sql_type_mapping.put(java.sql.Time.class, "TIME");
        java_sql_type_mapping.put(java.sql.Timestamp.class, "TIMESTAMP");
    }

    protected static final HashMap<String, Class> sql_java_type_mapping;

    static {
        sql_java_type_mapping = new HashMap<>();
        sql_java_type_mapping.put("VARCHAR", String.class);
        sql_java_type_mapping.put("NUMERIC", java.math.BigDecimal.class);
        sql_java_type_mapping.put("BIT", boolean.class);
        sql_java_type_mapping.put("TINYINT", byte.class);
        sql_java_type_mapping.put("SMALLINT", short.class);
        sql_java_type_mapping.put("INTEGER", int.class);
        sql_java_type_mapping.put("BIGINT", long.class);
        sql_java_type_mapping.put("REAL", float.class);
        sql_java_type_mapping.put("DOUBLE", double.class);
        sql_java_type_mapping.put("VARBINARY", byte[].class);
        sql_java_type_mapping.put("DATE", java.sql.Date.class);
        sql_java_type_mapping.put("TIME", java.sql.Time.class);
        sql_java_type_mapping.put("TIMESTAMP", java.sql.Timestamp.class);
    }

    public SqlManager(String filename) {
        super(filename);

        //check if file exists
        File f = new File(filename);
        if (f.exists() && !f.isDirectory()) {
            // load existing database
            loadDatabase();
        } else {
            // create new database
            createNewDatabase();
        }
    }

    @Override
    public void cleanup() {
        try {
            if (m_conn != null) {
                m_conn.close();
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void loadDatabase() {
        try {
            // db parameters
            String url = "jdbc:sqlite:" + m_filename;
            // create a connection to the database
            m_conn = DriverManager.getConnection(url);

            Logger.logMsg(Logger.INFO, "Connection to SQLite has been established.");

        } catch (SQLException e) {
            Logger.logMsg(Logger.ERROR, e.getMessage());
        }
    }

    private void createNewDatabase() {
        String url = "jdbc:sqlite:" + m_filename;

        try (Connection m_conn = DriverManager.getConnection(url)) {
            if (m_conn != null) {
                DatabaseMetaData meta = m_conn.getMetaData();
                Logger.logMsg(Logger.INFO, "The driver name is " + meta.getDriverName());
                Logger.logMsg(Logger.INFO, "A new database has been created.");
            }
        } catch (SQLException e) {
            Logger.logMsg(Logger.ERROR, e.getMessage());
        }
    }

    @Override
    public <T extends DataClass> boolean insertClassEntry(Class<T> c) {
        LinkedList<Field> fields = new LinkedList<>();
        try {
            // get all variables of the class
            Method m = c.getMethod("getAllFields", LinkedList.class, Class.class);
            fields = (LinkedList<Field>) m.invoke(null, fields, c);
        } catch (NoSuchMethodException | SecurityException e) {
            Logger.logMsg(Logger.ERROR, e.getMessage());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            Logger.logMsg(Logger.ERROR, e.getMessage());
        }

        // create a string to create a table with name = class name, primary  id = id
        // TODO: other columns contain the variables, reference to other DataClass = Foreign key.id -> create placeholder if possible
        String createQuery = createNewTableString(c.getName(), fields);
        Logger.logMsg(Logger.INFO, createQuery);

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private String createNewTableString(String name, LinkedList<Field> fields) {
        String query = "create table " + name + "(";

        for (Field field : fields) {
            query += field.getName() + " " + field.getType()
        }

        query += ")";

        return query;
    }

}
