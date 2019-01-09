package soap.cnm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by ZhangPY on 2018/11/12
 * Belong Organization OVERUN-9299
 * overun9299@163.com
 */
public class JdbcUtil {

    private static Connection conn = null;
    private static final String URL = "jdbc:mysql://localhost:3306/skysupport_1";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";



    public static Connection getConnection() {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
