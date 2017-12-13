import java.sql.*;

/**
 * Created by sgr on 2017/11/23.
 */
public class MysqlTest {
    private static final String url = "jdbc:mysql://node1/mysql";
    private static final String name = "com.mysql.jdbc.Driver";
    private static final String user = "root";
    private static final String password = "253824";
    private static Connection conn = null;
    private static Statement stmt;
    public static void main(String[] args) {
        String sql = "select user,host from user";
        try {
            Class.forName(name);//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            stmt = conn.createStatement();//准备执行语句
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()){
                String user = rs.getString("user");
                String host = rs.getString("host");
                System.out.println(user + "|" + host);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
