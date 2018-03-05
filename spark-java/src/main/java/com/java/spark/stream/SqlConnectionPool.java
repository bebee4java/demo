package com.java.spark.stream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;

/**
 * Created by sgr on 2018/3/4/004.
 */
public class SqlConnectionPool {
    private static String url;
    private static String user;
    private static String password;
    private static int maxTotal;
    private static LinkedList<Connection> connections;

    public static void init(String driverClass, String url, String user, String password, int maxTotal) throws ClassNotFoundException {
        Class.forName(driverClass);
        SqlConnectionPool.url = url;
        SqlConnectionPool.user = user;
        SqlConnectionPool.password = password;
        SqlConnectionPool.maxTotal = maxTotal;
    }
    public static synchronized Connection getConnection(){
        try{
            if (connections == null){
                connections = new LinkedList<Connection>();
                for (int i=0; i<maxTotal; i++){
                    Connection connection = DriverManager.getConnection(url,user,password);
                    connections.push(connection);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        while (connections.isEmpty()){
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return connections.poll();
    }
    public static void returnConnection(Connection connection){
        connections.push(connection);
    }

    public static void main(String[] args) {
        try {
            SqlConnectionPool.init("com.mysql.jdbc.Driver",
                    "jdbc:mysql://localhost:3306/test","root","root",10);
            Connection connection = SqlConnectionPool.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
            ResultSet resultSet =  preparedStatement.executeQuery();
            while (resultSet.next()){
                String name = resultSet.getString("name");
                System.out.println(name);
            }
            SqlConnectionPool.returnConnection(connection);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
