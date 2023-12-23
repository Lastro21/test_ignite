package com.example.test_ignite_2.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.sql.*;

@RestController
public class TestController {

    private static Connection conn = null;

    {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @PostConstruct
    public void init() throws ClassNotFoundException, SQLException {
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    @RequestMapping(value = "/hello")
    public String helloMethod() throws SQLException {

        System.out.println("Hello, World \n");


        createDatabaseTables(conn);

        insertData(conn);

        getData(conn);


        return "Hello, World !!!";
    }

    private static void createDatabaseTables(Connection conn) throws SQLException {

        Statement sql = conn.createStatement();
//        sql.executeUpdate("CREATE TABLE Staff (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        sql.executeUpdate("CREATE TABLE Employee (id INTEGER PRIMARY KEY, name VARCHAR (255), isEmployed tinyint(1))");

    }

    private static void insertData(Connection conn) throws SQLException {

        PreparedStatement sql =
                conn.prepareStatement("INSERT INTO Employee (id, name, isEmployed) VALUES (?, ?, ?)");
        sql.setLong(1, 1);
        sql.setString(2, "James");
        sql.setInt(3, 1);
        sql.executeUpdate();

        sql.setLong(1, 2);
        sql.setString(2, "Monica");
        sql.setInt(3, 0);
        sql.executeUpdate();
    }

    private static void getData(Connection conn) throws SQLException {

        Statement sql = conn.createStatement();
        ResultSet rs = sql.executeQuery("SELECT e.name, e.isEmployed " +
                " FROM Employee e " +
                " WHERE e.isEmployed = TRUE ");

        while (rs.next())
            System.out.println(rs.getString(1) + ", " + rs.getString(2));
    }


}