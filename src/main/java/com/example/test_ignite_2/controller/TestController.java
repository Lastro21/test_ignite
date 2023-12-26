package com.example.test_ignite_2.controller;

import lombok.SneakyThrows;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class TestController {

    private static AtomicInteger atomicSequence = new AtomicInteger(0);
    private static AtomicInteger atomicDB = new AtomicInteger(0);

    private final static ExecutorService EXECUTOR = Executors.newFixedThreadPool(20);

    private static Connection conn = null;

    static Connection[] connections = null;


    {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @PostConstruct
    public void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections = new Connection[20];
        connections[0] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[1] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[2] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[3] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[4] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[5] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[6] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[7] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[8] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[9] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[10] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[11] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[12] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[13] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[14] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[15] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[16] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[17] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[18] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
        connections[19] = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    @RequestMapping(value = "/hello")
    public String helloMethod() throws SQLException, InterruptedException {

        System.out.println("Hello, World \n");

        try {
            createDatabaseTables(conn);
        } catch (Exception e) {

        }

        insertData(conn);
        Thread.sleep(1000);
        getData(conn);

        return "Hello, World !!!";
    }

    private static void createDatabaseTables(final Connection conn) throws SQLException {

        final Statement sql = conn.createStatement();
//        sql.executeUpdate("CREATE TABLE Staff (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        sql.executeUpdate("CREATE TABLE Employee (id INTEGER PRIMARY KEY, name VARCHAR (255), isEmployed tinyint(1))");

    }

    private static void insertData(final Connection conn) throws SQLException {


        for (int j = 0; j < 20; j++) {

            int finalJ = j;
            EXECUTOR.execute(new Thread(new Runnable() {

                final Connection curentConn = connections[finalJ];

                PreparedStatement sql = curentConn.prepareStatement("INSERT INTO Employee (id, name, isEmployed) VALUES (?, ?, ?)");

                @SneakyThrows
                @Override
                public void run() {
                    for (int i = 0; i < 50_000; i++) {
                        int finalI = i;
//                    sql.setLong(1, finalI);
                        sql.setLong(1, atomicSequence.incrementAndGet());
                        sql.setString(2, "JamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJamesJames");
                        sql.setInt(3, 1);
                        sql.addBatch();
                    }
                    sql.executeBatch();
                }
            }));
        }


//        sql.setLong(1, 5);
//        sql.setString(2, "James");
//        sql.setInt(3, 1);
//        sql.executeUpdate();
//
//        sql.setLong(1, 6);
//        sql.setString(2, "Monica");
//        sql.setInt(3, 0);
//        sql.executeUpdate();
    }

    private static void getData(final Connection conn) throws SQLException {

//        Statement sql = conn.createStatement();
//        ResultSet rs = sql.executeQuery("SELECT e.name, e.isEmployed " +
//                " FROM Employee e " +
//                " WHERE e.isEmployed = TRUE ");
//
//        while (rs.next())
//            System.out.println(rs.getString(1) + ", " + rs.getString(2));



        Statement sql = conn.createStatement();
        ResultSet rs = sql.executeQuery("SELECT COUNT(*) FROM employee");

        while (rs.next())
            System.out.println(rs.getInt(1));




    }


}