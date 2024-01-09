package com.aristack.dbchangelistener;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class DbConnection {
        public Connection connectDb(String url, String username, String password) {

        try {

            //Registering the Driver
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

            // Getting the connection
            Connection con = DriverManager.getConnection(url, username, password);
            System.out.println("Connection established......");

            //Creating the Statement
            Statement stmt = con.createStatement();

            return con;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
