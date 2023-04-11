package com.aristack.dbchangelistener;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class DbConnection {
        public Connection connectdb() {

        try {
            Properties properties = new Properties();
            InputStream inputStream = new FileInputStream("application.properties");
//            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");
            properties.load(inputStream);

            // Get the connection properties from the properties file
            String url = properties.getProperty("oracleUrl");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");

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
