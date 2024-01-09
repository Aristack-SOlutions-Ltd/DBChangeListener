package com.aristack.dbchangelistener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ProcessingOrganizationCounterpartyMapping {

    public static void processingOrgCounterpartyMapping() {
        // Existing code for loading properties and establishing connections...
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream("application.properties")) {
            properties.load(inputStream);

//            String url = properties.getProperty("url");
//            String user = properties.getProperty("user");
//            String password = properties.getProperty("mappingPassword");

            String url = properties.getProperty("oldUrl");
            String user = properties.getProperty("oldUsername");
            String password = properties.getProperty("oldPassword");

            try (Connection connection = DriverManager.getConnection(url, user, password)) {
                connection.setAutoCommit(false); // Start transaction

                String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query)) {

                    Properties otherSchemaProperties = new Properties();
                    // Load properties for the other schema...
                    try (InputStream otherSchemaInputStream = new FileInputStream("application.properties")) {
                        otherSchemaProperties.load(otherSchemaInputStream);

//                        String otherSchemaUrl = otherSchemaProperties.getProperty("oracleURL2");
//                        String otherSchemaUser = otherSchemaProperties.getProperty("username2");
//                        String otherSchemaPassword = otherSchemaProperties.getProperty("password2");

                        String otherSchemaUrl = otherSchemaProperties.getProperty("newUrl");
                        String otherSchemaUser = otherSchemaProperties.getProperty("newUsername");
                        String otherSchemaPassword = otherSchemaProperties.getProperty("newPassword");

                        try (Connection otherSchemaConnection = DriverManager.getConnection(otherSchemaUrl, otherSchemaUser, otherSchemaPassword)) {
                            while (resultSet.next()) {
                                String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");

                                String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
                                try (PreparedStatement preparedStatement3 = connection.prepareStatement(query3)) {
                                    preparedStatement3.setString(1, legalEntityId);
                                    try (ResultSet resultSet3 = preparedStatement3.executeQuery()) {
                                        List<String> poIds = new ArrayList<>();
                                        while (resultSet3.next()) {
                                            poIds.add(resultSet3.getString("PO_ID"));
                                        }

//                                        List<String> excludedShortNames = Arrays.asList("CALYPSO", "CALYPSO_LDN", "CALYPSO_NYC");
                                        List<String> excludedShortNames = Arrays.asList("ECOBANK MALI", "ECOBANK BENIN", "ECOBANK COTE D IVOIRE", "ECOBANK BURKINA FASO", "ECOBANK GUINEA BISSAU", "CALYPSO_LDN",
                                        "CALYPSO_SFO", "ECOBANK KENYA", "ECOBANK CAMEROON", "ECOBANK GHANA", "NONE",
                                                "ECOBANK TOGO",
                                                "CALYPSO",
                                                "CLIENT", "ECOBANK PARIS EBI SA",
                                                "CALYPSO_GLOBAL",
                                                "CALYPSO_NYC",
                                                "CEMAC_REGION",
                                                "ECOBANK NIGERIA",
                                                "UEMOA_REGION",
                                                "ECOBANK SENEGAL",
                                                "ECOBANK NIGER",
                                                "EAC_REGION");
                                        for (String poId : poIds) {
                                            String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ? AND SHORT_NAME NOT IN (";
                                            for (int i = 0; i < excludedShortNames.size(); i++) {
                                                query4 += "?";
                                                if (i < excludedShortNames.size() - 1) {
                                                    query4 += ",";
                                                }
                                            }
                                            query4 += ")";

                                            try (PreparedStatement preparedStatement4 = connection.prepareStatement(query4)) {
                                                preparedStatement4.setString(1, poId);
                                                // Bind excludedShortNames values
                                                int index = 2;
                                                for (String excludedName : excludedShortNames) {
                                                    preparedStatement4.setString(index++, excludedName);
                                                }
                                                try (ResultSet resultSet4 = preparedStatement4.executeQuery()) {
                                                    while (resultSet4.next()) {
                                                        String processingOrgShortName = resultSet4.getString("SHORT_NAME");

                                                        String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
                                                        try (PreparedStatement preparedStatement2 = connection.prepareStatement(query2)) {
                                                            preparedStatement2.setString(1, legalEntityId);
                                                            try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
                                                                while (resultSet2.next()) {
                                                                    String shortName = resultSet2.getString("SHORT_NAME");

                                                                    // Check if mapping already exists in LEGAL_ENTITY_RELATIONSHIP table
                                                                    String mappingCheckQuery = "SELECT * FROM LEGAL_ENTITY_RELATIONSHIP WHERE LEGAL_ENTITY = ? AND PROCESSING_ORGANIZATION = ?";
                                                                    try (PreparedStatement mappingCheckStatement = otherSchemaConnection.prepareStatement(mappingCheckQuery)) {
                                                                        mappingCheckStatement.setString(1, shortName);
                                                                        mappingCheckStatement.setString(2, processingOrgShortName);
                                                                        ResultSet mappingCheckResultSet = mappingCheckStatement.executeQuery();

                                                                        if (!mappingCheckResultSet.next()) {
                                                                            // If mapping doesn't exist, insert it into LEGAL_ENTITY_RELATIONSHIP table
                                                                            String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (LEGAL_ENTITY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
                                                                            try (PreparedStatement insertStatement = otherSchemaConnection.prepareStatement(insertQuery)) {
                                                                                insertStatement.setString(1, shortName);
                                                                                insertStatement.setString(2, processingOrgShortName);
                                                                                insertStatement.executeUpdate();
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            connection.commit(); // Commit transaction
                        } catch (SQLException e) {
                            connection.rollback(); // Rollback if an exception occurs
                            e.printStackTrace();
                        }
                    }
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


//    public static void processingOrgCounterpartyMapping() {
//        Properties properties = new Properties();
//        try (InputStream inputStream = new FileInputStream("application.properties")) {
//            properties.load(inputStream);
//
//            String url = properties.getProperty("url");
//            String user = properties.getProperty("user");
//            String password = properties.getProperty("mappingPassword");
//
////            String url = properties.getProperty("oldUrl");
////            String user = properties.getProperty("oldUsername");
////            String password = properties.getProperty("oldPassword");
//
//            try (Connection connection = DriverManager.getConnection(url, user, password)) {
//                String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//                try (Statement statement = connection.createStatement();
//                     ResultSet resultSet = statement.executeQuery(query)) {
//
//                    // Read properties specific to the other schema
//                    Properties otherSchemaProperties = new Properties();
//                    try (InputStream otherSchemaInputStream = new FileInputStream("application.properties")) {
//                        otherSchemaProperties.load(otherSchemaInputStream);
//
//                        String otherSchemaUrl = otherSchemaProperties.getProperty("oracleURL2");
//                        String otherSchemaUser = otherSchemaProperties.getProperty("username2");
//                        String otherSchemaPassword = otherSchemaProperties.getProperty("password2");
//
////                        String otherSchemaUrl = otherSchemaProperties.getProperty("newUrl");
////                        String otherSchemaUser = otherSchemaProperties.getProperty("newUsername");
////                        String otherSchemaPassword = otherSchemaProperties.getProperty("newPassword");
//
//                        // Establishing connection to the other schema
//                        try (Connection otherSchemaConnection = DriverManager.getConnection(otherSchemaUrl, otherSchemaUser, otherSchemaPassword)) {
//                            while (resultSet.next()) {
//                                String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");
//
//                                String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                                try (PreparedStatement preparedStatement3 = connection.prepareStatement(query3)) {
//                                    preparedStatement3.setString(1, legalEntityId);
//                                    try (ResultSet resultSet3 = preparedStatement3.executeQuery()) {
//                                        List<String> poIds = new ArrayList<>();
//                                        while (resultSet3.next()) {
//                                            poIds.add(resultSet3.getString("PO_ID"));
//                                        }
//
//                                        List<String> excludedShortNames = Arrays.asList("CALYPSO", "CALYPSO_LDN", "CALYPSO_NYC");
//                                        for (String poId : poIds) {
////                                            String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ? AND SHORT_NAME != 'CALYPSO'";
//                                            //Attempting to filter some Processing orgs
//                                            String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ? AND SHORT_NAME NOT IN (";
//                                            for (int i = 0; i < excludedShortNames.size(); i++) {
//                                                query4 += "?";
//                                                if (i < excludedShortNames.size() - 1) {
//                                                    query4 += ",";
//                                                }
//                                            }
//                                            query4 += ")";
//
//                                            try (PreparedStatement preparedStatement4 = connection.prepareStatement(query4)) {
//                                                preparedStatement4.setString(1, poId);
//                                                try (ResultSet resultSet4 = preparedStatement4.executeQuery()) {
//                                                    while (resultSet4.next()) {
//                                                        String processingOrgShortName = resultSet4.getString("SHORT_NAME");
//
//                                                        String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                                        try (PreparedStatement preparedStatement2 = connection.prepareStatement(query2)) {
//                                                            preparedStatement2.setString(1, legalEntityId);
//                                                            try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
//                                                                while (resultSet2.next()) {
//                                                                    String shortName = resultSet2.getString("SHORT_NAME");
//
//
//                                                                    // Check if mapping already exists in LEGAL_ENTITY_RELATIONSHIP table
//                                                                    String mappingCheckQuery = "SELECT * FROM LEGAL_ENTITY_RELATIONSHIP WHERE LEGAL_ENTITY = ? AND PROCESSING_ORGANIZATION = ?";
//                                                                    try (PreparedStatement mappingCheckStatement = otherSchemaConnection.prepareStatement(mappingCheckQuery)) {
//                                                                        mappingCheckStatement.setString(1, shortName);
//                                                                        mappingCheckStatement.setString(2, processingOrgShortName);
//                                                                        ResultSet mappingCheckResultSet = mappingCheckStatement.executeQuery();
//
//                                                                        if (!mappingCheckResultSet.next()) {
//                                                                            // If mapping doesn't exist, insert it into LEGAL_ENTITY_RELATIONSHIP table
//                                                                            String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (LEGAL_ENTITY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
//                                                                            try (PreparedStatement insertStatement = otherSchemaConnection.prepareStatement(insertQuery)) {
//
//                                                                                insertStatement.setString(1, shortName);
//                                                                                insertStatement.setString(2, processingOrgShortName);
//                                                                                insertStatement.executeUpdate();
//                                                                            }
//                                                                        }
//                                                                    }
//
//                                                                    // Insert into LEGAL_ENTITY_RELATIONSHIP table
////                                                            String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (COUNTERPARTY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
////                                                            try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
////                                                                insertStatement.setString(1, shortName);
////                                                                insertStatement.setString(2, processingOrgShortName);
////                                                                insertStatement.executeUpdate();
////                                                            }
//                                                                }
//                                                            }
//                                                        }
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }


//package com.aristack.dbchangelistener;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//public class ProcessingOrganizationCounterpartyMapping {
//
//    public static void processingOrgCounterpartyMapping() {
//        Properties properties = new Properties();
//        try (InputStream inputStream = new FileInputStream("application.properties")) {
//            properties.load(inputStream);
//
//            String url = properties.getProperty("url");
//            String user = properties.getProperty("user");
//            String password = properties.getProperty("mappingPassword");
//
//            try (Connection connection = DriverManager.getConnection(url, user, password)) {
//                String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//                try (Statement statement = connection.createStatement();
//                     ResultSet resultSet = statement.executeQuery(query)) {
//
//                    while (resultSet.next()) {
//                        String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");
//
//                        String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                        try (PreparedStatement preparedStatement3 = connection.prepareStatement(query3)) {
//                            preparedStatement3.setString(1, legalEntityId);
//                            try (ResultSet resultSet3 = preparedStatement3.executeQuery()) {
//                                List<String> poIds = new ArrayList<>();
//                                while (resultSet3.next()) {
//                                    poIds.add(resultSet3.getString("PO_ID"));
//                                }
//
//                                for (String poId : poIds) {
//                                    String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                    try (PreparedStatement preparedStatement4 = connection.prepareStatement(query4)) {
//                                        preparedStatement4.setString(1, poId);
//                                        try (ResultSet resultSet4 = preparedStatement4.executeQuery()) {
//                                            while (resultSet4.next()) {
//                                                String processingOrgShortName = resultSet4.getString("SHORT_NAME");
//
//                                                String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                                try (PreparedStatement preparedStatement2 = connection.prepareStatement(query2)) {
//                                                    preparedStatement2.setString(1, legalEntityId);
//                                                    try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
//                                                        while (resultSet2.next()) {
//                                                            String shortName = resultSet2.getString("SHORT_NAME");
//
//                                                            // Check if mapping already exists in LEGAL_ENTITY_RELATIONSHIP table
//                                                            String mappingCheckQuery = "SELECT * FROM LEGAL_ENTITY_RELATIONSHIP WHERE LEGAL_ENTITY = ? AND PROCESSING_ORGANIZATION = ?";
//                                                            try (PreparedStatement mappingCheckStatement = connection.prepareStatement(mappingCheckQuery)) {
//                                                                mappingCheckStatement.setString(1, shortName);
//                                                                mappingCheckStatement.setString(2, processingOrgShortName);
//                                                                ResultSet mappingCheckResultSet = mappingCheckStatement.executeQuery();
//
//                                                                if (!mappingCheckResultSet.next()) {
//                                                                    // If mapping doesn't exist, insert it into LEGAL_ENTITY_RELATIONSHIP table
//                                                                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (LEGAL_ENTITY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
//                                                                    try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
//                                                                        insertStatement.setString(1, shortName);
//                                                                        insertStatement.setString(2, processingOrgShortName);
//                                                                        insertStatement.executeUpdate();
//                                                                    }
//                                                                }
//                                                            }
//
//                                                            // Insert into LEGAL_ENTITY_RELATIONSHIP table
////                                                            String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (COUNTERPARTY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
////                                                            try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
////                                                                insertStatement.setString(1, shortName);
////                                                                insertStatement.setString(2, processingOrgShortName);
////                                                                insertStatement.executeUpdate();
////                                                            }
//                                                        }
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}






//package com.aristack.dbchangelistener;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//public class ProcessingOrganizationCounterpartyMapping {
//
//    public static void processingOrgCounterpartyMapping() {
//        Properties properties = new Properties();
//        try (InputStream inputStream = new FileInputStream("application.properties")) {
//            properties.load(inputStream);
//
//            String url = properties.getProperty("url");
//            String user = properties.getProperty("user");
//            String password = properties.getProperty("mappingPassword");
//
//            try (Connection connection = DriverManager.getConnection(url, user, password)) {
//                String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//                try (Statement statement = connection.createStatement();
//                     ResultSet resultSet = statement.executeQuery(query)) {
//
//                    while (resultSet.next()) {
//                        String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");
//
//                        String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                        try (PreparedStatement preparedStatement3 = connection.prepareStatement(query3)) {
//                            preparedStatement3.setString(1, legalEntityId);
//                            try (ResultSet resultSet3 = preparedStatement3.executeQuery()) {
//                                List<String> poIds = new ArrayList<>();
//                                while (resultSet3.next()) {
//                                    poIds.add(resultSet3.getString("PO_ID"));
//                                }
//
//                                for (String poId : poIds) {
//                                    String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                    try (PreparedStatement preparedStatement4 = connection.prepareStatement(query4)) {
//                                        preparedStatement4.setString(1, poId);
//                                        try (ResultSet resultSet4 = preparedStatement4.executeQuery()) {
//                                            while (resultSet4.next()) {
//                                                String processingOrgShortName = resultSet4.getString("SHORT_NAME");
//
//                                                String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                                try (PreparedStatement preparedStatement2 = connection.prepareStatement(query2)) {
//                                                    preparedStatement2.setString(1, legalEntityId);
//                                                    try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
//                                                        while (resultSet2.next()) {
//                                                            String shortName = resultSet2.getString("SHORT_NAME");
//
//                                                            // Check if mapping already exists in LEGAL_ENTITY_RELATIONSHIP table
//                                                            String mappingCheckQuery = "SELECT * FROM LEGAL_ENTITY_RELATIONSHIP WHERE Legal Entity = ? AND Processing Organization = ?";
//                                                            try (PreparedStatement mappingCheckStatement = connection.prepareStatement(mappingCheckQuery)) {
//                                                                mappingCheckStatement.setString(1, shortName);
//                                                                mappingCheckStatement.setString(2, processingOrgShortName); // Replace with actual processing_org value
//                                                                ResultSet mappingCheckResultSet = mappingCheckStatement.executeQuery();
//
//                                                                if (!mappingCheckResultSet.next()) {
//                                                                    // If mapping doesn't exist, insert it into LEGAL_ENTITY_RELATIONSHIP table
//                                                                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (Legal Entity, Processing Organization) VALUES (?, ?)";
//                                                                    try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
//                                                                        insertStatement.setString(1, shortName);
//                                                                        insertStatement.setString(2, processingOrgShortName); // Replace with actual processing_org value
//                                                                        insertStatement.executeUpdate();
//                                                                    }
//                                                                }
//                                                            }
//
//                                                        }
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}



//package com.aristack.dbchangelistener;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.Properties;
//
//public class ProcessingOrganizationCounterpartyMapping {
//
//    public static void processingOrgCounterpartyMapping() {
//        String shortName = null;
//
//        Properties properties = new Properties();
//        try (InputStream inputStream = new FileInputStream("application.properties")) {
//            properties.load(inputStream);
//
//            String url = properties.getProperty("url");
//            String user = properties.getProperty("user");
//            String password = properties.getProperty("mappingPassword");
//
//            try (Connection connection = DriverManager.getConnection(url, user, password)) {
//                String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//                try (Statement statement = connection.createStatement();
//                     ResultSet resultSet = statement.executeQuery(query)) {
//
//                    while (resultSet.next()) {
//                        String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");
//
//                        String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                        try (PreparedStatement preparedStatement = connection.prepareStatement(query2)) {
//                            preparedStatement.setString(1, legalEntityId);
//                            try (ResultSet resultSet2 = preparedStatement.executeQuery()) {
//                                while (resultSet2.next()) {
//                                    shortName = resultSet2.getString("SHORT_NAME");
//
//                                    // Check if shortName already exists in LEGAL_ENTITY_RELATIONSHIP table
//                                    String duplicateCheckQuery = "SELECT LEGAL_ENTITY FROM LEGAL_ENTITY_RELATIONSHIP WHERE LEGAL_ENTITY = ?";
//                                    try (PreparedStatement duplicateCheckStatement = connection.prepareStatement(duplicateCheckQuery)) {
//                                        duplicateCheckStatement.setString(1, shortName);
//                                        ResultSet duplicateCheckResultSet = duplicateCheckStatement.executeQuery();
//
//                                        if (!duplicateCheckResultSet.next()) {
//                                            // If shortName doesn't exist, insert it into LEGAL_ENTITY_RELATIONSHIP table
//                                            String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (LEGAL_ENTITY) VALUES (?)";
//                                            try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
//                                                insertStatement.setString(1, shortName);
//                                                insertStatement.executeUpdate();
//                                            }
//                                        }
//
////                                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (LEGAL_ENTITY) VALUES (?)";
////                                    try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
////                                        insertStatement.setString(1, shortName);
////                                        insertStatement.executeUpdate();
////                                    }
//                                    }
//                                }
//                            }
//
//                            String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                            try (PreparedStatement preparedStatement3 = connection.prepareStatement(query3)) {
//                                preparedStatement3.setString(1, legalEntityId);
//                                try (ResultSet resultSet3 = preparedStatement3.executeQuery()) {
//                                    while (resultSet3.next()) {
//                                        String poId = resultSet3.getString("PO_ID");
//
//                                        String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                                        try (PreparedStatement preparedStatement4 = connection.prepareStatement(query4)) {
//                                            preparedStatement4.setString(1, poId);
//                                            try (ResultSet resultSet4 = preparedStatement4.executeQuery()) {
//                                                while (resultSet4.next()) {
//                                                    String processingOrgShortName = resultSet4.getString("SHORT_NAME");
//
//                                                    String insertQuery2 = "UPDATE LEGAL_ENTITY_RELATIONSHIP SET PROCESSING_ORGANIZATION = ? WHERE LEGAL_ENTITY = ?";
//                                                    try (PreparedStatement insertStatement2 = connection.prepareStatement(insertQuery2)) {
//                                                        insertStatement2.setString(1, processingOrgShortName);
//                                                        insertStatement2.setString(2, shortName);
//                                                        insertStatement2.executeUpdate();
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}


//package com.aristack.dbchangelistener;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.Properties;
//public class ProcessingOrganizationCounterpartyMapping {
//
//    public static void processingOrgCounterpartyMapping() throws IOException {
//
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet resultSet = null;
//        String shortName = null;
//
//        Properties properties = new Properties();
//        InputStream inputStream = new FileInputStream("application.properties");
//        properties.load(inputStream);
//
//        String url = properties.getProperty("url");
//        String user = properties.getProperty("user");
//        String password = properties.getProperty("mappingPassword");
//
//        try {
//            // Query to get LEGAL_ENTITY_ID from LEGAL_ENTITY_ROLE where ROLE_NAME is 'CounterParty'
////            String query1 = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
////            Statement statement1 = connection.createStatement();
////            ResultSet resultSet1 = statement1.executeQuery(query1);
//
//            connection = DriverManager.getConnection(url, user, password);
//
//            // Retrieve LEGAL_ENTITY_ID from LEGAL_ENTITY_ROLE table where ROLE_NAME is equal to CounterParty
//            String query = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//            statement = connection.createStatement();
//            resultSet = statement.executeQuery(query);
//
//            while (resultSet.next()) {
//                String legalEntityId = resultSet.getString("LEGAL_ENTITY_ID");
//
//                // For each LEGAL_ENTITY_ID obtained, retrieve SHORT_NAME from LEGAL_ENTITY table
//                String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                PreparedStatement preparedStatement = connection.prepareStatement(query2);
//                preparedStatement.setString(1, legalEntityId);
//                ResultSet resultSet2 = preparedStatement.executeQuery();
//
//                while (resultSet2.next()) {
//                    shortName = resultSet2.getString("SHORT_NAME");
//
//                    // Insert SHORT_NAME into LEGAL_ENTITY_RELATIONSHIP table into COUNTERPARTY column
//                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (COUNTERPARTY) VALUES (?)";
//                    PreparedStatement insertStatement = connection.prepareStatement(insertQuery);
//                    insertStatement.setString(1, shortName);
//                    insertStatement.executeUpdate();
//                    insertStatement.close();
//                }
//                resultSet2.close();
//
//                // Check LE_RELATION for PO_IDs corresponding to LEGAL_ENTITY_ID
//                String query3 = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                PreparedStatement preparedStatement3 = connection.prepareStatement(query3);
//                preparedStatement3.setString(1, legalEntityId);
//                ResultSet resultSet3 = preparedStatement3.executeQuery();
//
//                while (resultSet3.next()) {
//                    String poId = resultSet3.getString("PO_ID");
//
//                    // Find corresponding SHORT_NAME in LEGAL_ENTITY table for the matched PO_ID
//                    String query4 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                    PreparedStatement preparedStatement4 = connection.prepareStatement(query4);
//                    preparedStatement4.setString(1, poId);
//                    ResultSet resultSet4 = preparedStatement4.executeQuery();
//
//                    while (resultSet4.next()) {
//                        String processingOrgShortName = resultSet4.getString("SHORT_NAME");
//
//                        // Insert SHORT_NAME into LEGAL_ENTITY_RELATIONSHIP table into PROCESSING_ORGANIZATION column
//                        String insertQuery2 = "UPDATE LEGAL_ENTITY_RELATIONSHIP SET PROCESSING_ORGANIZATION = ? WHERE COUNTERPARTY = ?";
//                        PreparedStatement insertStatement2 = connection.prepareStatement(insertQuery2);
//                        insertStatement2.setString(1, processingOrgShortName);
//                        insertStatement2.setString(2, shortName);
//                        insertStatement2.executeUpdate();
//                        insertStatement2.close();
//                    }
//                    resultSet4.close();
//                }
//                resultSet3.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            // Close resources
//            try {
//                if (resultSet != null) resultSet.close();
//                if (statement != null) statement.close();
//                if (connection != null) connection.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//
//        }
//    }
//}




// Iterate through the results
//            while (resultSet1.next()) {
//                int counterpartyId = resultSet1.getInt("LEGAL_ENTITY_ID");
//                statement2.setInt(1, counterpartyId);
//                ResultSet resultSet2 = statement2.executeQuery();
//
//                // Iterate through the results
//                while (resultSet2.next()) {
//                    String shortName = resultSet2.getString("SHORT_NAME");
//                    insertStatement.setString(1, shortName);
//                    insertStatement.addBatch();
//                    count++;
//
//                    if (count % batchSize == 0) {
//                        insertStatement.executeBatch();
//                        count = 0;
//                    }
//                }
//            }
//
//            // Execute any remaining inserts
//            insertStatement.executeBatch();
//
//            // Close connections
//            resultSet1.close();
//            statement1.close();
//            connection.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }


//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.Properties;
//
//public class ProcessingOrganizationCounterpartyMapping {
//
//    public static void processingOrgCounterpartyMapping() throws IOException {
//        Properties properties = new Properties();
//        InputStream inputStream = new FileInputStream("application.properties");
//        properties.load(inputStream);
//
//        String url = properties.getProperty("url");
//        String user = properties.getProperty("user");
//        String password = properties.getProperty("mappingPassword");
//
//        try (Connection connection = DriverManager.getConnection(url, user, password)) {
//
//            // Query to get LEGAL_ENTITY_ID from LEGAL_ENTITY_ROLE where ROLE_NAME is 'CounterParty'
//            String query1 = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//            Statement statement1 = connection.createStatement();
//            ResultSet resultSet1 = statement1.executeQuery(query1);
//
//            // Iterate through the results
//            while (resultSet1.next()) {
//                int counterpartyId = resultSet1.getInt("LEGAL_ENTITY_ID");
//
//                // Query to retrieve SHORT_NAME from LEGAL_ENTITY where LEGAL_ENTITY_ID matches
//                String query2 = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                PreparedStatement statement2 = connection.prepareStatement(query2);
//                statement2.setInt(1, counterpartyId);
//                ResultSet resultSet2 = statement2.executeQuery();
//
//                // Iterate through the results
//                while (resultSet2.next()) {
//                    String shortName = resultSet2.getString("SHORT_NAME");
//
//                    // Insert into LEGAL_ENTITY_RELATIONSHIP table
//                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (COUNTERPARTY) VALUES (?)";
//                    PreparedStatement insertStatement = connection.prepareStatement(insertQuery);
//                    insertStatement.setString(1, shortName);
//                    insertStatement.executeUpdate();
//                }
//            }
//
//            // Close connections
//            resultSet1.close();
//            statement1.close();
//            connection.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//}


//            PreparedStatement statement2 = null;
//            ResultSet resultSet2 = null;
//            PreparedStatement insertStatement = null;
//            PreparedStatement relationStatement = null;
//            ResultSet relationResult = null;
//            PreparedStatement poStatement = null;
//            ResultSet poResult = null;
//
//            // Query to fetch LEGAL_ENTITY_ID from LEGAL_ENTITY_ROLE table where ROLE_NAME is 'CounterParty'
//            String query1 = "SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty'";
//            Statement statement1 = connection.createStatement();
//            ResultSet resultSet1 = statement1.executeQuery(query1);
//
//            // Iterate through the results
//            while (resultSet1.next()) {
//                int counterpartyId = resultSet1.getInt("LEGAL_ENTITY_ID");
//
//                // Query to retrieve SHORT_NAME from LEGAL_ENTITY table based on LEGAL_ENTITY_ID from LEGAL_ENTITY_ROLE
//                String query2 = "SELECT LEGAL_ENTITY_ID, SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                statement2 = connection.prepareStatement(query2);
//                statement2.setInt(1, counterpartyId);
//                resultSet2 = statement2.executeQuery();
//
//                // Iterate through the results
//                while (resultSet2.next()) {
//                    int legalEntityId = resultSet2.getInt("LEGAL_ENTITY_ID");
//                    String shortName = resultSet2.getString("SHORT_NAME");
//
//                    // Insert into LEGAL_ENTITY_RELATIONSHIP table
//                    String insertQuery = "INSERT INTO LEGAL_ENTITY_RELATIONSHIP (CounterParty, Processing_Organization) VALUES (?, ?)";
//                    insertStatement = connection.prepareStatement(insertQuery);
//                    insertStatement.setString(1, shortName);
//
//                    // Query to fetch PO_ID from LE_RELATION table
//                    String relationQuery = "SELECT PO_ID FROM LE_RELATION WHERE LE_ID = ?";
//                    relationStatement = connection.prepareStatement(relationQuery);
//                    relationStatement.setInt(1, legalEntityId);
//                    relationResult = relationStatement.executeQuery();
//
//                    // Iterate through the results from LE_RELATION table
//                    while (relationResult.next()) {
//                        int processingOrganizationId = relationResult.getInt("PO_ID");
//
//                        // Query to retrieve short name of processing organization from LEGAL_ENTITY table
//                        String poQuery = "SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE LEGAL_ENTITY_ID = ?";
//                        poStatement = connection.prepareStatement(poQuery);
//                        poStatement.setInt(1, processingOrganizationId);
//                        poResult = poStatement.executeQuery();
//
//                        // Iterate through the results
//                        while (poResult.next()) {
//                            String processingOrganizationShortName = poResult.getString("SHORT_NAME");
//                            insertStatement.setString(2, processingOrganizationShortName);
//                            // Execute the insertion query for LEGAL_ENTITY_RELATIONSHIP table
//                            insertStatement.executeUpdate();
//                        }
//                    }
//                }
//            }
//            // Close all connections
//            resultSet1.close();
//            statement1.close();
//            connection.close();
//            statement2.close();
//            resultSet2.close();
//            insertStatement.close();
//            relationStatement.close();
//            relationResult.close();
//            poStatement.close();
//            inputStream.close();
//            poResult.close();
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }


