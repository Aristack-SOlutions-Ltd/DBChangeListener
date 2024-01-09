package com.aristack.dbchangelistener;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DropStaticData {
    DbConnection dbcon = new DbConnection();
    StringBuilder stringBuilder = new StringBuilder();
    //String result = stringBuilder.toString();
    Connection con1 = null;

    Connection con2 = null;
    Statement stmt = null;
    Statement stmt2 = null;


    {

        try {
            Properties properties = new Properties();
            InputStream inputStream = new FileInputStream("application.properties");
            properties.load(inputStream);

//            String url = properties.getProperty("oracleUrl");
//            String username = properties.getProperty("username");
//            String password = properties.getProperty("password");

            String url = properties.getProperty("oldUrl");
            String username = properties.getProperty("oldUsername");
            String password = properties.getProperty("oldPassword");

//            String url2 = properties.getProperty("oracleURL2");
//            String username2 = properties.getProperty("username2");
//            String password2 = properties.getProperty("password2");

            String url2 = properties.getProperty("newUrl");
            String username2 = properties.getProperty("newUsername");
            String password2 = properties.getProperty("newPassword");

            con1 = dbcon.connectDb(url, username, password);
            con2 = dbcon.connectDb(url2, username2, password2);

            stmt = con1.createStatement();
            stmt2 = con2.createStatement();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public DropStaticData() throws SQLException, IOException {
    }

    public StringBuilder getProduct() throws SQLException {
        String Selectquery = "SELECT b.product_id, " +
                "b.bond_name, " +
                "b.currency, " +
                "TO_CHAR(b.issue_date, 'YYYY-MM-DD') AS ISSUE_DATE, " +
                "c.short_name, " +
                "TO_CHAR(b.maturity_date, 'YYYY-MM-DD') AS MATURITY_DATE, " +
                "b.daycount, " +
                "b.min_purchase_amt, " +
                "d.product_type, " +
                "MAX(DECODE(a.sec_code, 'ISIN', a.code_value, NULL)) PRODUCT_CODE_ISIN, " +
                "MAX(DECODE(a.sec_code, 'Name', a.code_value, NULL)) PRODUCT_CODE_Name " +
                "FROM product_sec_code a, product_bond b, legal_entity c, product_desc d " +
                "WHERE a.sec_code IN ('ISIN', 'Name') " +
                "AND a.product_id = b.product_id " +
                "AND b.issuer_le_id = c.legal_entity_id " +
                "AND a.product_id = d.product_id " +
                "AND b.maturity_date >= TRUNC(SYSDATE) " +
                "AND d.product_type IN ('Bond', 'BondMMDiscount') " +
                "AND (b.product_id, b.bond_name, b.currency, TO_CHAR(b.issue_date, 'YYYY-MM-DD'), c.short_name, TO_CHAR(b.maturity_date, 'YYYY-MM-DD'), b.daycount, b.min_purchase_amt, d.product_type) " +
                "NOT IN (SELECT product_id, bond_name, currency, TO_CHAR(issue_date, 'YYYY-MM-DD'), short_name, TO_CHAR(maturity_date, 'YYYY-MM-DD'), daycount, min_purchase_amt, product_type FROM TTCS.custom_bond_table) " +
                "GROUP BY b.product_id, b.bond_name, b.currency, TO_CHAR(b.issue_date, 'YYYY-MM-DD'), c.short_name, TO_CHAR(b.maturity_date, 'YYYY-MM-DD'), b.daycount, b.min_purchase_amt, d.product_type";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Product Id,Security Name,Product Currency,Issue Date,Issuer,Maturity Date,Day Count,Minimum Purchase Amount,Product Type,PRODUCT_CODE.ISIN,PRODUCT_CODE.Name\n");

            String Insertquery = "INSERT INTO custom_bond_table (PRODUCT_ID, BOND_NAME, CURRENCY, ISSUE_DATE, SHORT_NAME, MATURITY_DATE, DAYCOUNT, MIN_PURCHASE_AMT, PRODUCT_TYPE, PRODUCT_CODE_ISIN, PRODUCT_CODE_Name) " +
                    "VALUES (?, ?, ?, TO_DATE(?, 'YYYY-MM-DD'), ?, TO_DATE(?, 'YYYY-MM-DD'), ?, ?, ?, ?, ?)";

            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    insertStatement.setString(1, rs.getString("PRODUCT_ID"));
                    insertStatement.setString(2, rs.getString("BOND_NAME"));
                    insertStatement.setString(3, rs.getString("CURRENCY"));
                    insertStatement.setString(4, rs.getString("ISSUE_DATE"));
                    insertStatement.setString(5, rs.getString("SHORT_NAME"));
                    insertStatement.setString(6, rs.getString("MATURITY_DATE"));
                    insertStatement.setString(7, rs.getString("DAYCOUNT"));
                    insertStatement.setString(8, rs.getString("MIN_PURCHASE_AMT"));
                    insertStatement.setString(9, rs.getString("PRODUCT_TYPE"));
                    insertStatement.setString(10, rs.getString("PRODUCT_CODE_ISIN"));
                    insertStatement.setString(11, rs.getString("PRODUCT_CODE_Name"));

                    insertStatement.executeUpdate();

                    // Constructing the string representation
                    String line = String.format("\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                            rs.getString("PRODUCT_ID"), rs.getString("BOND_NAME"), rs.getString("CURRENCY"),
                            rs.getString("ISSUE_DATE"), rs.getString("SHORT_NAME"), rs.getString("MATURITY_DATE"),
                            rs.getString("DAYCOUNT"), rs.getString("MIN_PURCHASE_AMT"), rs.getString("PRODUCT_TYPE"),
                            rs.getString("PRODUCT_CODE_ISIN"), rs.getString("PRODUCT_CODE_Name"));
                    stringBuilder.append(line);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("Product: " + stringBuilder);
        return stringBuilder;
    }


    public StringBuilder getProcessingOrg() throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
//        List<String> excludedShortNames = Arrays.asList("CALYPSO", "CALYPSO_LDN", "CALYPSO_NYC");
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

        try {
            String selectQuery = "SELECT SHORT_NAME " +
                    "FROM LEGAL_ENTITY " +
                    "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'ProcessingOrg') " +
                    "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM TTCS.CUSTOM_PROCESSING_ORG) " +
                    "AND le_status = 'Enabled' " +
                    "AND \"SHORT_NAME\" NOT IN (";

            for (int i = 0; i < excludedShortNames.size(); i++) {
                selectQuery += "?";
                if (i < excludedShortNames.size() - 1) {
                    selectQuery += ",";
                }
            }
            selectQuery += ")";

            PreparedStatement statement1 = con1.prepareStatement(selectQuery);

            // Set excludedShortNames as parameters
            for (int i = 0; i < excludedShortNames.size(); i++) {
                statement1.setString(i + 1, excludedShortNames.get(i));
            }

            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Legal Entity\n");

            while (rs.next()) {
                String SHORT_NAME = rs.getString("SHORT_NAME");
                String line = String.format("%-20s", SHORT_NAME);
                stringBuilder.append(line);
                stringBuilder.append(" \n");

                // Insert the SHORT_NAME into CUSTOM_PROCESSING_ORG
                String insertQuery = "INSERT INTO CUSTOM_PROCESSING_ORG (SHORT_NAME) VALUES (?)";
                try (PreparedStatement statement = con2.prepareStatement(insertQuery)) {
                    statement.setString(1, SHORT_NAME);
                    statement.executeUpdate();
                }
            }

            rs.close();
            statement1.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("ProcessingOrg: " + stringBuilder);
        return stringBuilder;
    }



    public StringBuilder getCounterparty() throws SQLException {
        String Selectquery = "SELECT LEGAL_ENTITY, PROCESSING_ORGANIZATION " +
                "FROM LEGAL_ENTITY_RELATIONSHIP " +
                "WHERE (LEGAL_ENTITY, PROCESSING_ORGANIZATION) NOT IN " +
                "(SELECT LEGAL_ENTITY, PROCESSING_ORGANIZATION FROM TTCS.CUSTOM_COUNTERPARTY)";

        try (PreparedStatement statement = con2.prepareStatement(Selectquery)) {
            ResultSet rs = statement.executeQuery();

            stringBuilder.append("Legal Entity, Processing Organization\n");

            String Insertquery = "INSERT INTO CUSTOM_COUNTERPARTY (LEGAL_ENTITY, PROCESSING_ORGANIZATION) VALUES (?, ?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String LEGAL_ENTITY = rs.getString("LEGAL_ENTITY");
                    String PROCESSING_ORGANIZATION = rs.getString("PROCESSING_ORGANIZATION");

                    String line = String.format("\"%s\",%s", LEGAL_ENTITY, PROCESSING_ORGANIZATION);
                    stringBuilder.append(line);
                    stringBuilder.append(" \n");

                    // Insert the SHORT_NAME into custom_CounterParty
                    insertStatement.setString(1, LEGAL_ENTITY);
                    insertStatement.setString(2, PROCESSING_ORGANIZATION);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getCurrency() throws Exception {
        String Selectquery = "SELECT CURRENCY_CODE FROM CURRENCY_DEFAULT WHERE \"CURRENCY_CODE\" NOT IN " +
                "(SELECT \"CURRENCY_CODE\" FROM TTCS.CUSTOM_CURRENCY_DEFAULT)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Currency Code\n");

            String Insertquery = "INSERT INTO CUSTOM_CURRENCY_DEFAULT (CURRENCY_CODE) VALUES (?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String CURRENCY_CODE = rs.getString("CURRENCY_CODE");
                    String line = String.format("%-20s", CURRENCY_CODE);
                    stringBuilder.append(line);
                    stringBuilder.append(" \n");

                    // Insert the CURRENCY_CODE into CUSTOM_CURRENCY_DEFAULT
                    insertStatement.setString(1, CURRENCY_CODE);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getCurrencyPair() throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        String Selectquery = "SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR " +
                "FROM CURRENCY_PAIR " +
                "WHERE (PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR) NOT IN " +
                "(SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR FROM TTCS.CUSTOM_CURRENCY_PAIR)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Primary Currency,Secondary Currency,BP_FACTOR\n");

            String Insertquery = "INSERT INTO CUSTOM_CURRENCY_PAIR (PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR) VALUES (?, ?, ?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String PRIMARY_CURRENCY = rs.getString("PRIMARY_CURRENCY");
                    String QUOTING_CURRENCY = rs.getString("QUOTING_CURRENCY");
                    String BP_FACTOR = rs.getString("BP_FACTOR");

                    String line = String.format("\"%s\",%s,%s\n", PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR);
                    stringBuilder.append(line);

                    // Insert data into CUSTOM_CURRENCY_PAIR
                    insertStatement.setString(1, PRIMARY_CURRENCY);
                    insertStatement.setString(2, QUOTING_CURRENCY);
                    insertStatement.setString(3, BP_FACTOR);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getHoliday() throws Exception {

        String Selectquery = "SELECT HOLIDAY_CODE FROM HOLIDAY_CODE WHERE " +
                "\"HOLIDAY_CODE\" NOT IN (SELECT \"HOLIDAY_CODE\" FROM TTCS.CUSTOM_HOLIDAY_CODE)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("HOLIDAY_CODE\n");

            String Insertquery = "INSERT INTO CUSTOM_HOLIDAY_CODE (HOLIDAY_CODE) VALUES (?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String HOLIDAY_CODE = rs.getString("HOLIDAY_CODE");
                    String line = String.format("%-20s", HOLIDAY_CODE);
                    stringBuilder.append(line);
                    stringBuilder.append(" \n");

                    // Insert HOLIDAY_CODE into CUSTOM_HOLIDAY_CODE
                    insertStatement.setString(1, HOLIDAY_CODE);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getSalesPerson() throws Exception {
        String Selectquery = "SELECT VALUE FROM DOMAIN_VALUES WHERE " +
                "\"NAME\" = 'salesPerson' AND " +
                "\"VALUE\" NOT IN (SELECT \"NAME\" FROM TTCS.DOMAIN_VALUE_SALESPERSON)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Sales Person Name\n");

            String Insertquery = "INSERT INTO DOMAIN_VALUE_SALESPERSON (NAME) VALUES (?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String VALUE = rs.getString("VALUE");
                    String line = String.format("%-20s", VALUE);
                    stringBuilder.append(line);
                    stringBuilder.append(" \n");

                    // Insert VALUE into DOMAIN_VALUE_SALESPERSON
                    insertStatement.setString(1, VALUE);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getTrader() throws Exception {
        String Selectquery = "SELECT VALUE FROM DOMAIN_VALUES WHERE " +
                "\"NAME\" = 'trader' AND " +
                "\"VALUE\" NOT IN (SELECT \"TRADER_NAME\" FROM TTCS.DOMAIN_VALUE_TRADER)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Trader Name\n");

            String Insertquery = "INSERT INTO DOMAIN_VALUE_TRADER (TRADER_NAME) VALUES (?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String VALUE = rs.getString("VALUE");
                    String line = String.format("%-20s", VALUE);
                    stringBuilder.append(line);
                    stringBuilder.append(" \n");

                    // Insert VALUE into DOMAIN_VALUE_TRADER
                    insertStatement.setString(1, VALUE);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getRateIndex() throws Exception {

        String Selectquery = "SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE " +
                "FROM rate_index " +
                "WHERE (RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE) NOT IN " +
                "(SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE FROM TTCS.CUSTOM_RATEINDEX)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Rate Index,Rate Index Tenor,Rate Index Source\n");

            String Insertquery = "INSERT INTO CUSTOM_RATEINDEX (RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE) VALUES (?, ?, ?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String RATE_INDEX_CODE = rs.getString("RATE_INDEX_CODE");
                    String RATE_INDEX_TENOR = rs.getString("RATE_INDEX_TENOR");
                    String RATE_INDEX_SOURCE = rs.getString("RATE_INDEX_SOURCE");

                    String line = String.format("\"%s\",%s,%s\n", RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE);
                    stringBuilder.append(line);

                    // Insert data into CUSTOM_RATEINDEX
                    insertStatement.setString(1, RATE_INDEX_CODE);
                    insertStatement.setString(2, RATE_INDEX_TENOR);
                    insertStatement.setString(3, RATE_INDEX_SOURCE);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }

    //    public StringBuilder getprocessingOrg() throws Exception {
////        StringBuilder stringBuilder = new StringBuilder();
//        try {
//            String Selectquery = "SELECT SHORT_NAME\n" +
//                    "FROM LEGAL_ENTITY\n" +
//                    "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'ProcessingOrg')\n" +
//                    "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM TTCS.CUSTOM_PROCESSING_ORG)\n" +
//                    "AND le_status = 'Enabled'";
//
//            PreparedStatement statement1 = con1.prepareStatement(Selectquery);
//            ResultSet rs = statement1.executeQuery();
//
//            stringBuilder.append("Legal Entity\n");
//
//            while (rs.next()) {
//                String SHORT_NAME = rs.getString("SHORT_NAME");
//                String line = String.format("%-20s", SHORT_NAME);
//                stringBuilder.append(line);
//                stringBuilder.append(" \n");
//
//                // Insert the SHORT_NAME into CUSTOM_PROCESSING_ORG
//                String Insertquery = "INSERT INTO CUSTOM_PROCESSING_ORG (SHORT_NAME) VALUES (?)";
//                try (PreparedStatement statement = con2.prepareStatement(Insertquery)) {
//                    statement.setString(1, SHORT_NAME);
//                    statement.executeUpdate();
//                }
//            }
//
//            rs.close();
//            statement1.close();
//
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        System.out.println("ProcessingOrg: " + stringBuilder);
//        return stringBuilder;
//    }


    //String query2 = "INSERT INTO DOMAIN_VALUE_TRADER SELECT * FROM DOMAIN_VALUES WHERE \"NAME\" = 'trader' and \"VALUE\" not in (SELECT \"VALUE\" from DOMAIN_VALUE_TRADER)";
//            stmt.addBatch(query);
//            stmt.addBatch(query1);
    //SQL DEVELOPER -- INSERT INTO CUSTOM_PROCESSING_ORG SELECT SHORT_NAME FROM LEGAL_ENTITY WHERE "LEGAL_ENTITY_ID" IN  (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'ProcessingOrg') and "SHORT_NAME" not in (SELECT "SHORT_NAME" from CUSTOM_PROCESSING_ORG);
//        ResultSet rs = null;
//        try {
//
//            stringBuilder.append("Legal Entity\n");
//
//            rs = stmt.executeQuery(query1);
//
////            System.out.println("rows before batch execution = " + rs.getRow());
////            System.out.println(String.format("%-20s", "LEGAL ENTITY"));
//
//            while (rs.next()) {
//                String SHORT_NAME = rs.getString("SHORT_NAME");
////                rs.last();
//
//                String line = String.format("%-20s", SHORT_NAME);
//
//                stringBuilder.append(line);
//                stringBuilder.append(" \n");
////                stringBuilder.append(SHORT_NAME + "\n");
//
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(stringBuilder);
//        return stringBuilder;

//    public StringBuilder getQuotes() {
//        String sql = "INSERT INTO DOMAIN_VALUE_TRADER SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
//                "                \"NAME\" = 'trader' and\n" +
//                "                \"VALUE\" not in (SELECT \"VALUE\" from DOMAIN_VALUE_TRADER)";
//
//        //String query2 = "INSERT INTO DOMAIN_VALUE_TRADER SELECT * FROM DOMAIN_VALUES WHERE \"NAME\" = 'trader' and \"VALUE\" not in (SELECT \"VALUE\" from DOMAIN_VALUE_TRADER)";
////            stmt.addBatch(query);
////            stmt.addBatch(query1);
//
//
//        String line = null;
//        try {
//            ResultSet rs = stmt.executeQuery(sql);
//            Statement stmt = con1.createStatement();
////            Connection con = dbcon.connectdb();
//            //Statement stmt = con.createStatement();
//
//            stringBuilder.append(
//                    "QuoteSet,QuoteNameType,QuoteName,QuoteType,Date,Bid,Ask,Open,Close,High," +
//                            "Low,Last,EstimatedB,SourceName,PriceSource\n");
//
//            // con.prepareStatement(sql);
//
//            while (rs.next()) {
//                String Quote_Set_Name = rs.getString("Quote_Set_Name");
//                String BondName = rs.getString("Security Name");
//                String CurrencyId = rs.getString("Product Currency");
//                String IssueDate = rs.getString("Issue Date");
//                String Issuer = rs.getString("Issuer");
//                String MaturityDate = rs.getString("Maturity Date");
//                String DayCount = rs.getString("Day Count");
//                String MinimumPurshaseAmount = rs.getString("Minimum Purchase Amount");
//                String ProductType = rs.getString("Product Type");
//                String ISIN = rs.getString("PRODUCT_CODE.ISIN");
//                String Name = rs.getString("PRODUCT_CODE.Name");
//
//                line = String.format("\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
//                        Quote_Set_Name, BondName, CurrencyId, IssueDate, Issuer, MaturityDate, DayCount, MinimumPurshaseAmount, ProductType, ISIN, Name);
//                stringBuilder.append(line);
////                rs.last();
//            }
//
//            rs.close();
//            stmt.close();
////            con.prepareStatement(sql);
////            rs = stmt.executeQuery(sql);
////            System.out.println("rows before batch execution = " + rs);
////            System.out.println("rows before batch execution = ");
////            System.out.println(stringBuilder);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(stringBuilder);
//        return stringBuilder;
//    }

    public StringBuilder getFXReset() throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        String Selectquery = "SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY " +
                "FROM fx_reset " +
                "WHERE (FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY) NOT IN " +
                "(SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY FROM TTCS.CUSTOM_FXRESET)";

        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Name,Base Ccy,Quote Ccy\n");

            String Insertquery = "INSERT INTO CUSTOM_FXRESET (FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY) VALUES (?, ?, ?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
                while (rs.next()) {
                    String FX_RESET_NAME = rs.getString("FX_RESET_NAME");
                    String PRIMARY_CURRENCY = rs.getString("PRIMARY_CURRENCY");
                    String QUOTING_CURRENCY = rs.getString("QUOTING_CURRENCY");

                    String line = String.format("\"%s\",%s,%s\n", FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY);
                    stringBuilder.append(line);

                    // Insert data into CUSTOM_FXRESET
                    insertStatement.setString(1, FX_RESET_NAME);
                    insertStatement.setString(2, PRIMARY_CURRENCY);
                    insertStatement.setString(3, QUOTING_CURRENCY);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }


    public StringBuilder getBook() throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
//        List<String> excludedShortNames = Arrays.asList("CALYPSO", "CALYPSO_LDN", "CALYPSO_NYC");
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

        String selectQuery = "SELECT a.book_id, a.book_name, b.short_name " +
                "FROM book a " +
                "JOIN legal_entity b ON a.legal_entity_id = b.legal_entity_id " +
                "WHERE NOT EXISTS ( " +
                "  SELECT 1 " +
                "  FROM TTCS.CUSTOM_BOOK cb " +
                "  WHERE cb.BOOK_ID = a.BOOK_ID " +
                "  AND cb.BOOK_NAME = a.BOOK_NAME " +
                "  AND cb.SHORT_NAME = b.SHORT_NAME " +
                ") " +
                "AND b.SHORT_NAME NOT IN (";

        for (int i = 0; i < excludedShortNames.size(); i++) {
            selectQuery += "?";
            if (i < excludedShortNames.size() - 1) {
                selectQuery += ",";
            }
        }
        selectQuery += ")";

        try (PreparedStatement statement1 = con1.prepareStatement(selectQuery)) {
            // Set excludedShortNames as parameters
            for (int i = 0; i < excludedShortNames.size(); i++) {
                statement1.setString(i + 1, excludedShortNames.get(i));
            }

            ResultSet rs = statement1.executeQuery();

            stringBuilder.append("Name,Legal Entity,Book Id\n");

            String insertQuery = "INSERT INTO CUSTOM_BOOK (BOOK_ID, BOOK_NAME, SHORT_NAME) VALUES (?, ?, ?)";
            try (PreparedStatement insertStatement = con2.prepareStatement(insertQuery)) {
                while (rs.next()) {
                    String BOOK_NAME = rs.getString("BOOK_NAME");
                    String SHORT_NAME = rs.getString("SHORT_NAME");
                    String BOOK_ID = rs.getString("BOOK_ID");

                    String line = String.format("\"%s\",%s,%s\n", BOOK_NAME, SHORT_NAME, BOOK_ID);
                    stringBuilder.append(line);

                    // Insert data into CUSTOM_BOOK
                    insertStatement.setString(1, BOOK_ID);
                    insertStatement.setString(2, BOOK_NAME);
                    insertStatement.setString(3, SHORT_NAME);
                    insertStatement.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println(stringBuilder);
        return stringBuilder;
    }
}

    //old getBook method without filter

//    public StringBuilder getBook() throws Exception {
//        StringBuilder stringBuilder = new StringBuilder();
//        String Selectquery = "SELECT a.book_id, a.book_name, b.short_name " +
//                "FROM book a " +
//                "JOIN legal_entity b ON a.legal_entity_id = b.legal_entity_id " +
//                "WHERE NOT EXISTS ( " +
//                "  SELECT 1 " +
//                "  FROM TTCS.CUSTOM_BOOK cb " +
//                "  WHERE cb.BOOK_ID = a.BOOK_ID " +
//                "  AND cb.BOOK_NAME = a.BOOK_NAME " +
//                "  AND cb.SHORT_NAME = b.SHORT_NAME " +
//                ")";
//
//        try (PreparedStatement statement1 = con1.prepareStatement(Selectquery)) {
//            ResultSet rs = statement1.executeQuery();
//
//            stringBuilder.append("Name,Legal Entity,Book Id\n");
//
//            String Insertquery = "INSERT INTO CUSTOM_BOOK (BOOK_ID, BOOK_NAME, SHORT_NAME) VALUES (?, ?, ?)";
//            try (PreparedStatement insertStatement = con2.prepareStatement(Insertquery)) {
//                while (rs.next()) {
//                    String BOOK_NAME = rs.getString("BOOK_NAME");
//                    String SHORT_NAME = rs.getString("SHORT_NAME");
//                    String BOOK_ID = rs.getString("BOOK_ID");
//
//                    String line = String.format("\"%s\",%s,%s\n", BOOK_NAME, SHORT_NAME, BOOK_ID);
//                    stringBuilder.append(line);
//
//                    // Insert data into CUSTOM_BOOK
//                    insertStatement.setString(1, BOOK_ID);
//                    insertStatement.setString(2, BOOK_NAME);
//                    insertStatement.setString(3, SHORT_NAME);
//                    insertStatement.executeUpdate();
//                }
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        System.out.println(stringBuilder);
//        return stringBuilder;
//    }

