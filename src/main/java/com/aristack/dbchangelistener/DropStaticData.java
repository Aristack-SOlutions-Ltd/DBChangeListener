package com.aristack.dbchangelistener;


import java.sql.*;

public class DropStaticData {
    DbConnection dbcon = new DbConnection();
    StringBuilder stringBuilder = new StringBuilder();
    String result = stringBuilder.toString();
    Connection con = dbcon.connectdb();
    Statement stmt = con.createStatement();

    public DropStaticData() throws SQLException {
    }

    public StringBuilder getProduct() throws SQLException {
        String Insertquery = "INSERT INTO custom_bond_table\n" +
                "SELECT b.product_id, \n" +
                "       b.bond_name, \n" +
                "       b.currency, \n" +
                "       b.issue_date,\n" +
                "       c.short_name, \n" +
                "       b.maturity_date,\n" +
                "       b.daycount, \n" +
                "       b.min_purchase_amt, \n" +
                "       d.product_type, \n" +
                "      MAX(DECODE(a.sec_code, 'ISIN', a.code_value, NULL)) PRODUCT_CODE_ISIN, \n" +
                "      MAX(DECODE(a.sec_code, 'Name', a.code_value, NULL)) PRODUCT_CODE_Name\n" +
                "      --a.sec_code,\n" +
                "     -- a.code_value\n" +
                "FROM product_sec_code a, product_bond b, legal_entity c, product_desc d\n" +
                "WHERE a.sec_code IN ('ISIN', 'Name')\n" +
                "  AND a.product_id = b.product_id\n" +
                "  AND b.issuer_le_id = c.legal_entity_id\n" +
                "  AND a.product_id = d.product_id\n" +
                "  AND b.maturity_date >= TRUNC(SYSDATE)\n" +
                "  AND d.product_type IN ('Bond', 'BondMMDiscount')\n" +
                " AND (b.product_id, b.bond_name, b.currency, b.issue_date, c.short_name, TRUNC(b.maturity_date), b.daycount, b.min_purchase_amt, d.product_type)\n" +
                "     NOT IN (SELECT product_id, bond_name, currency, issue_date, short_name, TRUNC(maturity_date), daycount, min_purchase_amt, product_type FROM custom_bond_table)\n" +
                "GROUP BY b.product_id, b.bond_name, b.currency, b.issue_date, c.short_name, b.maturity_date, b.daycount, b.min_purchase_amt, d.product_type\n";

        String Selectquery = "SELECT b.product_id, \n" +
                "       b.bond_name, \n" +
                "       b.currency, \n" +
                "       b.issue_date,\n" +
                "       c.short_name, \n" +
                "       b.maturity_date,\n" +
                "       b.daycount, \n" +
                "       b.min_purchase_amt, \n" +
                "       d.product_type, \n" +
                "      MAX(DECODE(a.sec_code, 'ISIN', a.code_value, NULL)) PRODUCT_CODE_ISIN, \n" +
                "      MAX(DECODE(a.sec_code, 'Name', a.code_value, NULL)) PRODUCT_CODE_Name\n" +
                "      --a.sec_code,\n" +
                "     -- a.code_value\n" +
                "FROM product_sec_code a, product_bond b, legal_entity c, product_desc d\n" +
                "WHERE a.sec_code IN ('ISIN', 'Name')\n" +
                "  AND a.product_id = b.product_id\n" +
                "  AND b.issuer_le_id = c.legal_entity_id\n" +
                "  AND a.product_id = d.product_id\n" +
                "  AND b.maturity_date >= TRUNC(SYSDATE)\n" +
                "  AND d.product_type IN ('Bond', 'BondMMDiscount')\n" +
                " AND (b.product_id, b.bond_name, b.currency, b.issue_date, c.short_name, TRUNC(b.maturity_date), b.daycount, b.min_purchase_amt, d.product_type)\n" +
                "     NOT IN (SELECT product_id, bond_name, currency, issue_date, short_name, TRUNC(maturity_date), daycount, min_purchase_amt, product_type FROM custom_bond_table)\n" +
                "GROUP BY b.product_id, b.bond_name, b.currency, b.issue_date, c.short_name, b.maturity_date, b.daycount, b.min_purchase_amt, d.product_type\n";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append(
                   "Product Id,Security Name,Product Currency,Issue Date,Issuer,Maturity Date,Day Count,Minimum Purshase Amount,Product Type,PRODUCT_CODE.ISIN,PRODUCT_CODE.Name\n");
            while (rs.next()) {
                String PRODUCT_ID = rs.getString("PRODUCT_ID");
                String BOND_NAME = rs.getString("BOND_NAME");
                String CURRENCY = rs.getString("CURRENCY");
                String ISSUE_DATE = rs.getString("ISSUE_DATE");
                String SHORT_NAME = rs.getString("SHORT_NAME");
                String MATURITY_DATE = rs.getString("MATURITY_DATE");
                String DAYCOUNT = rs.getString("DAYCOUNT");
                String MIN_PURCHASE_AMT = rs.getString("MIN_PURCHASE_AMT");
                String PRODUCT_TYPE = rs.getString("PRODUCT_TYPE");
                String PRODUCT_CODE_ISIN = rs.getString("PRODUCT_CODE_ISIN");
                String PRODUCT_CODE_Name = rs.getString("PRODUCT_CODE_Name");
                line = String.format("\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                        PRODUCT_ID, BOND_NAME, CURRENCY, ISSUE_DATE, SHORT_NAME, MATURITY_DATE, DAYCOUNT, MIN_PURCHASE_AMT, PRODUCT_TYPE, PRODUCT_CODE_ISIN, PRODUCT_CODE_Name);
                stringBuilder.append(line);
//                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Product: " + stringBuilder);
        return stringBuilder;


    }

    public StringBuilder getprocessingOrg() throws Exception {
//         String query1 = "SELECT * from CUSTOM_PROCESSING_ORG";

        String Insertquery = "INSERT INTO CUSTOM_PROCESSING_ORG (SHORT_NAME)\n" +
                "SELECT SHORT_NAME\n" +
                "FROM LEGAL_ENTITY\n" +
                "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'ProcessingOrg')\n" +
                "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM CUSTOM_PROCESSING_ORG)\n" +
                "AND le_status = 'Enabled'";
        String Selectquery = "SELECT SHORT_NAME\n" +
                "FROM LEGAL_ENTITY\n" +
                "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'ProcessingOrg')\n" +
                "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM CUSTOM_PROCESSING_ORG)\n" +
                "AND le_status = 'Enabled'";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Legal Entity\n");
            while (rs.next()) {
                String SHORT_NAME = rs.getString("SHORT_NAME");
                line = String.format("%-20s", SHORT_NAME);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("ProcessingOrg: " + stringBuilder);
        return stringBuilder;

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

    }

    public StringBuilder getCounterparty() throws SQLException {
//        String query1 = "SELECT * from custom_CounterParty";
        String Insertquery = "INSERT INTO custom_CounterParty (SHORT_NAME)\n" +
                "SELECT SHORT_NAME\n" +
                "FROM LEGAL_ENTITY\n" +
                "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty')\n" +
                "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM custom_CounterParty)\n" +
                "AND le_status = 'Enabled'";
        String Selectquery = "SELECT SHORT_NAME\n" +
                "FROM LEGAL_ENTITY\n" +
                "WHERE \"LEGAL_ENTITY_ID\" IN (SELECT LEGAL_ENTITY_ID FROM LEGAL_ENTITY_ROLE WHERE ROLE_NAME = 'CounterParty')\n" +
                "AND \"SHORT_NAME\" NOT IN (SELECT \"SHORT_NAME\" FROM custom_CounterParty)\n" +
                "AND le_status = 'Enabled'";
        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Legal Entity\n");
            while (rs.next()) {
                String SHORT_NAME = rs.getString("SHORT_NAME");
                line = String.format("%-20s", SHORT_NAME);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Counterparty: " + stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getCurrency() throws Exception {
//        String Selectquery = "SELECT * from CUSTOM_CURRENCY_DEFAULT";
        String Insertquery = "INSERT INTO CUSTOM_CURRENCY_DEFAULT CURRENCY_CODE\n" +
                "SELECT CURRENCY_CODE FROM CURRENCY_DEFAULT WHERE \"CURRENCY_CODE\" not in\n" +
                "(SELECT \"CURRENCY_CODE\" from CUSTOM_CURRENCY_DEFAULT)";

        String Selectquery = "SELECT CURRENCY_CODE FROM CURRENCY_DEFAULT WHERE \"CURRENCY_CODE\" not in\n" +
                "(SELECT \"CURRENCY_CODE\" from CUSTOM_CURRENCY_DEFAULT)";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Currency Code\n");
            while (rs.next()) {
                String CURRENCY_CODE = rs.getString("CURRENCY_CODE");
                line = String.format("%-20s", CURRENCY_CODE);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getcurrencyPair() throws Exception {
//        String query1 = "SELECT * from CUSTOM_CURRENCY_PAIR";
        String Insertquery = "INSERT INTO CUSTOM_CURRENCY_PAIR (PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR)\n" +
                "SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR\n" +
                "FROM CURRENCY_PAIR\n" +
                "WHERE (PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR) NOT IN \n" +
                "(SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR FROM CUSTOM_CURRENCY_PAIR)";
        String Selectquery = "SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR\n" +
                "FROM CURRENCY_PAIR\n" +
                "WHERE (PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR) NOT IN \n" +
                "(SELECT PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR FROM CUSTOM_CURRENCY_PAIR)";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Primary Currency,Secondary Currency,BP_FACTOR\n");
            while (rs.next()) {
                String PRIMARY_CURRENCY = rs.getString("PRIMARY_CURRENCY");
                String QUOTING_CURRENCY = rs.getString("QUOTING_CURRENCY");
                String BP_FACTOR = rs.getString("BP_FACTOR");


                line = String.format("\"%s\",%s,%s\n", PRIMARY_CURRENCY, QUOTING_CURRENCY, BP_FACTOR);
                stringBuilder.append(line);
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getHoliday() throws Exception {
//            String query1 = "SELECT * from CUSTOM_HOLIDAY_CODE";
        String Insertquery = "INSERT INTO CUSTOM_HOLIDAY_CODE SELECT HOLIDAY_CODE FROM HOLIDAY_CODE WHERE" +
                " \"HOLIDAY_CODE\" not in (SELECT \"HOLIDAY_CODE\" from CUSTOM_HOLIDAY_CODE)";
        String Selectquery = "SELECT HOLIDAY_CODE FROM HOLIDAY_CODE WHERE\n" +
                "\"HOLIDAY_CODE\" not in (SELECT \"HOLIDAY_CODE\" from CUSTOM_HOLIDAY_CODE)";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("HOLIDAY_CODE\n");
            while (rs.next()) {
                String HOLIDAY_CODE = rs.getString("HOLIDAY_CODE");

                line = String.format("%-20s", HOLIDAY_CODE);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getSalesPerson() throws Exception {
//        String query1 = "SELECT * from DOMAIN_VALUE_SALESPERSON";
        String Insertquery = "INSERT INTO DOMAIN_VALUE_SALESPERSON SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
                "\"NAME\" = 'salesPerson' and\n" +
                "\"VALUE\" not in (SELECT \"NAME\" from DOMAIN_VALUE_SALESPERSON)";
        String Selectquery = "SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
                "\"NAME\" = 'salesPerson' and\n" +
                "\"VALUE\" not in (SELECT \"NAME\" from DOMAIN_VALUE_SALESPERSON)";


        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Sales Person Name\n");
            while (rs.next()) {
                String VALUE = rs.getString("VALUE");

                line = String.format("%-20s", VALUE);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getTrader() throws Exception {
//        String query1 = "SELECT * from DOMAIN_VALUE_TRADER";
        String Insertquery = "INSERT INTO DOMAIN_VALUE_TRADER SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
                "\"NAME\" = 'trader' and\n" +
                "\"VALUE\" not in (SELECT \"TRADER_NAME\" from DOMAIN_VALUE_TRADER)";
        String Selectquery = "SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
                "\"NAME\" = 'trader' and\n" +
                "\"VALUE\" not in (SELECT \"TRADER_NAME\" from DOMAIN_VALUE_TRADER)";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Trader Name\n");
            while (rs.next()) {
                String VALUE = rs.getString("VALUE");

                line = String.format("%-20s", VALUE);
                stringBuilder.append(line);
                stringBuilder.append(" \n");
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;

    }

    public StringBuilder getRateIndex() throws Exception {
//        String query1 = "SELECT * from CUSTOM_RATEINDEX";
        String Insertquery = "INSERT INTO CUSTOM_RATEINDEX (RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE)\n" +
                "SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE\n" +
                "FROM rate_index\n" +
                "WHERE (RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE) NOT IN\n" +
                "(SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE FROM CUSTOM_RATEINDEX)";

        String Selectquery = "SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE\n" +
                "FROM rate_index\n" +
                "WHERE (RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE) NOT IN\n" +
                "(SELECT RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE FROM CUSTOM_RATEINDEX)";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Rate Index,Rate Index Tenor,Rate Index Source\n");
            while (rs.next()) {
                String RATE_INDEX_CODE = rs.getString("RATE_INDEX_CODE");
                String RATE_INDEX_TENOR = rs.getString("RATE_INDEX_TENOR");
                String RATE_INDEX_SOURCE = rs.getString("RATE_INDEX_SOURCE");


                line = String.format("\"%s\",%s,%s\n", RATE_INDEX_CODE, RATE_INDEX_TENOR, RATE_INDEX_SOURCE);
                stringBuilder.append(line);
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
            statement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;

    }

    public StringBuilder getQuotes() {
        String sql = "INSERT INTO DOMAIN_VALUE_TRADER SELECT VALUE FROM DOMAIN_VALUES WHERE\n" +
                "                \"NAME\" = 'trader' and\n" +
                "                \"VALUE\" not in (SELECT \"VALUE\" from DOMAIN_VALUE_TRADER)";

        //String query2 = "INSERT INTO DOMAIN_VALUE_TRADER SELECT * FROM DOMAIN_VALUES WHERE \"NAME\" = 'trader' and \"VALUE\" not in (SELECT \"VALUE\" from DOMAIN_VALUE_TRADER)";
//            stmt.addBatch(query);
//            stmt.addBatch(query1);


        String line = null;
        try {
            ResultSet rs = stmt.executeQuery(sql);
            Statement stmt = con.createStatement();
//            Connection con = dbcon.connectdb();
            //Statement stmt = con.createStatement();

            stringBuilder.append(
                    "QuoteSet,QuoteNameType,QuoteName,QuoteType,Date,Bid,Ask,Open,Close,High," +
                            "Low,Last,EstimatedB,SourceName,PriceSource\n");

            // con.prepareStatement(sql);

            while (rs.next()) {
                String Quote_Set_Name = rs.getString("Quote_Set_Name");
                String BondName = rs.getString("Security Name");
                String CurrencyId = rs.getString("Product Currency");
                String IssueDate = rs.getString("Issue Date");
                String Issuer = rs.getString("Issuer");
                String MaturityDate = rs.getString("Maturity Date");
                String DayCount = rs.getString("Day Count");
                String MinimumPurshaseAmount = rs.getString("Minimum Purchase Amount");
                String ProductType = rs.getString("Product Type");
                String ISIN = rs.getString("PRODUCT_CODE.ISIN");
                String Name = rs.getString("PRODUCT_CODE.Name");

                line = String.format("\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                        Quote_Set_Name, BondName, CurrencyId, IssueDate, Issuer, MaturityDate, DayCount, MinimumPurshaseAmount, ProductType, ISIN, Name);
                stringBuilder.append(line);
//                rs.last();
            }

            rs.close();
            stmt.close();
//            con.prepareStatement(sql);
//            rs = stmt.executeQuery(sql);
//            System.out.println("rows before batch execution = " + rs);
//            System.out.println("rows before batch execution = ");
//            System.out.println(stringBuilder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getFXReset() throws SQLException {
//                String query1 = "SELECT * from CUSTOM_FXRESET";
        String Insertquery = "INSERT INTO CUSTOM_FXRESET (FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY)\n" +
                "SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY\n" +
                "FROM fx_reset\n" +
                "WHERE (FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY) NOT IN\n" +
                "(SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY FROM CUSTOM_FXRESET)";
        String Selectquery = "SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY\n" +
                "FROM fx_reset\n" +
                "WHERE (FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY) NOT IN\n" +
                "(SELECT FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY FROM CUSTOM_FXRESET)";
        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {
//            stmt.execute(query1);
//            rs = stmt.getResultSet();
//            Statement stmt = con.createStatement();
            ResultSet rs;
//            statement.executeUpdate(query1);
            rs = statement1.executeQuery(Selectquery);
//            ResultSet rs = stmt.executeUpdate(query1);
            stringBuilder.append("Name,Base Ccy,Quote Ccy\n");
//            System.out.println("Result :" + rs.getRow() );

            while (rs.next()) {
                String FX_RESET_NAME = rs.getString("FX_RESET_NAME");
                String PRIMARY_CURRENCY = rs.getString("PRIMARY_CURRENCY");
                String QUOTING_CURRENCY = rs.getString("QUOTING_CURRENCY");


                line = String.format("\"%s\",%s,%s\n", FX_RESET_NAME, PRIMARY_CURRENCY, QUOTING_CURRENCY);
                stringBuilder.append(line);
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

    public StringBuilder getBook() throws Exception {

        String Insertquery = "INSERT INTO CUSTOM_BOOK (BOOK_ID, BOOK_NAME, SHORT_NAME)\n" +
                "SELECT a.book_id, a.book_name, b.short_name\n" +
                "FROM book a\n" +
                "JOIN legal_entity b ON a.legal_entity_id = b.legal_entity_id\n" +
                "WHERE NOT EXISTS (\n" +
                "  SELECT 1 \n" +
                "  FROM CUSTOM_BOOK cb \n" +
                "  WHERE cb.BOOK_ID = a.BOOK_ID \n" +
                "  AND cb.BOOK_NAME = a.BOOK_NAME \n" +
                "  AND cb.SHORT_NAME = b.SHORT_NAME\n" +
                ")";

        String Selectquery = "SELECT a.book_id, a.book_name, b.short_name\n" +
                "FROM book a\n" +
                "JOIN legal_entity b ON a.legal_entity_id = b.legal_entity_id\n" +
                "WHERE NOT EXISTS (\n" +
                "  SELECT 1 \n" +
                "  FROM CUSTOM_BOOK cb \n" +
                "  WHERE cb.BOOK_ID = a.BOOK_ID \n" +
                "  AND cb.BOOK_NAME = a.BOOK_NAME \n" +
                "  AND cb.SHORT_NAME = b.SHORT_NAME\n" +
                ")";

        PreparedStatement statement = con.prepareStatement(Insertquery);
        PreparedStatement statement1 = con.prepareStatement(Selectquery);
        String line = null;
        try {

            ResultSet rs;
            rs = statement1.executeQuery(Selectquery);
            stringBuilder.append("Name,Legal Entity,Book Id\n");

            while (rs.next()) {
                String BOOK_NAME = rs.getString("BOOK_NAME");
                String SHORT_NAME = rs.getString("SHORT_NAME");
                String BOOK_ID = rs.getString("BOOK_ID");

                line = String.format("\"%s\",%s,%s\n", BOOK_NAME, SHORT_NAME, BOOK_ID);
                stringBuilder.append(line);
            }
            statement.executeUpdate();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println(stringBuilder);
        return stringBuilder;
    }

}
