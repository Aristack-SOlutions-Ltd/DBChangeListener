package com.aristack.dbchangelistener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Main {
//    private static final String WAIT_PERIOD_KEY = "scheduler.wait.period";
//    private static final long INITIAL_DELAY = 0; // start immediately
//    //    private static final long PERIOD = 6 * 60 * 60; // repeat every 6 hours
//    private static final long PERIOD = 10; // repeat every 1 minute
//    private static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;
////    private static final TimeUnit TIME_UNIT = TimeUnit.HOURS;
    public static void main(String[] args) throws FileNotFoundException {

        String propertiesFilePath = args.length > 0 ? args[0] : "application.properties";
        Properties properties = loadProperties(propertiesFilePath);

        run(properties);
    }
    private static Properties loadProperties(String propertiesFilePath) {
        try (InputStream inputStream = new FileInputStream(propertiesFilePath)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            String property1 = properties.getProperty("property.filename");
            System.out.println("Properties: " + property1);
            return properties;
        } catch (IOException e) {
            System.err.println("Failed to load application properties.");
            return new Properties(); // Return empty properties in case of failure
        }
    }

    private static void run(Properties properties) {
//        Properties properties = new Properties();
//        InputStream inputStream = new FileInputStream("application.properties");
//        InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("application.properties");
        //            properties.load(inputStream);
        try {
            String productPath = properties.getProperty("product.path");
            String processingOrgPath = properties.getProperty("processing.org.path");
            String counterpartyPath = properties.getProperty("counterparty.path");
            String currencyPath = properties.getProperty("currency.path");
            String currencyPairPath = properties.getProperty("currency.pair.path");
            String holidayPath = properties.getProperty("holiday.path");
            String salesPersonPath = properties.getProperty("salesperson.path");
            String traderPath = properties.getProperty("trader.path");
            String rateIndexPath = properties.getProperty("rateindex.path");
            String fxresetPath = properties.getProperty("fxreset.path");
            String bookPath = properties.getProperty("book.path");

            ProcessingOrganizationCounterpartyMapping mapping = new ProcessingOrganizationCounterpartyMapping();
            mapping.processingOrgCounterpartyMapping();

            FileUtils filewrite = new FileUtils();
            filewrite.getProduct(productPath);
            filewrite.getprocessingOrg(processingOrgPath);
            filewrite.getCounterparty(counterpartyPath);
            filewrite.getCurrency(currencyPath);
            filewrite.getcurrencyPair(currencyPairPath);
            filewrite.getHoliday(holidayPath);
            filewrite.getSalesPerson(salesPersonPath);
            filewrite.getTrader(traderPath);
            filewrite.getRateIndex(rateIndexPath);
            filewrite.getFXReset(fxresetPath);
            filewrite.getBook(bookPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}