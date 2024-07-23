package com.aristack.dbchangelistener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileUtils {

    public void getProduct(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;
                DropStaticData staticData = new DropStaticData();
                StringBuilder stringBuilder = staticData.getProduct();

            if (!stringBuilder.toString().trim().equals("Product Id,Security Name,Product Currency,Issue Date,Issuer,Maturity Date,Day Count,Minimum Purchase Amount,Product Type,PRODUCT_CODE.ISIN,PRODUCT_CODE.Name")) {
                try (FileWriter fw = new FileWriter(timestampedFilePath);
                     BufferedWriter bw = new BufferedWriter(fw)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.write("\n"); // Write unix format line ending
                    }
                    System.out.println("Successfully Processed for Product Table");
                }
            }
            else{
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getprocessingOrg(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getProcessingOrg();

            String data = stringBuilder.toString().trim();
            if (!data.equals("Legal Entity") && !data.isEmpty()) {
                try (FileWriter fw = new FileWriter(timestampedFilePath);
                     BufferedWriter bw = new BufferedWriter(fw)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.write("\n"); // Write unix format line ending
                    }
                    System.out.println("Successfully Processed for Product Table");
                }
            }
            else{
                System.out.println("No data to write. Skipping file creation.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getCounterparty(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getCounterparty();

            // Check if the StringBuilder contains only the header line
            String header = "Legal Entity,Processing Organization";
            if (!stringBuilder.toString().trim().equals(header)) {
                try (FileWriter fw = new FileWriter(timestampedFilePath);
                     BufferedWriter bw = new BufferedWriter(fw)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.write("\n"); // Write unix format line ending
                    }
                    System.out.println("Successfully Processed for Counterparty Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void getCurrency(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getCurrency();

            // Check if the StringBuilder contains only the header line
            String header = "Currency Code";
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for Currency Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    //old method without the unix format line ending
//    public void getCurrency(String filePath) {
//        try {
//            // Generating timestamp
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
//            String timeStamp = dateFormat.format(new Date());
//
//            // Extracting the file extension from the original filePath
//            String extension = "";
//            int dotIndex = filePath.lastIndexOf('.');
//            if (dotIndex >= 0) {
//                extension = filePath.substring(dotIndex);
//            }
//
//            // Constructing the file name with timestamp and original file extension
//            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;
//
//            FileWriter fw = new FileWriter(timestampedFilePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//            DropStaticData staticData = new DropStaticData();
//            String stat = String.valueOf(staticData.getCurrency());
//            stat = stat.replaceAll("\"", "");
////            fw.write(stat);
//            String[] lines = stat.split("\\r?\\n"); // Split into lines
//            for (int i = 0; i < lines.length; i++) {
//                bw.write(lines[i]);
////                bw.write("\r\n"); // Write Windows format line ending
//                bw.write("\n"); // Write unix format line ending
//            }
//            bw.close();
//            fw.close();
//            System.out.println("Successfully Processed for Currency Table");
//        } catch (Exception e) {
//            e.printStackTrace();
//
//
//        }
//
//    }
    public void getcurrencyPair(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getCurrencyPair();

            // Check if the StringBuilder contains only the header line
            String header = "Primary Currency,Secondary Currency,BP_FACTOR";
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for CurrencyPair Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void getHoliday(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getHoliday();

            // Define the header line to check against
            String header = "HOLIDAY_CODE";

            // Check if the StringBuilder contains only the header line
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for Holiday Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//    public void getSalesPerson(String filePath) {
//        try {
//            // Generating timestamp
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
//            String timeStamp = dateFormat.format(new Date());
//
//            // Extracting the file extension from the original filePath
//            String extension = "";
//            int dotIndex = filePath.lastIndexOf('.');
//            if (dotIndex >= 0) {
//                extension = filePath.substring(dotIndex);
//            }
//
//            // Constructing the file name with timestamp and original file extension
//            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;
//
//            FileWriter fw = new FileWriter(timestampedFilePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//            DropStaticData staticData = new DropStaticData();
//            String stat = String.valueOf(staticData.getSalesPerson());
//            stat = stat.replaceAll("\"", "");
////            fw.write(stat);
//            String[] lines = stat.split("\\r?\\n"); // Split into lines
//            for (int i = 0; i < lines.length; i++) {
//                bw.write(lines[i]);
// //               bw.write("\r\n"); // Write Windows format line ending
//                bw.write("\n"); // Write unix format line ending
//            }
//            bw.close();
//            fw.close();
//            System.out.println("Successfully Processed for SalesPerson Table");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    public void getTrader(String filePath) {
//        try {
//            // Generating timestamp
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
//            String timeStamp = dateFormat.format(new Date());
//
//            // Extracting the file extension from the original filePath
//            String extension = "";
//            int dotIndex = filePath.lastIndexOf('.');
//            if (dotIndex >= 0) {
//                extension = filePath.substring(dotIndex);
//            }
//
//            // Constructing the file name with timestamp and original file extension
//            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;
//
//            FileWriter fw = new FileWriter(timestampedFilePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//            DropStaticData staticData = new DropStaticData();
//            String stat = String.valueOf(staticData.getTrader());
//            stat = stat.replaceAll("\"", "");
////            fw.write(stat);
//            String[] lines = stat.split("\\r?\\n"); // Split into lines
//            for (int i = 0; i < lines.length; i++) {
//                bw.write(lines[i]);
// //               bw.write("\r\n"); // Write Windows format line ending
//                bw.write("\n"); // Write unix format line ending
//            }
//            bw.close();
//            fw.close();
//            System.out.println("Successfully Processed for Trader Table");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public void getRateIndex(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getRateIndex();

            // Define the header line to check against
            String header = "Rate Index,Rate Index Tenor,Rate Index Source";

            // Check if the StringBuilder contains only the header line
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for RateIndex Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void getFXReset(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getFXReset();

            // Define the header line to check against
            String header = "Name,Base Ccy,Quote Ccy";

            // Check if the StringBuilder contains only the header line
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for FXReset Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getBook(String filePath) {
        try {
            // Generating timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timeStamp = dateFormat.format(new Date());

            // Extracting the file extension from the original filePath
            String extension = "";
            int dotIndex = filePath.lastIndexOf('.');
            if (dotIndex >= 0) {
                extension = filePath.substring(dotIndex);
            }

            // Constructing the file name with timestamp and original file extension
            String timestampedFilePath = filePath.substring(0, dotIndex) + "_" + timeStamp + extension;

            DropStaticData staticData = new DropStaticData();
            StringBuilder stringBuilder = staticData.getBook();

            // Define the header line to check against
            String header = "Name,Legal Entity,Book Id";

            // Check if the StringBuilder contains only the header line
            if (!stringBuilder.toString().trim().equals(header)) {
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(timestampedFilePath), StandardCharsets.UTF_8)) {
                    String stat = stringBuilder.toString();
                    stat = stat.replaceAll("\"", "");
                    String[] lines = stat.split("\\r?\\n");
                    for (String line : lines) {
                        bw.write(line);
                        bw.newLine(); // Write Unix format line ending
                    }
                    System.out.println("Successfully Processed for Book Table");
                }
            } else {
                System.out.println("No data to write. Skipping file creation.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    //dropping the file but not only new ones
//    public void getCounterparty(Connection connection, String filePath) {
//        BufferedWriter bw = null;
//        try  {
//            FileWriter fw = new FileWriter(filePath);
//            bw = new BufferedWriter(fw);
//
//            String query = "SELECT LEGAL_ENTITY, PROCESSING_ORGANIZATION FROM LEGAL_ENTITY_RELATIONSHIP";
//
//            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
//                 ResultSet resultSet = preparedStatement.executeQuery()) {
//
//                bw.write("Legal Entity,Processing Organization");
//                bw.newLine();
//
//                while (resultSet.next()) {
//                    String legalEntity = resultSet.getString("LEGAL_ENTITY");
//                    String processingOrg = resultSet.getString("PROCESSING_ORGANIZATION");
//
//                    bw.write(legalEntity + "," + processingOrg);
//                    bw.newLine();
//                }
//
//                System.out.println("Successfully Processed for LEGAL_ENTITY_RELATIONSHIP Table");
//            }
//
//            System.out.println("Exported LEGAL_ENTITY_RELATIONSHIP table to: " + filePath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            try {
//                if (bw != null) {
//                    bw.close(); // Close the BufferedWriter
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public void getCounterparty(Connection connection, String filePath) {
//        FileWriter fw = null;
//        BufferedWriter bw = null;
//
//        try {
//            fw = new FileWriter(filePath);
//            bw = new BufferedWriter(fw);
//
//            String query = "SELECT LEGAL_ENTITY, PROCESSING_ORGANIZATION FROM LEGAL_ENTITY_RELATIONSHIP";
//
//            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
//                 ResultSet resultSet = preparedStatement.executeQuery()) {
//
//                bw.write("Legal Entity,Processing Organization");
//                bw.newLine();
//
//                while (resultSet.next()) {
//                    String legalEntity = resultSet.getString("LEGAL_ENTITY");
//                    String processingOrg = resultSet.getString("PROCESSING_ORGANIZATION");
//
//                    bw.write(legalEntity + "," + processingOrg);
//                    bw.newLine();
//                }
//
//                System.out.println("Successfully Processed for LEGAL_ENTITY_RELATIONSHIP Table");
//            }
//
//            System.out.println("Exported LEGAL_ENTITY_RELATIONSHIP table to: " + filePath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (bw != null) {
//                    bw.close();
//                }
//                if (fw != null) {
//                    fw.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public void getCounterparty(Connection connection, String filePath) {
//        try {
//            FileWriter fw = new FileWriter(filePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//
//            // Query to fetch data from LEGAL_ENTITY_RELATIONSHIP table
//            String query = "SELECT LEGAL_ENTITY, PROCESSING_ORGANIZATION FROM LEGAL_ENTITY_RELATIONSHIP";
//
//            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
//                 ResultSet resultSet = preparedStatement.executeQuery()) {
//
//                // Write CSV header
//                bw.write("Legal Entity,Processing Organization");
//                bw.newLine();
//
//                // Write table data to CSV file
//                while (resultSet.next()) {
//                    String legalEntity = resultSet.getString("LEGAL_ENTITY");
//                    String processingOrg = resultSet.getString("PROCESSING_ORGANIZATION");
//
//                    bw.write(legalEntity + "," + processingOrg);
//                    bw.newLine();
//                }
//
//                System.out.println("Successfully Processed for LEGAL_ENTITY_RELATIONSHIP Table");
//            }
//            bw.close();
//            fw.close();
//            System.out.println("Exported LEGAL_ENTITY_RELATIONSHIP table to: " + filePath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

}

