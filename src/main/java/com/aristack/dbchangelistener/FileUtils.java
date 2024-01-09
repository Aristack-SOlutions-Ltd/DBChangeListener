package com.aristack.dbchangelistener;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileUtils {
//    public void getProduct(String filePath) {
//        try {
//            FileWriter fw = new FileWriter(filePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//            DropStaticData staticData = new DropStaticData();
//            String stat = String.valueOf(staticData.getProduct());
//            stat = stat.replaceAll("\"", "");
////            fw.write(stat);
//            String[] lines = stat.split("\\r?\\n"); // Split into lines
//            for (int i = 0; i < lines.length; i++) {
//                bw.write(lines[i]);
//                bw.write("\r\n"); // Write Windows format line ending
//            }
//            bw.close();
//            fw.close();
//            System.out.println("Successfully Processed for Product Table");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

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

        FileWriter fw = new FileWriter(timestampedFilePath);
        BufferedWriter bw = new BufferedWriter(fw);
        DropStaticData staticData = new DropStaticData();
        String stat = String.valueOf(staticData.getProduct());
        stat = stat.replaceAll("\"", "");
        String[] lines = stat.split("\\r?\\n");
        for (int i = 0; i < lines.length; i++) {
            bw.write(lines[i]);
            bw.write("\r\n");
        }
        bw.close();
        fw.close();
        System.out.println("Successfully Processed for Product Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
//            FileWriter fw = new FileWriter(filePath, true);
//            fw.write(System.getProperty("line.separator"));
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getProcessingOrg());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for ProcessingOrg Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
//            ProcessingOrganizationCounterpartyMapping organizationCounterpartyMapping = new ProcessingOrganizationCounterpartyMapping();
            String stat = String.valueOf(staticData.getCounterparty());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Counterparty Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getCurrency());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Currency Table");
        } catch (Exception e) {
            e.printStackTrace();


        }

    }

//    public void getcurrencyPair(String filePath) {
//        try {
//            FileWriter fw = new FileWriter(filePath);
//            BufferedWriter bw = new BufferedWriter(fw);
//            DropStaticData staticData = new DropStaticData();
//            String stat = String.valueOf(staticData.getcurrencyPair());
//            fw.write(stat);
//            bw.close();
//            fw.close();
//            System.out.println("Successfully Processed for CurrencyPair Table");
//        } catch (Exception e) {
//            e.printStackTrace();

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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getCurrencyPair());
            stat = stat.replaceAll("\"", "");
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for CurrencyPair Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getHoliday());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Holiday Table");
        } catch (Exception e) {
            e.printStackTrace();


        }

    }

    public void getSalesPerson(String filePath) {
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getSalesPerson());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for SalesPerson Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getTrader(String filePath) {
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getTrader());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Trader Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getRateIndex());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for RateIndex Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getFXReset());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for FXReset Table");
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

            FileWriter fw = new FileWriter(timestampedFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getBook());
            stat = stat.replaceAll("\"", "");
//            fw.write(stat);
            String[] lines = stat.split("\\r?\\n"); // Split into lines
            for (int i = 0; i < lines.length; i++) {
                bw.write(lines[i]);
                bw.write("\r\n"); // Write Windows format line ending
            }
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Book Table");
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

