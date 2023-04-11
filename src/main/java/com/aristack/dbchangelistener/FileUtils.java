package com.aristack.dbchangelistener;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class FileUtils {
    public void getProduct(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getProduct());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Product Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getprocessingOrg(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getprocessingOrg());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for ProcessingOrg Table");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void getCounterparty(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getCounterparty());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Counterparty Table");
        } catch (Exception e) {
            e.printStackTrace();


        }
    }

    public void getCurrency(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getCurrency());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Currency Table");
        } catch (Exception e) {
            e.printStackTrace();


        }

    }

    public void getcurrencyPair(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getcurrencyPair());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for CurrencyPair Table");
        } catch (Exception e) {
            e.printStackTrace();


        }
    }

    public void getHoliday(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getHoliday());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Holiday Table");
        } catch (Exception e) {
            e.printStackTrace();


        }

    }

    public void getSalesPerson(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getSalesPerson());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for SalesPerson Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getTrader(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getTrader());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Trader Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getRateIndex(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getRateIndex());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for RateIndex Table");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void getFXReset(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getFXReset());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for FXReset Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getBook(String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            DropStaticData staticData = new DropStaticData();
            String stat = String.valueOf(staticData.getBook());
            fw.write(stat);
            bw.close();
            fw.close();
            System.out.println("Successfully Processed for Book Table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

