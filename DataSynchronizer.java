package com.temenos.test03;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

public class DataSynchronizer {

    private static final String CSV_FILE_PATH = "data.csv";
    private static final String DB_FILE_PATH = "database.txt";
    private static final String LOG_FILE_PATH = "sync_log.txt";

    public static void main(String[] args) {
        try {
            List<DataRecord> csvData = readCsv(CSV_FILE_PATH);
            List<DataRecord> dbData = readDatabase(DB_FILE_PATH);
       //   ja
            List<DataRecord> newRecords = findNewRecords(csvData, dbData);
            List<DataRecord> updatedRecords = findUpdatedRecords(csvData, dbData);
            List<DataRecord> deletedRecords = findDeletedRecords(csvData, dbData);

            applyChanges(newRecords, updatedRecords, deletedRecords);

            logSyncResults(newRecords, updatedRecords, deletedRecords);
        } catch (Exception e) {
            logError("Synchronization failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
//  they have to full filling the runnig 
    private static List<DataRecord> readCsv(String filePath) throws IOException {
        List<DataRecord> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                DataRecord record = new DataRecord(values[0], values[1], values[2]);
                records.add(record);
            }
        }
        return records;
    }

    private static List<DataRecord> readDatabase(String filePath) throws IOException {
        List<DataRecord> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                DataRecord record = new DataRecord(values[0], values[1], values[2]);
                records.add(record);
            }
        }
        return records;
    }

    private static List<DataRecord> findNewRecords(List<DataRecord> csvData, List<DataRecord> dbData) {
        return csvData.stream()
                .filter(record -> !dbData.contains(record))
                .collect(Collectors.toList());
    }

    private static List<DataRecord> findUpdatedRecords(List<DataRecord> csvData, List<DataRecord> dbData) {
        return csvData.stream()
                .filter(record -> dbData.contains(record) && !record.equals(findRecordById(dbData, record.getId())))
                .collect(Collectors.toList());
    }

    private static List<DataRecord> findDeletedRecords(List<DataRecord> csvData, List<DataRecord> dbData) {
        return dbData.stream()
                .filter(record -> !csvData.contains(record))
                .collect(Collectors.toList());
    }

    private static DataRecord findRecordById(List<DataRecord> records, String id) {
        return records.stream()
                .filter(record -> record.getId().equals(id))
                .findFirst()
                .orElse(null);
    }

    private static void applyChanges(List<DataRecord> newRecords, List<DataRecord> updatedRecords, List<DataRecord> deletedRecords) throws IOException {
        List<DataRecord> currentData = readDatabase(DB_FILE_PATH);

        currentData.addAll(newRecords);
        for (DataRecord updatedRecord : updatedRecords) {
            DataRecord existingRecord = findRecordById(currentData, updatedRecord.getId());
            if (existingRecord != null) {
                existingRecord.setName(updatedRecord.getName());
                existingRecord.setEmail(updatedRecord.getEmail());
            }
        }
        currentData.removeAll(deletedRecords);

        writeDatabase(DB_FILE_PATH, currentData);
    }

    private static void writeDatabase(String filePath, List<DataRecord> data) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (DataRecord record : data) {
                bw.write(record.toCsvString());
                bw.newLine();
            }
        }
    }

    private static void logSyncResults(List<DataRecord> newRecords, List<DataRecord> updatedRecords, List<DataRecord> deletedRecords) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            bw.write("Synchronization Results:");
            bw.newLine();
            bw.write("New Records:");
            bw.newLine();
            for (DataRecord record : newRecords) {
                bw.write(record.toString());
                bw.newLine();
            }
            bw.write("Updated Records:");
            bw.newLine();
            for (DataRecord record : updatedRecords) {
                bw.write(record.toString());
                bw.newLine();
            }
            bw.write("Deleted Records:");
            bw.newLine();
            for (DataRecord record : deletedRecords) {
                bw.write(record.toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
  // JAR ER FIEL CAN CRATE 
    private static void logError(String message) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            bw.write("ERROR: " + message);
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
