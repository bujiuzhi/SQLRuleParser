package org.bigdata.utils;

import org.apache.commons.io.IOUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {

    private static final String OLD_RULE_CODE_TEMPLATE = "AM\\d+";
    private static final String NEW_RULE_CODE_TEMPLATE = "AM%05d";

    public static void main(String[] args) {
        String oldSqlPath = "src/main/resources/input/20240530.sql";
        String newSqlPath = "src/main/resources/output/20240530_corrected.sql";
        String excelPath = "src/main/resources/output/20240530_metadata.xlsx";

        try {
            String sqlContent = readFile(oldSqlPath);
            String header = extractHeader(sqlContent);
            String footer = extractFooter(sqlContent);
            List<String> ruleBlocks = splitIntoRuleBlocks(sqlContent);
            int activeRulesCount = countActiveRules(ruleBlocks);

            System.out.println("\n共有" + ruleBlocks.size() + "条规则，其中" + activeRulesCount + "条规则正在工作\n");

            int i = 1;
            StringBuilder correctedContent = new StringBuilder(header.trim() + "\n");
            List<String> mismatchedRules = new ArrayList<>();
            List<Map<String, String>> metadataList = new ArrayList<>();
            List<Map<String, String>> originalMetadataList = new ArrayList<>();

            for (String block : ruleBlocks) {
                String newRuleCode = String.format(NEW_RULE_CODE_TEMPLATE, i);
                String[] parts = splitMetadataAndSQL(block);
                String metadata = parts[0];
                String sql = parts[1];

                Map<String, String> originalMetadataMap = parseMetadata(metadata, sql);
                originalMetadataList.add(originalMetadataMap);

                if (!checkRuleCode(newRuleCode, metadata, sql)) {
                    mismatchedRules.add(newRuleCode);
                }

                metadata = updateRuleCode(metadata, newRuleCode);
                sql = updateRuleCodeInSQL(sql, newRuleCode);

                Map<String, String> metadataMap = parseMetadata(metadata, sql);
                metadataList.add(metadataMap);

                correctedContent.append(metadata).append("\n").append(sql).append("\n\n");
                i++;
            }
            correctedContent.append(footer);

            if (!mismatchedRules.isEmpty()) {
                System.out.println("以下规则代码与计算出的规则代码不匹配:");
                for (String code : mismatchedRules) {
                    System.out.println(code);
                }
                writeFile(newSqlPath, correctedContent.toString());
                System.out.println("已生成修正后的文件: " + newSqlPath);
                System.out.println("请手动格式化代码,并更新到原文件中。");
            } else {
                writeExcel(excelPath, adjustSQLIndentationInMetadata(originalMetadataList));
                System.out.println("已生成Metadata文件: " + excelPath);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String readFile(String filePath) throws IOException {
        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    private static String extractHeader(String content) {
        int firstRuleIndex = content.indexOf("/*");
        return (firstRuleIndex != -1) ? content.substring(0, firstRuleIndex) : "";
    }

    private static String extractFooter(String content) {
        int lastIndex = content.lastIndexOf("*/");
        if (lastIndex != -1) {
            String tempFooter = content.substring(lastIndex);
            return tempFooter.contains(";\r\n    COMMIT;") ? tempFooter.substring(2) : "";
        }
        return "";
    }

    private static List<String> splitIntoRuleBlocks(String content) {
        List<String> ruleBlocks = new ArrayList<>();
        Pattern pattern = Pattern.compile(
                "(\\s*/\\*\\s*=+\\s*规则代码:.*?\\*/\\s*.*?)(?=\\s*/\\*\\s*=+\\s*规则代码:|\\s*;\\s*$)",
                Pattern.DOTALL | Pattern.MULTILINE
        );
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String ruleBlock = matcher.group(1).trim();
            ruleBlocks.add(ruleBlock);
        }
        if (!ruleBlocks.isEmpty()) {
            String lastBlock = ruleBlocks.get(ruleBlocks.size() - 1);
            int semicolonIndex = lastBlock.lastIndexOf(";");
            if (semicolonIndex != -1) {
                lastBlock = lastBlock.substring(0, semicolonIndex).trim();
                ruleBlocks.set(ruleBlocks.size() - 1, lastBlock);
            }
        }
        return ruleBlocks;
    }

    private static int countActiveRules(List<String> ruleBlocks) {
        int activeCount = 0;
        Pattern activePattern = Pattern.compile("工作状态:\\s*1");
        for (String block : ruleBlocks) {
            if (activePattern.matcher(block).find()) {
                activeCount++;
            }
        }
        return activeCount;
    }

    private static String[] splitMetadataAndSQL(String ruleBlock) {
        Pattern metadataPattern = Pattern.compile(
                "/\\*\\s*=+\\s*规则代码:.*?\\*/", Pattern.DOTALL | Pattern.MULTILINE
        );
        Matcher matcher = metadataPattern.matcher(ruleBlock);
        if (matcher.find()) {
            String metadata = "    " + matcher.group().trim();
            String sql = "    " + ruleBlock.substring(matcher.end()).trim();
            return new String[]{metadata, sql};
        }
        return new String[]{"", ruleBlock}; // If no metadata found, return the entire block as SQL part
    }

    private static boolean checkRuleCode(String newRuleCode, String metadata, String sql) {
        Pattern ruleCodePattern = Pattern.compile("规则代码:\\s*(" + OLD_RULE_CODE_TEMPLATE + ")");
        Matcher metadataMatcher = ruleCodePattern.matcher(metadata);
        Matcher sqlMatcher = Pattern.compile("'(" + OLD_RULE_CODE_TEMPLATE + ")'\\s+AS\\s+gzdm").matcher(sql);

        if (metadataMatcher.find() && sqlMatcher.find()) {
            String metadataCode = metadataMatcher.group(1);
            String sqlCode = sqlMatcher.group(1);
            return newRuleCode.equals(metadataCode) && newRuleCode.equals(sqlCode);
        }
        return false;
    }

    private static String updateRuleCode(String metadata, String newRuleCode) {
        return metadata.replaceAll("规则代码:\\s*" + OLD_RULE_CODE_TEMPLATE, "规则代码: " + newRuleCode);
    }

    private static String updateRuleCodeInSQL(String sql, String newRuleCode) {
        return sql.replaceAll("'" + OLD_RULE_CODE_TEMPLATE + "'\\s+AS\\s+gzdm", "'" + newRuleCode + "' AS gzdm");
    }

    private static String adjustSQLIndentation(String sql) {
        String[] lines = sql.split("\n");
        StringBuilder adjustedSQL = new StringBuilder(lines[0].trim() + "\n");
        String indentation = "    "; // Four spaces to be removed
        for (int i = 1; i < lines.length; i++) {
            adjustedSQL.append(lines[i].replaceFirst(indentation, "")).append("\n");
        }
        return adjustedSQL.toString().trim();
    }

    private static List<Map<String, String>> adjustSQLIndentationInMetadata(List<Map<String, String>> metadataList) {
        List<Map<String, String>> adjustedMetadataList = new ArrayList<>();
        for (Map<String, String> metadataMap : metadataList) {
            Map<String, String> adjustedMetadataMap = new LinkedHashMap<>(metadataMap);
            String adjustedSQL = adjustSQLIndentation(metadataMap.get("SQL"));
            adjustedMetadataMap.put("SQL", adjustedSQL);
            adjustedMetadataList.add(adjustedMetadataMap);
        }
        return adjustedMetadataList;
    }

    private static Map<String, String> parseMetadata(String metadata, String sql) {
        Map<String, String> metadataMap = new LinkedHashMap<>();
        String[] lines = metadata.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (line.contains(":")) {
                int index = line.indexOf(":");
                String key = line.substring(0, index).trim();
                String value = line.substring(index + 1).trim();
                metadataMap.put(key, value);
            }
        }

        sql = sql.trim();
        // Remove block comments if present
        if (sql.startsWith("/*") && sql.endsWith("*/")) {
            sql = sql.substring(2, sql.length() - 2).trim();
        }

        // Remove the first occurrence of UNION ALL if present
        if (sql.startsWith("UNION ALL")) {
            sql = sql.substring(9).trim();
        }

        metadataMap.put("SQL", sql.trim());
        return metadataMap;
    }

    private static void writeExcel(String filePath, List<Map<String, String>> metadataList) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Metadata");

            // Create header row
            Row headerRow = sheet.createRow(0);
            Set<String> headers = metadataList.get(0).keySet();
            int colIndex = 0;
            for (String header : headers) {
                headerRow.createCell(colIndex++).setCellValue(header);
            }

            // Create data rows
            int rowIndex = 1;
            for (Map<String, String> metadataMap : metadataList) {
                Row row = sheet.createRow(rowIndex++);
                colIndex = 0;
                for (String value : metadataMap.values()) {
                    row.createCell(colIndex++).setCellValue(value);
                }
            }

            try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
                workbook.write(outputStream);
            }
        }
    }

    private static void writeFile(String filePath, String content) throws IOException {
        try (FileWriter writer = new FileWriter(filePath, StandardCharsets.UTF_8)) {
            writer.write(content);
        }
    }
}
