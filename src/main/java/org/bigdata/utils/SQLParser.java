package org.bigdata.utils;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class SQLParser {
    // 全局变量，用于保存文件头和文件尾
    private static String fileHeader = null;
    private static String fileTail = null;

    public static void main(String[] args) {
        String filePath = "src/main/resources/input/20240530.sql";
        List<String> ruleBlocks = parseSQLFile(filePath);

        // 打印文件头
//        System.out.println("文件头:");
//        System.out.println(fileHeader);

        // 打印解析后的规则块
        System.out.println("解析后的规则块:");
        List<RuleBlock> parsedBlocks = parseRuleBlocks(ruleBlocks);
//        for (RuleBlock block : parsedBlocks) {
//            System.out.println(block.toString());
//        }

        // 打印规则数量和工作状态
        printRuleStats(parsedBlocks);

        // 检查规则代码
        List<RuleBlock> updatedBlocks = checkAndFixRuleCodes(parsedBlocks);

        // 生成输出文件
        generateOutputFiles(filePath, updatedBlocks);

        // 打印文件尾部
//        if (fileTail != null) {
//            System.out.println("文件尾:");
//            System.out.println(fileTail);
//        }
    }

    public static List<String> parseSQLFile(String filePath) {
        List<String> ruleBlocks = new ArrayList<>();
        StringBuilder currentBlock = new StringBuilder();
        boolean isRuleBlock = false;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;

            while ((line = br.readLine()) != null) {
                // 检查规则块的起始标记
                if (line.trim().startsWith("/*====================================================================================================")) {
                    if (isRuleBlock) {
                        // 如果当前在规则块中，添加收集到的块到列表中
                        ruleBlocks.add(currentBlock.toString());
                        currentBlock.setLength(0);
                    } else {
                        // 如果还未进入规则块，标记从头部过渡到规则块
                        isRuleBlock = true;
                        fileHeader = new String(currentBlock);
                        currentBlock.setLength(0);
                    }
                }
                currentBlock.append(line).append("\n");
            }

            // 添加最后收集到的块（如果有）
            if (currentBlock.length() > 0) {
                ruleBlocks.add(currentBlock.toString());
            }

            // 以第一个分号为分隔符，切割出文件尾
            String lastBlock = ruleBlocks.get(ruleBlocks.size() - 1);
            int finishIndex = lastBlock.indexOf(';');

            if (finishIndex != -1) {
                fileTail = "    " + lastBlock.substring(finishIndex).trim();
                ruleBlocks.set(ruleBlocks.size() - 1, lastBlock.substring(0, finishIndex));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return ruleBlocks;
    }

    public static List<RuleBlock> parseRuleBlocks(List<String> ruleBlocks) {
        List<RuleBlock> parsedBlocks = new ArrayList<>();

        for (String block : ruleBlocks) {
            // 将每个规则块按换行符切分
            String[] lines = block.split("\n");
            StringBuilder descriptionBuilder = new StringBuilder();
            StringBuilder sqlBuilder = new StringBuilder();
            boolean isDescription = true;

            for (String line : lines) {
                // 找到描述和SQL的分隔线
                if (line.trim().startsWith("====================================================================================================*/")) {
                    descriptionBuilder.append(line).append("\n");
                    isDescription = false;
                    continue;
                }

                // 根据分隔线位置区分描述和SQL
                if (isDescription) {
                    descriptionBuilder.append(line).append("\n");
                } else {
                    sqlBuilder.append(line).append("\n");
                }
            }

            String description = "    " + descriptionBuilder.toString().trim();
            String sql = "    " + sqlBuilder.toString().trim();

            Map<String, String> metadata = parseDescriptionToMetadata(description, sql);

            parsedBlocks.add(new RuleBlock(description, sql, metadata));
        }

        return parsedBlocks;
    }

    private static Map<String, String> parseDescriptionToMetadata(String description, String sql) {
        Map<String, String> metadata = new LinkedHashMap<>();
        String[] lines = description.split("\n");
        String currentKey = null;
        StringBuilder currentValue = new StringBuilder();

        for (int i = 1; i < lines.length - 1; ++i) {
            String line = lines[i];
            if (line.trim().startsWith("#")) {
                if (currentKey != null) {
                    metadata.put(currentKey, currentValue.toString().trim());
                }
                int colonIndex = line.indexOf(":");
                if (colonIndex != -1) {
                    currentKey = line.substring(line.indexOf("#") + 1, colonIndex).trim();
                    currentValue = new StringBuilder(line.substring(colonIndex + 1).trim());
                }
            } else if (currentKey != null) {
                currentValue.append("\n").append(line);
            }
        }

        if (currentKey != null) {
            metadata.put(currentKey, currentValue.toString().trim());
        }

        sql = sql.trim();
        // 去掉块注释
        if (sql.startsWith("/*") && sql.endsWith("*/")) {
            sql = sql.substring(2, sql.length() - 2).trim();
        }

        // 去掉UNION ALL
        if (sql.startsWith("UNION ALL")) {
            sql = sql.substring(9).trim();
        }
        metadata.put("sql", sql);

        return metadata;
    }

    private static void printRuleStats(List<RuleBlock> parsedBlocks) {
        int workingCount = 0;
        for (RuleBlock block : parsedBlocks) {
            if ("1".equals(block.metadata.get("工作状态"))) {
                workingCount++;
            }
        }
        System.out.println("总规则数: " + parsedBlocks.size());
        System.out.println("工作中的规则数: " + workingCount);
    }

    private static List<RuleBlock> checkAndFixRuleCodes(List<RuleBlock> parsedBlocks) {
        List<RuleBlock> updatedBlocks = new ArrayList<>();
        int expectedCode = 1;

        System.out.println("检查规则代码:\n。。。。。。。。。。");
        for (RuleBlock block : parsedBlocks) {
            String expectedRuleCode = String.format("AM%05d", expectedCode);
            String actualRuleCode = block.metadata.get("规则代码");

            if (!expectedRuleCode.equals(actualRuleCode)) {
                System.out.println("错误代码: " + actualRuleCode + " 正确代码: " + expectedRuleCode);

                // 更新元数据中的规则代码
                block.metadata.put("规则代码", expectedRuleCode);

                // 更新描述中的规则代码
                block.description = block.description.replace(actualRuleCode, expectedRuleCode);

                // 更新SQL中的规则代码
                block.sql = block.sql.replace(actualRuleCode, expectedRuleCode);
            }

            updatedBlocks.add(block);
            expectedCode++;
        }
        System.out.println("规则代码检查完毕！");

        return updatedBlocks;
    }

    private static void generateOutputFiles(String inputFilePath, List<RuleBlock> updatedBlocks) {
        try {
            if (inputFilePath.contains("input")) {
                String outputSQLFilePath = inputFilePath.replace("input", "output").replace(".sql", "_updated.sql");
                try (FileWriter writer = new FileWriter(outputSQLFilePath)) {
                    if (fileHeader != null) {
                        writer.write(fileHeader);
                    }

                    for (RuleBlock block : updatedBlocks) {
                        writer.write(block.description + "\n");
                        writer.write(block.sql + "\n\n");
                    }

                    if (fileTail != null) {
                        writer.write(fileTail);
                    }
                }

                String outputExcelFilePath = inputFilePath.replace("input", "output").replace(".sql", "_metadata.xlsx");
                try (Workbook workbook = new XSSFWorkbook()) {
                    Sheet sheet = workbook.createSheet("Metadata");

                    // 写入标题行
                    Row headerRow = sheet.createRow(0);
                    List<String> allKeys = new ArrayList<>(updatedBlocks.get(0).metadata.keySet());
                    for (int i = 0; i < allKeys.size(); i++) {
                        Cell cell = headerRow.createCell(i);
                        cell.setCellValue(allKeys.get(i));
                    }

                    // 写入规则数据
                    int rowIndex = 1;
                    for (RuleBlock block : updatedBlocks) {
                        Row row = sheet.createRow(rowIndex++);
                        int cellIndex = 0;
                        for (String key : allKeys) {
                            Cell cell = row.createCell(cellIndex++);
                            cell.setCellValue(formatText(block.metadata.get(key), key.equals("sql")));
                        }
                    }

                    try (FileOutputStream fileOut = new FileOutputStream(outputExcelFilePath)) {
                        workbook.write(fileOut);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String formatText(String text, boolean isSQL) {
        if (text == null) {
            return "";
        }
        StringBuilder formattedText = new StringBuilder();
        String[] lines = text.split("\n");
        for (String line : lines) {
            if (isSQL) {
                formattedText.append(line.replaceFirst("^\\s{4}", "")).append("\n");
            } else {
                formattedText.append(line.trim()).append("\n");
            }
        }
        return formattedText.toString().trim();
    }

    // 内部类，用于保存解析后的规则块
    public static class RuleBlock {
        String description;
        String sql;
        Map<String, String> metadata;

        public RuleBlock(String description, String sql, Map<String, String> metadata) {
            this.description = description;
            this.sql = sql;
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Description:\n").append(description).append("\n");
            sb.append("SQL:\n").append(sql).append("\n");
            sb.append("Metadata:\n");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                if ("sql".equals(entry.getKey())) {
                    sb.append(entry.getKey()).append(": ").append("\n    ").append(entry.getValue()).append("\n");
                } else {
                    sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                }
            }
            return sb.toString();
        }
    }
}
