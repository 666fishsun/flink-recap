package com.fishsun666.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 21:49
 * @Desc:
 * @Version: v1.0
 */

public class ParquetUtils {

    public static List<Map<String, String>> readParquet(String filePath) throws IOException {
        List<Map<String, String>> result = new LinkedList<>();
        Path file = new Path(filePath);
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
        ParquetReader<Group> reader = builder.build();
        SimpleGroup group = null;
        GroupType groupType = null;
        while ((group = (SimpleGroup) reader.read()) != null) {
            if (groupType == null) {
                groupType = group.getType();
            }
            Map<String, String> resultMap = new LinkedHashMap<>();
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                String tmpName = groupType.getFieldName(i);
                try {
                    String tmp = group.getValueToString(i, 0);
                    resultMap.put(tmpName, tmp);
                } catch (Exception e) {
                    System.out.println(tmpName + ":" + "");
                }
            }
            result.add(resultMap);
        }
        return result;
    }

    public static List<String> getFieldNames(String filePath) throws Exception {
        List<String> fieldNames = new LinkedList<>();
        Path file = new Path(filePath);
        ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()));
        MessageType schema = parquetFileReader.getFileMetaData().getSchema();
        //遍历字段
        for (Type field : schema.getFields()) {
            Map<String, Object> map = new LinkedHashMap<>();
            //获取基本类型字段名称
            String typeName = field.asPrimitiveType().getPrimitiveTypeName().name();
            OriginalType originalType = field.asPrimitiveType().getOriginalType();
            String originalName = originalType != null ? originalType.name() : "";
            // System.out.println(field.getName() + ":" + typeName + ":" + originalName);
            fieldNames.add(field.getName());
        }
        parquetFileReader.close();
        return fieldNames;
    }

    public static List<File> listFiles(String path) {
        File file = new File(path);
        return Arrays.asList(file.listFiles());
    }

    public static void main(String[] args) throws Exception {

        List<String> dataDirList = Arrays.asList("data/cdc_tx_site_should_sign/dt=2023-05-06/hour=3",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=6",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=8",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=18",
                "data/cdc_tx_site_should_sign/dt=2023-05-06/hour=20");
        for (String dataDir : dataDirList) {
            List<File> files = listFiles(dataDir);
            for (File file : files) {
                List<Map<String, String>> parquetContent = readParquet("file://" + file.getAbsolutePath());
                System.out.println(parquetContent.get(0).get("message_body"));
            }
        }
    }
}
