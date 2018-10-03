/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.app;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.jdbc.StreamConf;
import org.apache.spark.util.Utils;

public class JavaCdcStreamingApp extends JavaSnappySQLJob{

    /**
     * A map of source and destination table. The source table should have been defined in
     * the source database and destination table in SnappyData
     */
    private final java.util.Map<String, String> sourceDestTables = new LinkedHashMap<>();

    private java.util.Map<String, String> sourceOptions = new HashMap<>();

    private SnappySession snappySpark;

    public Object runSnappyJob(SnappySession snappy, Config jobConfig) {
        try{

            System.setProperty("java.security.egd", "file:///dev/urandom");
            JavaCdcStreamingApp _this = new JavaCdcStreamingApp();
            String configFile =  jobConfig.getString("configFile");
            Properties prop = new Properties();
            prop.load(new FileInputStream(configFile));
            //String configFile1 = "/Users/smahajan/Downloads/scriptsForApp1/cdc_source_connection.properties";
            String configFile1 = jobConfig.getString("configFile1");
            Properties prop1 = new Properties();
            prop1.load(new FileInputStream(configFile1));
            _this.initSourceOptions(prop1);
            _this.startJob(prop);
        }
        catch ( Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    public SnappyJobValidation isValidJob(SnappySession snappy, Config config) {
        return SnappyJobValid.apply();
    }

    /* public static void main(String[] args) throws Exception {
        JavaCdcStreamingApp _this = new JavaCdcStreamingApp();
        _this.initSourceOptions(args);
        _this.startJob(args);
    } */

    private SnappySession createSnappySession(String table) throws ClassNotFoundException, IOException {
        String checkPointDir = Utils.createTempDir(".", "stream-spark-" + table).getCanonicalPath();
        snappySpark = new SnappySession(SparkSession.builder()
                .config("spark.sql.streaming.checkpointLocation", checkPointDir)
                .getOrCreate().sparkContext());
        return snappySpark;
    }

    private void initSourceOptions(Properties args) throws Exception {
        sourceOptions = fillSourceOptions(args);
    }

    private void startJob(Properties args) throws Exception {
        configureTables(args);
        ArrayList<StreamingQuery> activeQueries = new ArrayList<>(sourceDestTables.size());
        System.out.println("SourceOptiona are " + sourceOptions);

        for (String sourceTable : sourceDestTables.keySet()) {
            SnappySession newSession = createSnappySession(sourceTable);
            DataStreamReader reader = newSession.readStream()
                    .format(StreamConf.JDBC_STREAM())
                    .option(StreamConf.SPEC(), "io.snappydata.app.SqlServerSpec")
                    .option(StreamConf.SOURCE_TABLE_NAME(), sourceTable)
                    .option(StreamConf.MAX_EVENTS(), "50000")
                    .options(sourceOptions);

            Dataset<Row> ds = reader.load();
            StreamingQuery q = getStreamWriter(sourceDestTables.get(sourceTable), ds);
            activeQueries.add(q);
        }

        snappySpark.sparkContext().addSparkListener(new SparkContextListener(activeQueries));

        for (StreamingQuery q : activeQueries) {
            q.awaitTermination();
        }
    }

    private class SparkContextListener extends SparkListener {
        ArrayList<StreamingQuery> activeQueries;
        public SparkContextListener(ArrayList<StreamingQuery> activeQueries) {
            this.activeQueries = activeQueries;
        }
        @Override
        public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
            activeQueries.stream().forEach(q -> {
                if (q.isActive()){
                    q.stop();
                }
            });
        }
    }

    protected StreamingQuery getStreamWriter(String tableName,
                                             Dataset<Row> reader) throws IOException {

        Dataset<Column> keyColumns = snappySpark.sessionCatalog().getKeyColumns(tableName);
        String keyCols = convertDataSetToString(keyColumns);
        System.out.println("Key Columns are :: " + keyCols);
        return reader.writeStream()
                .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
                .format(StreamConf.SNAPPY_SINK())
                .option("sink", ProcessEvents.class.getName())
                .option("tableName", tableName)
                .option("keyColumns", keyCols)
                .option("handleconflict", keyColumns != null ? "true" : "false")
                .start();
    }

    private static Properties readPropertyFile(String filePath) throws Exception {
        File file = new File(filePath);
        FileInputStream fileInput = new FileInputStream(file);
        Properties properties = new Properties();
        properties.load(fileInput);
        fileInput.close();
        return properties;
    }

    private static java.util.Map<String, String> fillSourceOptions(Properties args) throws Exception {
        java.util.Map<String, String> options = new HashMap<>();

        // String connPropertiesPath = args[0];
        Properties properties = args; // readPropertyFile(connPropertiesPath);

        Enumeration enuKeys = properties.keys();
        while (enuKeys.hasMoreElements()) {
            String key = (String)enuKeys.nextElement();
            String value = properties.getProperty(key);
            options.put(key, value);
        }
        return options;
    }

    private void configureTables(Properties args) throws Exception {
        // String sourceDestTablePath = args[1];
        Properties properties = args;// readPropertyFile(sourceDestTablePath);
        Enumeration enuKeys = properties.keys();
        while (enuKeys.hasMoreElements()) {
            String key = (String)enuKeys.nextElement();
            String value = properties.getProperty(key);
            sourceDestTables.put(key, value);
        }
    }

    private String convertDataSetToString(Dataset<Column> columns) {
        StringBuilder builder = new StringBuilder();
        Column[] all = (Column[]) columns.collect();
        int numColumns = all.length;
        int i = 1;
        for(Column c : all){
            builder.append(c.name());
            if (i != numColumns){
                builder.append(",");
            }
            i++;
        }
        return builder.toString();
    }

}
