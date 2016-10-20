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
package io.snappydata.hydra.installJar;

import com.typesafe.config.Config;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.sql.SnappySQLJob;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

public class DynamicJarLoadingJob extends SnappySQLJob {
    @Override
    public Object runSnappyJob(SnappyContext snc, Config jobConfig) {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName"))), true)){
//            PrintWriter pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName")), true));
            pw.println("****** DynamicJarLoadingJob started ******");
            String currentDirectory = new File(".").getCanonicalPath();
            TestUtils.verify(snc, jobConfig.getString("classVersion"), pw);
            pw.println("****** DynamicJarLoadingJob finished ******");
//            pw.close();
            return String.format("See %s/" + jobConfig.getString("logFileName"),
                    currentDirectory);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter spw = new PrintWriter(sw);
            spw.println("ERROR: failed with " + e);
            e.printStackTrace(spw);
            return spw.toString();
        }
    }

    @Override
    public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {
        return new SnappyJobValid();
    }
}


