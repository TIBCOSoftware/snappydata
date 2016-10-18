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

public class DJob1 extends SnappySQLJob {
    @Override
    public Object runSnappyJob(SnappyContext snc, Config jobConfig) {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName"))), true)){
            String currentDirectory = new File(".").getCanonicalPath();
            TestUtils.verify(snc, jobConfig.getString("classVersion"), pw);
            return String.format("See %s/" + jobConfig.getString("logFileName"), currentDirectory);
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