package com.envision;

import org.apache.hadoop.fs.Path;

/**
 * Created by xujingnan on 15-11-15.
 */
public class MachineDataTool {

    private static final String PATH_SUFFIX = ".reduce.input.records";
    public static final String RECORD_INTERVAL = "record.interval";

    public static Path getReduceInputRecordsPath(String mapredOutputDir) {
        String parent = mapredOutputDir;
        if (parent.endsWith("/")) {
            parent = parent.substring(0, parent.length() - 1);
        }
        parent += PATH_SUFFIX;
        return new Path(parent);
    }

    public static String getID(String mapredTaskID, boolean isMap) {
        if (isMap) {
            return mapredTaskID.split("m_")[1].split("_")[0];
        } else {
            return mapredTaskID.split("r_")[1].split("_")[0];
        }
    }
}
