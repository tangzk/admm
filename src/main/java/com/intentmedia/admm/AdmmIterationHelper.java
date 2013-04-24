package com.intentmedia.bfgs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class AdmmIterationHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern COMPILE = Pattern.compile(",");
    private static final Logger LOG = Logger.getLogger(AdmmIterationHelper.class.getName());
    private static final Pattern TAB_PATTERN = Pattern.compile("\t");

    private AdmmIterationHelper() {
    }

    public static String admmMapperContextToJson(AdmmMapperContext context) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(context);
    }

    public static String admmReducerContextToJson(AdmmReducerContext context) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(context);
    }

    public static String mapToJson(Map<String, String> vector) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(vector);
    }

    public static Map<String, String> jsonToMap(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, HashMap.class);
    }

    public static AdmmMapperContext jsonToAdmmMapperContext(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AdmmMapperContext.class);
    }

    public static AdmmReducerContext jsonToAdmmReducerContext(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AdmmReducerContext.class);
    }

    public static double[][] createMatrixFromDataString(String dataString, Set<Integer> columnsToExclude, boolean addIntercept)
            throws ArrayIndexOutOfBoundsException {
        String[] rows = dataString.split("\\n");
        int numRows = rows.length;
        int numColumns = rows[0].split("\\s+").length - columnsToExclude.size();
        int interceptOffset = 0;
        if (addIntercept) {
            interceptOffset = 1;
        }
        double[][] data = new double[numRows][numColumns + interceptOffset];

        int[] columnArray = new int[numColumns];
        int newColumnArrayIndex = 0;
        for (int i = 0; i < rows[0].split("\\s+").length; i++) {
            if (!columnsToExclude.contains(i)) {
                columnArray[newColumnArrayIndex] = i;
                newColumnArrayIndex++;
            }
        }

        for (int i = 0; i < numRows; i++) {
            String[] elements = rows[i].split("\\s+");
            if (addIntercept) {
                data[i][0] = 1;
            }
            for (int j = 0; j < numColumns; j++) {
                // request_id, requested_at_iso, s1, s2, ..., y
                newColumnArrayIndex = columnArray[j];
                try {
                    data[i][j + interceptOffset] = Double.parseDouble(elements[newColumnArrayIndex]);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    LOG.log(Level.FINE,
                            String.format(
                                    "i value: %d, j value: %d, newColumnArrayIndex: %d, data rows: %d, data cols: %d, elements size: %d\n",
                                    i,
                                    j,
                                    newColumnArrayIndex,
                                    data.length,
                                    data[i].length,
                                    elements.length));
                    throw e;
                }
            }
        }
        return data;
    }

    public static Set<Integer> getColumnsToExclude(String columnsToExcludeString) {
        String[] columnsToExcludeArray;
        if (columnsToExcludeString == null || columnsToExcludeString.isEmpty()) {
            columnsToExcludeArray = new String[0];
        }
        else {
            columnsToExcludeArray = COMPILE.split(columnsToExcludeString);
        }

        Set<Integer> columnsToExclude = new HashSet<Integer>();
        for (String col : columnsToExcludeArray) {
            columnsToExclude.add(Integer.parseInt(col));
        }
        return columnsToExclude;
    }

    public static String removeIpFromHdfsFileName(String fileString) {
        if (fileString.contains("hdfs")) {
            int indexOfSecondForwardSlash = fileString.indexOf("/") + 1; //add 1 to get index of second forward slash
            int indexOfThirdForwardSlash = fileString.indexOf("/", indexOfSecondForwardSlash + 1);

            return fileString.substring(0, indexOfSecondForwardSlash) + fileString
                    .substring(indexOfThirdForwardSlash, fileString.length());
        }
        else {
            return fileString;
        }
    }

    public static String fsDataInputStreamToString(FSDataInputStream in, int inputSize) throws IOException {
        byte[] fileContents = new byte[inputSize];
        IOUtils.readFully(in, fileContents, 0, fileContents.length);
        String keyValue = new Text(fileContents).toString();
        return TAB_PATTERN.split(keyValue)[1]; // output from the last reduce job will be key | value
    }

    public static int getFileLength(FileSystem fs, Path thisFilePath) throws IOException {
        return (int) fs.getFileStatus(thisFilePath).getLen();
    }
}
