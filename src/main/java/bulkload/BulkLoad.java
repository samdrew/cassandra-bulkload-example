/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad {
    public static final SimpleDateFormat DATE_FORMAT_DIR = new SimpleDateFormat("yyyyMMddHHmm");
    public static final SimpleDateFormat DATE_FORMAT_FILE = new SimpleDateFormat("'data/data-'yyyyMMddHHmm'.gz'");

    /**
     * Default output directory
     */
    public static final String DEFAULT_OUTPUT_DIR = "./data" + DATE_FORMAT_DIR.format(new Date());


    /**
     * Keyspace name
     */
    public static final String KEYSPACE = "wiki";
    /**
     * Table name
     */
    public static final String TABLE = "views";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
            "datetime timestamp, " +
            "wiki text, " +
            "title text, " +
            "views int, " +
            "PRIMARY KEY ((datetime, wiki, title)) " +
            ")", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
                "datetime, wiki, title, views" +
            ") VALUES (" +
                "?, ?, ?, ?" +
            ")", KEYSPACE, TABLE);

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("usage: java bulkload.BulkLoad <list of files>");
            return;
        }

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                // set target schema
                .forTable(SCHEMA)
                // set CQL statement to put data
                .using(INSERT_STMT)
                // set partitioner if needed
                // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

        for (String file : args) {
            CsvPreference csvPreference = new CsvPreference.Builder('\"', ' ', "\n").build();
            try (
                    InputStream fileStream = new FileInputStream(file);
                    InputStream gzipStream = new GZIPInputStream(fileStream);
                    Reader decoder = new InputStreamReader(gzipStream);
                    BufferedReader buffered = new BufferedReader(decoder);
                    CsvListReader csvReader = new CsvListReader(buffered, csvPreference)
            ) {
                csvReader.getHeader(false);

                // Write to SSTable while reading data
                String line;
                while ((line = buffered.readLine()) != null) {
                    List<String> record = Arrays.asList(line.split(" "));
                    // We use Java types here based on
                    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                    writer.addRow(DATE_FORMAT_FILE.parse(file),
                            record.get(0),
                            record.get(1),
                            new Integer(record.get(2)));
                }
            } catch (InvalidRequestException | ParseException | IOException e) {
                e.printStackTrace();
            }
        }

        try {
            writer.close();
        } catch (IOException ignore) {
        }
    }
}
