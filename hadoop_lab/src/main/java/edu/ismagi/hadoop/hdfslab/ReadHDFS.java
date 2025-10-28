package edu.ismagi.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class ReadHDFS {

    public static void main(String[] args) throws IOException {
        // Usage: ReadHDFS <hdfs_file_path>
        if (args.length < 1) {
            System.err.println("Usage: ReadHDFS <hdfs_file_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");  // 👈 add this line

        try (FileSystem fs = FileSystem.get(conf)) {
            Path file = new Path(args[0]);
            if (!fs.exists(file)) {
                System.err.println("File not found: " + file);
                System.exit(2);
            }

            try (FSDataInputStream in = fs.open(file);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            }
        }
    }
}
