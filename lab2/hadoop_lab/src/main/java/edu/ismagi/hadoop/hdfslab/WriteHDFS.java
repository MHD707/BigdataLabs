package edu.ismagi.hadoop.hdfslab;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        // Usage: WriteHDFS <hdfs_target_file> [content]
        if (args.length < 1) {
            System.err.println("Usage: WriteHDFS <hdfs_target_file> [content]");
            System.exit(1);
        }

        String target = args[0];
        String content = (args.length >= 2) ? args[1] : "Bonjour tout le monde !";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000"); // connect to HDFS

        try (FileSystem fs = FileSystem.get(conf)) {
            Path file = new Path(target);

            if (fs.exists(file)) {
                System.out.println("File already exists: " + file);
                System.exit(2);
            }

            try (FSDataOutputStream out = fs.create(file)) {
                out.write(content.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }

            System.out.println("✅ File written to: " + file);
        }
    }
}
