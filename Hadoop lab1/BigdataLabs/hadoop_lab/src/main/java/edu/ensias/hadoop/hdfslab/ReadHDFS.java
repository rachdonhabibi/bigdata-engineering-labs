package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: ReadHDFS <hdfs_file_path>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);
        if (!fs.exists(nomcomplet)) {
            System.out.println("File does not exist: " + nomcomplet);
            fs.close();
            System.exit(2);
        }
        FSDataInputStream inStream = fs.open(nomcomplet);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        inStream.close();
        fs.close();
    }
}
