package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: WriteHDFS <hdfs_file_path> <text_to_write>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);
        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(args[1]);
            outStream.close();
        } else {
            System.out.println("File already exists: " + nomcomplet);
        }
        fs.close();
    }
}
