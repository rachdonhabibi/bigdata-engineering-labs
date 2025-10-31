package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: HadoopFileStatus <hdfs_dir> <file_name> <new_name>");
            System.exit(1);
        }
        String dir = args[0], name = args[1], newName = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path filepath = new Path(dir, name);
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(2);
            }

            FileStatus infos = fs.getFileStatus(filepath);
            System.out.println(infos.getLen() + " bytes");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File owner: " + infos.getOwner());
            System.out.println("File permission: " + infos.getPermission());
            System.out.println("File Replication: " + infos.getReplication());
            System.out.println("File Block Size: " + infos.getBlockSize());

            BlockLocation[] blocs = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for (BlockLocation b : blocs) {
                System.out.println("Block offset: " + b.getOffset());
                System.out.println("Block length: " + b.getLength());
                System.out.print("Block hosts: ");
                for (String host : b.getHosts()) System.out.print(host + " ");
                System.out.println();
            }

            boolean ok = fs.rename(filepath, new Path(dir, newName));
            System.out.println("Rename " + (ok ? "successful" : "failed"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(3);
        } finally {
            if (fs != null) try { fs.close(); } catch (IOException ignored) {}
        }
    }
}
