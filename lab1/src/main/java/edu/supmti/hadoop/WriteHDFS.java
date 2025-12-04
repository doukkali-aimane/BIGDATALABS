package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println("Usage: hadoop jar WriteHDFS.jar <chemin_fichier> <message>");
            System.exit(1);
        }

        Path filePath = new Path(args[0]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(filePath)) {
            System.out.println("Le fichier existe déjà !");
            fs.close();
            System.exit(1);
        }

        FSDataOutputStream out = fs.create(filePath);
        out.writeUTF(args[1]);
        out.close();

        fs.close();
        System.out.println("Fichier créé : " + args[0]);
    }
}
