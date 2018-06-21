package hadoop.wholefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;

public class WholeImageTranslator {

    public static void loadHdfsImage(String hdfsPath, String dir) {
        File folder = new File(dir);
        if(folder.exists()) {
            System.err.println(dir + " already exists!");
            System.exit(1);
        } else if (!(new File(dir).mkdirs())) {
            System.err.println("create " + dir + " failed!");
            System.exit(1);
        };

        // prepare Hadoop sequence file writer
        Configuration conf = new Configuration();
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        try {
            FileSystem fs = FileSystem.get(new URI(hdfsPath), conf);
            Path inDir = new Path(hdfsPath);
            if (!fs.exists(inDir)) {
                System.err.println("Directory " + hdfsPath + " already exists!");
                return;
            } else if (!fs.isDirectory(inDir)) {
                System.err.println(inDir + " is not a directory!");
                return;
            }

            SequenceFile.Reader reader = null;

            FileStatus[] fileStatus = fs.listStatus(inDir);
            for(FileStatus status : fileStatus){
                System.out.println(status.getPath().toString());

                //reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(status.getPath()), SequenceFile.Reader.bufferSize(4096), SequenceFile.Reader.start(0));
//                Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
//                Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(status.getPath()));
                Text key = new Text();
                BytesWritable value = new BytesWritable();
                //long position = reader.getPosition();
                //reader.seek(position);
                while (reader.next(key, value)) {
                    String syncSeen = reader.syncSeen() ? "*" : "";
                    byte[] bytes = value.copyBytes();
                    System.out.printf("[%s]\t%s\t%s\n", syncSeen, key, bytes.length);

                    String[] paths = key.toString().split("/");
                    String fileName = paths[paths.length - 1];
                    File outputfile = new File(dir + "/" + fileName);

                    InputStream myInputStream = new ByteArrayInputStream(value.copyBytes());
                    BufferedImage bufImg = ImageIO.read(myInputStream);
                    ImageIO.write(bufImg, "jpg", outputfile);
                    System.out.println(" image is written to file.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
