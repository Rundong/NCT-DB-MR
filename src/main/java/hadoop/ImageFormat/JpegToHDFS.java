package hadoop.ImageFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class JpegToHDFS {

    public static void upload(String dir, String hdfsPath) {
        File folder = new File(dir);
        if(!folder.isDirectory()) {
            System.err.println(dir + " is not a folder!");
            System.exit(1);
        }

        try {
            // prepare Hadoop sequence file writer
            Configuration conf = new Configuration();
            conf.set("io.serializations",
                    "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

            FileSystem fs = FileSystem.get(conf);
            Path outDir = new Path(hdfsPath);
            if (fs.exists(outDir)) {
                System.err.println("Directory " + hdfsPath + " already exists!");
                return;
            }

            for (final File jpegFile : Objects.requireNonNull(folder.listFiles())) {
                // load each jpeg file in the folder
                // parse the file name to obtain x,y,z coordinates
                String strTile = jpegFile.getName().split(".jpg")[0];
                String[] dims = strTile.split("_");
                String[] dimx = dims[0].split("-");
                int x = Integer.parseInt(dimx[0]);
                int xDim = Integer.parseInt(dimx[1]) - x;
                String[] dimy = dims[1].split("-");
                int y = Integer.parseInt(dimy[0]);
                int yDim = Integer.parseInt(dimy[1]) - y;
                String[] dimz = dims[2].split("-");
                int z = Integer.parseInt(dimz[0]);
                int zDim = Integer.parseInt(dimz[1]) - z;
                PixelKey pixelKey = new PixelKey(x,y,z,xDim,yDim,zDim);

                // translate jpeg into pixel array format for HDFS
                BufferedImage img = ImageIO.read(jpegFile);
                byte[] pixels = ((DataBufferByte) img.getRaster().getDataBuffer()).getData();
                ByteWritable[] pixelsWritable = new ByteWritable[pixels.length];
                for (int i = 0; i < pixels.length; i++) {
                    pixelsWritable[i] = new ByteWritable(pixels[i]);
                }
                System.out.println("num of pixels: " + pixels.length);

                // write this pixel array to HDFS
                SequenceFile.Writer writer;
                Path path = new Path(hdfsPath + "/" + strTile);
                writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
                        SequenceFile.Writer.keyClass(PixelKey.class), SequenceFile.Writer.valueClass(PixelArrayWritable.class));
                writer.append(pixelKey, new PixelArrayWritable(pixelsWritable));
                IOUtils.closeStream(writer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
                    byte[] pixels = value.copyBytes();
                    System.out.printf("[%s]\t%s\t%s\n", syncSeen, key, pixels.length);

                    String[] dims = key.toString().split("_");
                    String[] dimx = dims[0].split("-");
                    int x = Integer.parseInt(dimx[0]);
                    int xDim = Integer.parseInt(dimx[1]) - x;
                    String[] dimy = dims[1].split("-");
                    int y = Integer.parseInt(dimy[0]);
                    int yDim = Integer.parseInt(dimy[1]) - y;
                    String[] dimz = dims[2].split("-");
                    int z = Integer.parseInt(dimz[0]);
                    int zDim = Integer.parseInt(dimz[1]) - z;
                    BufferedImage img = new BufferedImage(xDim, yDim * zDim, BufferedImage.TYPE_BYTE_GRAY);
                    byte[] data = ((DataBufferByte) img.getRaster().getDataBuffer()).getData();
                    System.arraycopy(pixels, 0, data, 0, pixels.length);

                    ImageIO.write(img, "jpg", new File(
                            dir + "/" + key.toString() + ".jpg"));


                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
