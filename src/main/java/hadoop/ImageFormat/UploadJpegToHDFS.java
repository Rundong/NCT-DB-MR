package hadoop.ImageFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class UploadJpegToHDFS {

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
            SequenceFile.Writer writer;

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

                // write this pixel array to HDFS
                Path path = new Path(hdfsPath + "/" + strTile);
                writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
                        SequenceFile.Writer.keyClass(PixelKey.class), SequenceFile.Writer.valueClass(PixelArrayWritable.class));
                writer.append(pixelKey, new PixelArrayWritable(pixelsWritable));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
