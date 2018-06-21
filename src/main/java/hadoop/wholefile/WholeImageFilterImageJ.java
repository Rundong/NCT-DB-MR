package hadoop.wholefile;

import hadoop.BatchSequenceFilter;
import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class WholeImageFilterImageJ {

    public static class WholeImageFilterMapper extends
            Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private double sigmaX, sigmaY, sigmaZ;
        private Text fileNameKey;

        protected void setup(Context c) throws IOException, InterruptedException {
            System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);

            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println(" file path: " + filePathString);
            fileNameKey = new Text(filePathString);
//            String[] inputlocations = c.getInputSplit().getLocations();
//            System.out.println(" input locations length: " + inputlocations.length);
//            for (String str : inputlocations) {
//                System.out.println(" input split location: " + str);
//            }
            System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            System.out.println("map function: key = " + key.toString() + ", image byte length = " + value.getLength());

            // interpret the image data: meta-info (x,y,z,dims) and pixels
//            String[] paths = fileNameKey.toString().split("/");
//            String[] dims = paths[paths.length - 1].split(".jpg")[0].split("_");
//            System.out.println(" meta data pased: " + paths[paths.length - 1]);
//            String[] dimx = dims[0].split("-");
//            int x = Integer.parseInt(dimx[0]);
//            int xDim = Integer.parseInt(dimx[1]) - x;
//            String[] dimy = dims[1].split("-");
//            int y = Integer.parseInt(dimy[0]);
//            int yDim = Integer.parseInt(dimy[1]) - y;
//            String[] dimz = dims[2].split("-");
//            int z = Integer.parseInt(dimz[0]);
//            int zDim = Integer.parseInt(dimz[1]) - z;

            // parse the jpeg file into java image
            InputStream myInputStream = new ByteArrayInputStream(value.copyBytes());
            BufferedImage bufImg = ImageIO.read(myInputStream);

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", bufImg);
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

//            // write results back as pixel array
//            int sliceSize = xDim * yDim;
//            byte[] pixResult = new byte[zDim * sliceSize];
//            for (int iz = 0; iz < zDim; iz++) {
//                for (int iy = 0; iy < yDim; iy++) {
//                    for (int ix = 0; ix < xDim; ix++) {
//                        pixResult[iz * sliceSize + iy * xDim + ix] = (byte)(imgPlus.getStack().getVoxel(ix, iy, iz));
//                    }
//                }
//            }

            // write result image back to jpeg byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(imgPlus.getBufferedImage(), "jpg", baos);
            baos.flush();
            byte[] imageInByte = baos.toByteArray();
            baos.close();

            context.write(fileNameKey, new BytesWritable(imageInByte));
            System.out.println(" num bytes: " + imageInByte.length);

        }//map function close

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setDouble("sigmax", 1.0);
        conf.setDouble("sigmay", 0.9);
        conf.setDouble("sigmaz", 1.0);
        Job job = Job.getInstance(conf, "WholeImageFilterImageJ");
        job.setJarByClass(WholeImageFilterImageJ.class);

        job.setMapperClass(WholeImageFilterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //close of driver class
    }

}
