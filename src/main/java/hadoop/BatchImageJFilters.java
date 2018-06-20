package hadoop;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;

public class BatchImageJFilters {

    public static class QueryDBMapper
            extends Mapper<Object, Text, NullWritable, NullWritable> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) throws IOException, InterruptedException {
            System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);

            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println("file path: " + filePathString);

            String[] inputlocations = c.getInputSplit().getLocations();
            System.out.println(" input locations length: " + inputlocations.length);
            for (String str : inputlocations) {
                System.out.println(" input split location: " + str);
            }
            System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map function");
            // interpret the image data: meta-info (x,y,z,dims) and pixels

            // get the meta-info
            String[] info = value.toString().split(",");
            int imageID = Integer.parseInt(info[0]);
            int zoomOut = Integer.parseInt(info[1]);
            int xStart = Integer.parseInt(info[2]);
            int yStart = Integer.parseInt(info[3]);
            int zStart = Integer.parseInt(info[4]);
            int width = Integer.parseInt(info[5]);
            int height = Integer.parseInt(info[6]);
            int depth = Integer.parseInt(info[7]);
            System.out.println("imageID: " + imageID);
            System.out.println("zoomOut: " + zoomOut);
            System.out.println("x: " + xStart + ", " + (xStart + width));
            System.out.println("y: " + yStart + ", " + (yStart + height));
            System.out.println("z: " + zStart + ", " + (zStart + depth));


            // construct a 3D image, in the form of an ImageStack object
            byte[] pix = null;
            ImageStack imgStack = new ImageStack(width, height, depth);
            for (int iz = 0; iz < depth; iz++) {
                byte[] slicePixels = Arrays.copyOfRange(pix, iz * width * height, (iz + 1) * width * height);
                imgStack.setPixels(slicePixels, iz + 1);
            }

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", imgStack);
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

            // write results back
            byte[] pixRes = new byte[width * height * depth];
            for (int iz = 0; iz < depth; iz++) {
                for (int iy = 0; iy < height; iy++) {
                    for (int ix = 0; ix < width; ix++) {
                        pixRes[iz * width * height + iy * width + ix] = (byte)(imgPlus.getStack().getVoxel(ix, iy, iz));
                    }
                }
            }
            System.out.println("result size = " + pixRes.length);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Batch ImageJ Filter (Query DataBase) in Hadoop");
        job.setJarByClass(BatchImageJFilters.class);
        job.setMapperClass(QueryDBMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
