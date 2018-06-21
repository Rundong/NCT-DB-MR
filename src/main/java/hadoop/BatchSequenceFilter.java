package hadoop;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.zookeeper.common.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.Mapper.Context;


public class BatchSequenceFilter {

    public static class SequenceImageFilterMapper extends
            Mapper<Text, BytesWritable, Text, BytesWritable> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) throws IOException, InterruptedException {
            System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);

            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println("file path: " + filePathString);
//            String[] inputlocations = c.getInputSplit().getLocations();
//            System.out.println(" input locations length: " + inputlocations.length);
//            for (String str : inputlocations) {
//                System.out.println(" input split location: " + str);
//            }
            System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        @Override
        public void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            System.out.println("map function: key = " + key.toString());
            // interpret the image data: meta-info (x,y,z,dims) and pixels
            // get the meta-info
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

            // parse the pixel array
            byte[] pix = value.copyBytes();
            //System.out.println(" bytes array length = " + value.getLength());
            int sliceSize = xDim * yDim;
            ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
            for (int iz = 0; iz < zDim; iz++) {
                int zbase = iz * sliceSize;
                byte[] slicePixels = new byte[sliceSize];
                for (int iy = 0; iy < yDim ;iy++) {
                    int sliceIndexBase = iy * xDim;
                    int ybase = zbase + sliceIndexBase;
                    for (int ix = 0; ix < xDim; ix++) {
                        slicePixels[sliceIndexBase] = pix[ybase + ix];
                    }
                }
                imgStack.setPixels(slicePixels, iz + 1);
            }

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", imgStack);
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

            // write results back
            byte[] pixResult = new byte[pix.length];
            for (int iz = 0; iz < zDim; iz++) {
                for (int iy = 0; iy < yDim; iy++) {
                    for (int ix = 0; ix < xDim; ix++) {
                        pixResult[iz * sliceSize + iy * xDim + ix] = (byte)(imgPlus.getStack().getVoxel(ix, iy, iz));
                    }
                }
            }

            context.write(key, new BytesWritable(pixResult));
            //System.out.println(" num pixels: " + pixResult.length);

        }//map function close

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setDouble("sigmax", 1.0);
        conf.setDouble("sigmay", 0.9);
        conf.setDouble("sigmaz", 1.0);
        Job job = Job.getInstance(conf, "Batch Filter Sequence Files");
        job.setJarByClass(BatchSequenceFilter.class);

        job.setMapperClass(SequenceImageFilterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //close of driver class
    }

}
