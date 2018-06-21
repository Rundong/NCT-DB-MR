package hadoop;

import hadoop.ImageFormat.PixelArrayWritable;
import hadoop.ImageFormat.PixelKey;
import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * For unknown reason, map function does not run, although the setup function runs.
 */

public class BatchHdfsImageJFilters {

    public static class BatchHdfsMapper
            extends Mapper<PixelKey, PixelArrayWritable, PixelKey, PixelArrayWritable> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) throws IOException, InterruptedException {
            System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);

            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println(" file path: " + filePathString);
//            String[] inputlocations = c.getInputSplit().getLocations();
//            System.out.println(" input locations length: " + inputlocations.length);
//            for (String str : inputlocations) {
//                System.out.println(" input split location: " + str);
//            }
            System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        public void map(PixelKey key, PixelArrayWritable value, Context context) throws IOException, InterruptedException {
            // interpret the image data: meta-info (x,y,z,dims) and pixels
            // get the meta-info
            System.out.println("map function");
            System.out.println("x: " + key.x + ", " + (key.x + key.x_dim));

            // construct a 3D image, in the form of an ImageStack object
            ByteWritable[] pix = value.get();
            int sliceSize = key.x_dim * key.y_dim;
            ImageStack imgStack = new ImageStack(key.x_dim, key.y_dim, key.z_dim);
            for (int iz = 0; iz < key.z_dim; iz++) {
                int zbase = iz * sliceSize;
                byte[] slicePixels = new byte[sliceSize];
                for (int iy = 0; iy < key.y_dim ;iy++) {
                    int sliceIndexBase = iy * key.x_dim;
                    int ybase = zbase + sliceIndexBase;
                    for (int ix = 0; ix < key.x_dim; ix++) {
                        slicePixels[sliceIndexBase] = pix[ybase + ix].get();
                    }
                }
                imgStack.setPixels(slicePixels, iz + 1);
            }

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", imgStack);
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

            // write results back
            ByteWritable[] pixelsWritable = new ByteWritable[pix.length];
            for (int iz = 0; iz < key.z_dim; iz++) {
                for (int iy = 0; iy < key.y_dim; iy++) {
                    for (int ix = 0; ix < key.x_dim; ix++) {
                        pixelsWritable[iz * sliceSize + iy * key.x_dim + ix] = new ByteWritable((byte)(imgPlus.getStack().getVoxel(ix, iy, iz)));
                    }
                }
            }

            context.write(key, new PixelArrayWritable(pixelsWritable));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.setDouble("sigmax", 1.0);
        conf.setDouble("sigmay", 0.9);
        conf.setDouble("sigmaz", 1.0);
        Job job = Job.getInstance(conf, "Batch ImageJ filter in Hadoop (HDFS)");
        job.setJarByClass(BatchHdfsImageJFilters.class);

        job.setMapperClass(BatchHdfsMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        job.setMapperClass(BatchHdfsMapper.class);
//        job.setMapOutputKeyClass(PixelKey.class);
//        job.setMapOutputValueClass(PixelArrayWritable.class);
//        job.setNumReduceTasks(0);
//        job.setOutputKeyClass(PixelKey.class);
//        job.setOutputValueClass(PixelArrayWritable.class);
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        System.out.println("intput file: " + args[0]);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
////        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, BatchHdfsMapper.class);
////        MapFileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
