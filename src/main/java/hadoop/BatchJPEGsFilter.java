package hadoop;

import ij.ImagePlus;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.zookeeper.common.IOUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;


public class BatchJPEGsFilter {

    public static class ConvImageToSequenceFileMapper extends
            Mapper<Object, Text, NullWritable, Text> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) throws IOException, InterruptedException {
            System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);

            System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //Reading path from the value passed above in map where the image file is present.
            String pathToRead = value.toString();

            Configuration conf=context.getConfiguration();
            Path path = new Path(pathToRead);
            //Creating a FileSystem setup from the above path
            FileSystem fileToRead = FileSystem.get(URI.create(pathToRead), conf);

            //creating a DataInput stream class where  it reads the file and outputs bytes stream
            DataInputStream dis = null;
            try{
                // load jpeg file into java image format
                dis = fileToRead.open(path);
                //File jpegFile = new File(new URI(path.toString()));
                BufferedImage img = ImageIO.read(dis);

                // apply filter
                ImagePlus imgPlus = new ImagePlus("pix", img);
                GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

                //writing the ByteArrayOutputStream bout
                String strTile = path.getName().split(".jpg")[0];
//                context.write(NullWritable.get(), imgPlus.getBufferedImage().);
                System.out.println(strTile);

                //try block close
            } catch (Exception e) {
                e.printStackTrace();
            } finally{
                //final block open

                //the difference between dis.close() and IOUtils.closeStream(dis);
                dis.close();
                IOUtils.closeStream(dis);
                //final block close
            }
        }//map function close

    }//ConvImageToSequenceFileMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Convert Jpegs To Sequence Files");
        job.setJarByClass(BatchJPEGsFilter.class);

        job.setMapperClass(ConvImageToSequenceFileMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //close of driver class
    }

}
