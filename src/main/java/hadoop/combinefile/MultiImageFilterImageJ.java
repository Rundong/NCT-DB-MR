package hadoop.combinefile;

import hadoop.wholefile.WholeFileInputFormat;
import ij.ImagePlus;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class MultiImageFilterImageJ {

    public static class MultiImageFilterMapper extends
            Mapper<Text, BytesWritable, Text, BytesWritable> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) throws IOException, InterruptedException {
            //System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);
            //System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        @Override
        public void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            //System.out.println("map function: key = " + key.toString() + ", image byte length = " + value.getLength());

            // parse the jpeg file into java image
            InputStream myInputStream = new ByteArrayInputStream(value.copyBytes());
            BufferedImage bufImg = ImageIO.read(myInputStream);

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", bufImg);
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

            // write result image back to jpeg byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(imgPlus.getBufferedImage(), "jpg", baos);
            baos.flush();
            byte[] imageInByte = baos.toByteArray();
            baos.close();

            context.write(key, new BytesWritable(imageInByte));
            //System.out.println(" num bytes: " + imageInByte.length);

        }//map function close

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setDouble("sigmax", 1.0);
        conf.setDouble("sigmay", 0.9);
        conf.setDouble("sigmaz", 1.0);
        Job job = Job.getInstance(conf, "MultiImageFilterImageJ");
        job.setJarByClass(MultiImageFilterImageJ.class);

        job.setMapperClass(MultiImageFilterMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(CombineWholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //close of driver class
    }

}
