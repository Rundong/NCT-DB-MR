package hadoop.ImageFormat;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.zookeeper.common.IOUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.imageio.ImageIO;


public class JpegToSequence {

    public static class ConvImageToSequenceFileMapper extends
            Mapper<Object, Text,Text,BytesWritable> {

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
                //try block open

//                dis = fileToRead.open(path);
//
//                /*Because you don't know the image size that is the reason why we are creating a
//                 byte array of max size to read all at once instead of reading single byte
//                 to save read time and resources */
//                byte tempBuffer[] = new byte[1024 * 1024];
//                int len = 0;
//
//                ByteArrayOutputStream bout = new ByteArrayOutputStream();
//
//                /*dis variable reads bytes into buffer starting from zero and to max length
//                 until end of file which  returns a -1 once it does while breaks */
//                while((len = dis.read(tempBuffer, 0, tempBuffer.length)) >= 0)
//                {
//                    /*if tempBuffer is full and still dis is reading because it didn't receive end
//                     of file as -1 is not encountered then the ByteArrayOutputStream bout
//                     need to be added with remaining bytes until end of file  */
//                    System.out.println("write " + len + " bytes");
//                    bout.write(tempBuffer);
//
//                }
//                /*why can't we write the below line if dis is done reading before the
//                 * buffer size is full */
//                //context.write(value,new BytesWritable(tempBuffer.clone()));
//
//                //writing the ByteArrayOutputStream bout
//                String strTile = path.getName().split(".jpg")[0];
//                context.write(new Text(strTile), new BytesWritable(bout.toByteArray()));
//                System.out.println(strTile);

                // load jpeg file into java image format
                dis = fileToRead.open(path);
                //File jpegFile = new File(new URI(path.toString()));
                BufferedImage img = ImageIO.read(dis);
                byte[] pixels = ((DataBufferByte) img.getRaster().getDataBuffer()).getData();
                System.out.println("num of pixels: " + pixels.length);

                //writing the ByteArrayOutputStream bout
                String strTile = path.getName().split(".jpg")[0];
                context.write(new Text(strTile), new BytesWritable(pixels));
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
        job.setJarByClass(JpegToSequence.class);

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
