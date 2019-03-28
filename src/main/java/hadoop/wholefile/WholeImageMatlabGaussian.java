package hadoop.wholefile;

import Gaussian3dT.Class1;
import com.mathworks.toolbox.javabuilder.MWArray;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.sqlite.JDBC;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class WholeImageMatlabGaussian {

    public static class WholeImageFilterMapper extends
            Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {

        private double sigmaX, sigmaY, sigmaZ;
        private Text fileNameKey;

        @Override
        protected void setup(Context c) {
            //System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);
            //System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);

            // obtain the file path
            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println("Input file path: " + filePathString);
            fileNameKey = new Text(filePathString);
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            //System.out.println("map function: key = " + key.toString() + ", image byte length = " + value.getLength());

            // interpret the image data: meta-info (x,y,z,dims) and pixels
            String[] path = fileNameKey.toString().split("/");
            String name = path[path.length - 1].split(".jpg")[0];
            String[] dims = name.split("_");
            String[] dimx = dims[0].split("-");
            int x = Integer.parseInt(dimx[0]);
            int xDim = Integer.parseInt(dimx[1]) - x;
            String[] dimy = dims[1].split("-");
            int y = Integer.parseInt(dimy[0]);
            int yDim = Integer.parseInt(dimy[1]) - y;
            String[] dimz = dims[2].split("-");
            int z = Integer.parseInt(dimz[0]);
            int zDim = Integer.parseInt(dimz[1]) - z;
            System.out.println(" meta data pased: " + name + " --> " + x + "," + y + "," + z
                    + " with dims = " + xDim + "," + yDim + "," + zDim);

            // parse the jpeg file into java image
            InputStream myInputStream = new ByteArrayInputStream(value.copyBytes());
            BufferedImage bufImg = ImageIO.read(myInputStream);

            // convert to MWNumericArray
            byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
            System.out.println(" obtained pixels from buffered image");
            MWNumericArray mwPixels = MWNumericArray.newInstance(new int[]{xDim, yDim, zDim}, pixels, MWClassID.UINT8);

            // apply filter
            Class1 gaussian3dT = null;
            byte[] pixRes = null;
            Object[] matlabResult = null;
            try {
                gaussian3dT = new Class1();
                /*gaussian3dT.Gaussian3dT(new Object[]{mwPixelsResult}, new Object[]{mwPixels, 3.0}); // todo: exception
                byte[] pixRes = (byte[])mwPixelsResult.getData();
                System.out.println(pixRes.length);*/
                matlabResult = gaussian3dT.Gaussian3dT(1, mwPixels, sigmaX);
                pixRes = ((MWNumericArray)matlabResult[0]).getByteData();
                //System.out.println(" done filtering");
            } catch (MWException e) {
                e.printStackTrace();
            } finally {
                // Free native resources
                MWArray.disposeArray(mwPixels);
                MWArray.disposeArray(matlabResult);
                gaussian3dT.dispose();
            }

            System.out.println(" filtered: pixRes size is " + pixRes.length);

            context.write(NullWritable.get(), new BytesWritable(pixRes));

            System.out.println(" one image written.");

        }//map function close

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        args = parser.getRemainingArgs();
        double sigmaX = Double.parseDouble(args[2]);
        double sigmaY = Double.parseDouble(args[3]);
        double sigmaZ = Double.parseDouble(args[4]);

        conf.setDouble("sigmax", sigmaX);
        conf.setDouble("sigmay", sigmaY);
        conf.setDouble("sigmaz", sigmaZ);
        Job job = Job.getInstance(conf, "WholeImage-MATLAB_gaussian");
        job.setJarByClass(WholeImageMatlabGaussian.class);

        job.setMapperClass(WholeImageFilterMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
