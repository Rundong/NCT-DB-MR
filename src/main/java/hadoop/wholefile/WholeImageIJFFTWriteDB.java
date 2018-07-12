package hadoop.wholefile;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import ij.process.ImageProcessor;
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
import sequential.ImageJfftFilter;

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
import java.util.Arrays;

public class WholeImageIJFFTWriteDB {

    public static class WholeImageFilterMapper extends
            Mapper<NullWritable, BytesWritable, NullWritable, NullWritable> {

        private ImageJfftFilter fftFilter;
        private Text fileNameKey;
        Connection conn = null;
        private int image_id;

        @Override
        protected void setup(Context c) {
            //System.out.println("mapper setup");
            fftFilter = new ImageJfftFilter();
            ImageJfftFilter.filterLargeDia = c.getConfiguration().getDouble("largeDia", 40.1);
            ImageJfftFilter.filterSmallDia = c.getConfiguration().getDouble("smallDia", 3.0);
            ImageJfftFilter.choiceDia = c.getConfiguration().get("choiceDia", "None");
            ImageJfftFilter.toleranceDia = c.getConfiguration().getDouble("toleranceDia", 5.0);
            ImageJfftFilter.doScalingDia = c.getConfiguration().getBoolean("doScalingDia", true);
            ImageJfftFilter.saturateDia = c.getConfiguration().getBoolean("saturateDia", true);
            image_id = c.getConfiguration().getInt("imageID", 99);

            // obtain the file path
            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println("Input file path: " + filePathString);
            fileNameKey = new Text(filePathString);

            // initialize db connection
            try {
                // create a connection to the database
                //Class.forName("org.sqlite.JDBC");
                DriverManager.registerDriver(new JDBC());
                String url = c.getConfiguration().get("jdbcURL");
                conn = DriverManager.getConnection(url);
                System.out.println(" Connection to SQLite has been established.");
            } catch (SQLException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                System.exit(1);
            } /*catch (ClassNotFoundException e) {
                e.printStackTrace();
            }*/
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException {
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

            // convert to 3D
            int sliceSize = xDim * yDim;
            byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
            System.out.println(" obtained pixels from buffered image");
            ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
            for (int iz = 0; iz < zDim; iz++) {
                imgStack.setPixels(Arrays.copyOfRange(pixels, iz * sliceSize, (iz + 1) * sliceSize), iz + 1);
            }

            // apply filter
            ImagePlus imgPlus = new ImagePlus("pix", imgStack);
            fftFilter.setup("FFT-arg", imgPlus);
            for (int slice = 1; slice <= zDim; slice++) {
                imgPlus.setSlice(slice);
                ImageProcessor ip = imgPlus.getProcessor();
                fftFilter.run(ip);
            }
            //System.out.println(" done filtering");

            // convert 3D array into 1D array
            byte[] pixRes = new byte[zDim * sliceSize];
            //System.out.println(" pixel length = " + pixRes.length);
            /*byte[][] imgArray = (byte[][]) (imgPlus.getStack().getImageArray());
            System.out.println(" obtained slices");*/
            for (int iz = 0; iz < zDim; iz++) {
                /*System.out.print(" slice " + iz + ": ");
                System.arraycopy(imgArray[iz], 0, pixRes, iz * sliceSize, sliceSize);*/
                for (int iy = 0; iy < yDim; iy++) {
                    for (int ix = 0; ix < xDim; ix++) {
                        int pixIndex = iz * sliceSize + iy * xDim + ix;
                        pixRes[pixIndex] = (byte)(imgPlus.getStack().getVoxel(ix, iy, iz));
                    }
                }
            }
            System.out.println(" done converting filtered image into 1D pixel array");

            try {
                // write results to DB
                String sqlInsertPix = "insert into pix (image_id, name, zoom_out, x, y, z, x_dim, y_dim, z_dim, pixels) " +
                        "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement ps = conn.prepareStatement(sqlInsertPix);
                int i = 1;
                ps.setInt(i++, image_id); // hard code image_id = 2, just for testing
                ps.setString(i++, path[path.length - 1].split(".jpg")[0]);
                ps.setInt(i++, 1); // filter should always be applied to zoom out level 1, i.e., original images
                ps.setInt(i++, x);
                ps.setInt(i++, y);
                ps.setInt(i++, z);
                ps.setInt(i++, xDim);
                ps.setInt(i++, yDim);
                ps.setInt(i++, zDim);
                ps.setBytes(i, pixRes);
                ps.executeUpdate();
                System.out.println(" finish writing filtered stack " + path[path.length - 1]);

            } catch (SQLException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }

        }//map function close

        @Override
        protected void cleanup(Context c) {
            try {
                System.out.println(" conn: " + conn.toString());
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (extraArgs.length < 11) {
            System.out.println("There should be 6 filter parameters and 3 database arguments.");
            System.exit(-1);
        }
        conf.setDouble("largeDia", Double.parseDouble(args[2]));
        conf.setDouble("smallDia", Double.parseDouble(args[3]));
        conf.set("choiceDia", args[4]);
        conf.setDouble("toleranceDia", Double.parseDouble(args[5]));
        conf.setBoolean("doScalingDia", Boolean.parseBoolean(args[6]));
        conf.setBoolean("saturateDia", Boolean.parseBoolean(args[7]));
        int image_id = Integer.parseInt(args[8]);
        String image_name = args[9];
        String jdbcURL = args[10];
        conf.setInt("imageID", image_id);
        conf.set("imageName", image_name);
        conf.set("jdbcURL", jdbcURL);

        Job job = Job.getInstance(conf, "WholeImage-IJ_FFT-WriteDB");
        job.setJarByClass(WholeImageIJFFTWriteDB.class);

        job.setMapperClass(WholeImageFilterMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
