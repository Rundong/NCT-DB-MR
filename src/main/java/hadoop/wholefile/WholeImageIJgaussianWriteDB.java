package hadoop.wholefile;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
import java.util.Arrays;

public class WholeImageIJgaussianWriteDB {

    public static class WholeImageFilterMapper extends
            Mapper<NullWritable, BytesWritable, NullWritable, NullWritable> {

        private double sigmaX, sigmaY, sigmaZ;
        private Text fileNameKey;
        Connection conn = null;
        private int image_id;

        @Override
        protected void setup(Context c) {
            //System.out.println("mapper setup");
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);
            image_id = c.getConfiguration().getInt("imageID", 99);
            //System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);

            // obtain the file path
            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println("Input file path: " + filePathString);
            fileNameKey = new Text(filePathString);

            // initialize db connection
            try {
                // create a connection to the database
                //Class.forName("org.sqlite.JDBC");
                DriverManager.registerDriver(new JDBC());
                //String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
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
            GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);
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
        double sigmaX = Double.parseDouble(args[2]);
        double sigmaY = Double.parseDouble(args[3]);
        double sigmaZ = Double.parseDouble(args[4]);
        String jdbcURL = args[5];
        int image_id = Integer.parseInt(args[6]);
        String image_name = args[7];

        Configuration conf = new Configuration();
        conf.setDouble("sigmax", sigmaX);
        conf.setDouble("sigmay", sigmaY);
        conf.setDouble("sigmaz", sigmaZ);
        conf.set("jdbcURL", jdbcURL);
        conf.setInt("imageID", image_id);
        Job job = Job.getInstance(conf, "WholeImage-IJ_gaussian-WriteDB");
        job.setJarByClass(WholeImageIJgaussianWriteDB.class);

        job.setMapperClass(WholeImageFilterMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        /*// write an entry to the image table in nctracer.db
        try {
            Connection conn = DriverManager.getConnection(jdbcURL);
            System.out.println(" conn: " + conn.toString());
            String sqlInsertPix = "insert into image (id, name) Values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(sqlInsertPix);
            ps.setInt(1, image_id);
            ps.setString(2, image_name);
            ps.executeUpdate();
            System.out.println("Complete writing an entry into image table.");
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }*/
    }

}
