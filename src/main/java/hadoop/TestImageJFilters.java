package hadoop;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;

public class TestImageJFilters {

    public static class QueryDBMapper
            extends Mapper<Object, Text, NullWritable, NullWritable> {

        private double sigmaX, sigmaY, sigmaZ;

        protected void setup(Context c) {
            sigmaX = c.getConfiguration().getDouble("sigmax", 1.0);
            sigmaY = c.getConfiguration().getDouble("sigmay", 1.0);
            sigmaZ = c.getConfiguration().getDouble("sigmaz", 1.0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // db parameters
            String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
            Connection conn = null;
            try {
                // create a connection to the database
                conn = DriverManager.getConnection(url);
                System.out.println("Connection to SQLite has been established.");

                // get the info about the pixels of interest
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

                // SELECT x, y, z from pix where image_id = 1 and zoom_out = 1 and x > 640 and x < 896 and y > 384 and y < 640 and z > -64 and z < 64 order by z, y, x;
                String sqlSelectPix = "SELECT x, y, z, x_dim, y_dim, z_dim, pixels from pix " +
                        "where image_id = ? and zoom_out = ? " +
                        "and x > ? - x_dim and x < ? " +
                        "and y > ? - y_dim and y < ? " +
                        "and z > ? - z_dim and z < ? order by z, y, x";
                PreparedStatement ps = conn.prepareStatement(sqlSelectPix);
                int i = 1;
                ps.setInt(i++, imageID);
                ps.setInt(i++, zoomOut);
                ps.setInt(i++, xStart);
                ps.setInt(i++, xStart + width);
                ps.setInt(i++, yStart);
                ps.setInt(i++, yStart + height);
                ps.setInt(i++, zStart);
                ps.setInt(i, zStart + depth);
                ResultSet rs = ps.executeQuery();

                byte[] pix = null;
                if (rs.next()) { // TODO: process multiple blobs, i.e., use while(...) instead of if(...)
                    pix = rs.getBytes("pixels");
                    System.out.println("x_dim = " + rs.getInt("x_dim"));
                }
                assert(pix != null);

                // construct a 3D image, in the form of an ImageStack object
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

                String sqlInsertPix = "insert into pix (image_id, name, zoom_out, x, y, z, x_dim, y_dim, z_dim, pixels) " +
                        "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                ps = conn.prepareStatement(sqlInsertPix);
                i = 1;
                ps.setInt(i++, 2); // hard code image_id = 2, just for testing
                ps.setString(i++, "mapreduce");
                ps.setInt(i++, zoomOut);
                ps.setInt(i++, xStart);
                ps.setInt(i++, yStart);
                ps.setInt(i++, zStart);
                ps.setInt(i++, width);
                ps.setInt(i++, height);
                ps.setInt(i++, depth);
                ps.setBytes(i, pixRes);
                ps.executeUpdate();

            } catch (SQLException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                System.exit(1);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reflection (MatLab jar) in Hadoop");
        job.setJarByClass(TestImageJFilters.class);
        job.setMapperClass(QueryDBMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
