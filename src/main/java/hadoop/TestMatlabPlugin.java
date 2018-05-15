package hadoop;

import java.io.IOException;
import java.sql.*;
import ij.*;
import ij.plugin.GaussianBlur3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMatlabPlugin {

    public static void connect() {
        // db parameters
        String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
        Connection conn = null;
        try {
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            System.err.println(e.getMessage());
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

    public static class QueryDBMapper
            extends Mapper<Object, Text, Text, IntWritable> {

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
                int xStart = Integer.parseInt(info[1]);
                int yStart = Integer.parseInt(info[2]);
                int zStart = Integer.parseInt(info[3]);
                int xDim = Integer.parseInt(info[4]);
                int yDim = Integer.parseInt(info[5]);
                int zDim = Integer.parseInt(info[6]);
                int zoomOut = Integer.parseInt(info[7]);

                // query the pixels between upper-left-index and lower-right-index
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
                ps.setInt(i++, xStart + xDim);
                ps.setInt(i++, yStart);
                ps.setInt(i++, yStart + yDim);
                ps.setInt(i++, zStart);
                ps.setInt(i++, zStart + zDim);
                ResultSet rs = ps.executeQuery(sqlSelectPix);
                final byte[] pix = rs.getBytes("pixels");

                // construct a 3D image and apply filter
                // TODO: transform pixel into image object that ImageJ can process
                ImageStack imstck = new ImageStack(xDim, yDim, zDim);
                for (int iz = 0 ; iz < zDim; iz++) {
                    for (int iy = 0 ; iy < yDim; iy++) {
                        for (int ix = 0 ; ix < xDim ; ix++) {
                            double voxel = pix[iz * xDim * yDim + iy * xDim + ix];
                            imstck.setVoxel(ix, iy, iz, voxel);
                        }
                    }
                }
                ImagePlus imp = new ImagePlus("pix", imstck);
                GaussianBlur3D.blur(imp, sigmaX, sigmaY, sigmaZ);

                // write results back
                byte[] pixRes = new byte[pix.length];
                for (int iz = 0 ; iz < zDim; iz++) {
                    for (int iy = 0 ; iy < yDim; iy++) {
                        for (int ix = 0 ; ix < xDim ; ix++) {
                            pixRes[iz * xDim * yDim + iy * xDim + ix] = (byte) imp.getStack().getVoxel(ix, iy, iz);
                        }
                    }
                }

                String sqlInsertPix = "insert into pix (image_id, zoom_out, x, y, z, x_dim, y_dim, z_dim, pixels) " +
                        "Values (2, ?, ?, ?, ?, ?, ?, ?, ?)";
                ps = conn.prepareStatement(sqlInsertPix);
                i = 1;
                ps.setInt(i++, zoomOut);
                ps.setInt(i++, xStart);
                ps.setInt(i++, yStart);
                ps.setInt(i++, zStart);
                ps.setInt(i++, xDim);
                ps.setInt(i++, yDim);
                ps.setInt(i++, zDim);
                ps.setBytes(i++, pixRes);
                ps.executeQuery(sqlInsertPix);

            } catch (SQLException e) {
                System.err.println(e.getMessage());
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

            context.write(new Text("connected to db"), new IntWritable(1));
        }
    }

    public static class WriteToDBReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reflection (MatLab jar) in Hadoop");
        job.setJarByClass(TestMatlabPlugin.class);
        job.setMapperClass(QueryDBMapper.class);
        job.setCombinerClass(WriteToDBReducer.class);
        job.setReducerClass(WriteToDBReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
