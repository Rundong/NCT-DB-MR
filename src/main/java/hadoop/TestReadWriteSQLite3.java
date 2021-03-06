package hadoop;

import java.io.IOException;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestReadWriteSQLite3 {

//    public static void connect() {
//        // db parameters
//        String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
//        Connection conn = null;
//        try {
//            // create a connection to the database
//            conn = DriverManager.getConnection(url);
//            System.out.println("Connection to SQLite has been established.");
//
//        } catch (SQLException e) {
//            System.err.println(e.getMessage());
//            System.exit(1);
//        } finally {
//            try {
//                if (conn != null) {
//                    conn.close();
//                }
//            } catch (SQLException ex) {
//                System.out.println(ex.getMessage());
//            }
//        }
//    }

    public static class QueryDBMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        protected void setup(Context c) {        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // db parameters
            String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
            Connection conn = null;
            try {
                // create a connection to the database
                conn = DriverManager.getConnection(url);
                System.out.println("Connection to SQLite has been established.");

//                Statement stmt = conn.createStatement();
//                ResultSet rs = stmt.executeQuery(" select image_id, zoom_out, x, y, z, x_dim, y_dim, z_dim, pixels from pix where image_id = 1 and zoom_out=1 and x >= 0 - x_dim and x <= 128");

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
//                System.out.println("imageID: " + imageID);
//                System.out.println("zoomOut: " + zoomOut);
//                System.out.println("x: " + xStart + ", " + (xStart + width));
//                System.out.println("y: " + yStart + ", " + (yStart + height));
//                System.out.println("z: " + zStart + ", " + (zStart + depth));

                // query the pixels between upper-left-index and lower-right-index
                // SELECT x, y, z from pix where image_id = 1 and zoom_out = 1 and x >= -128 and x <= 0 and y >= 2176 and y <= 2304 and z >= 0 and z <= 64 order by z, y, x;
                String sqlSelectPix = "SELECT x, y, z, x_dim, y_dim, z_dim, pixels from pix " +
                        "where image_id = ? and zoom_out = ? " +
                        "and x >= ? - x_dim and x <= ? " +
                        "and y >= ? - y_dim and y <= ? " +
                        "and z >= ? - z_dim and z <= ? order by z, y, x";
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

                while (rs.next()) {
                    System.out.println("x_dim = " + rs.getInt("x_dim"));
                }

                // write results back
                byte[] pixRes = new byte[width * height * depth];
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
        job.setJarByClass(TestReadWriteSQLite3.class);
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
