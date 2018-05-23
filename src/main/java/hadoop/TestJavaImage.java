package hadoop;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.GaussianBlur3D;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.Arrays;

public class TestJavaImage {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:sqlite:/home/rundongli/LabWork/nctracer-related/nctracer.db";
        Connection conn = null;
        try {
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to SQLite has been established.");

//                Statement stmt = conn.createStatement();
//                ResultSet rs = stmt.executeQuery(" select image_id, zoom_out, x, y, z, x_dim, y_dim, z_dim, pixels from pix where image_id = 1 and zoom_out=1 and x >= 0 - x_dim and x <= 128");

            // get the info about the pixels of interest
            String[] info = args[0].split(",");
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

            byte[] pix;
            while (rs.next()) {
                pix = rs.getBytes("pixels");
                System.out.println("x_dim = " + rs.getInt("x_dim"));

                // construct a 3D image, in the form of an ImageStack object
                ImageStack imgStack = new ImageStack(width, height, depth);
                for (int iz = 0 ; iz < depth; iz++) {
                    byte[] slicePixels = Arrays.copyOfRange(pix, iz * width * height, (iz + 1) * width * height);
                    imgStack.setPixels(slicePixels, iz + 1);
                }
                ImagePlus imgPlus = new ImagePlus("pix", imgStack);

                // test: take the last layer of the image stack and display it
                ImageStack lastSlice = imgStack.crop(0, 0, imgStack.getSize() - 1, imgStack.getWidth(), imgStack.getHeight(), 1);
                ImagePlus imp = new ImagePlus("last slice", lastSlice);
                BufferedImage bufImg = imp.getBufferedImage();
                LoadAndShow test = new LoadAndShow(bufImg);
                JFrame f = new JFrame();
                f.setTitle("Image: the last layer");
                f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                f.add(new JScrollPane(test));
                f.setSize(256,256);
                f.setLocation(200,200);
                f.setVisible(true);

                // apply filter
                double sigmaX = 1.0, sigmaY = 1.0, sigmaZ = 1.0;
                GaussianBlur3D.blur(imgPlus, sigmaX, sigmaY, sigmaZ);

                // test: show the last layer of the results
                imgPlus.setSlice(depth - 1);
                ImagePlus lastSliceResult = new ImagePlus("last slice result", imgPlus.getStack().crop(0, 0, imgStack.getSize() - 1, imgStack.getWidth(), imgStack.getHeight(), 1));
                BufferedImage bufImgResult = lastSliceResult.getBufferedImage();
                LoadAndShow testRes = new LoadAndShow(bufImgResult);
                JFrame fRes = new JFrame();
                fRes.setTitle("Result: the last layer");
                fRes.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                fRes.add(new JScrollPane(testRes));
                fRes.setSize(256,256);
                fRes.setLocation(500,200);
                fRes.setVisible(true);
            }


            // write results back
//            byte[] pixRes = new byte[width * height * depth];
//                for (int iz = 0 ; iz < zDim; iz++) {
//                    for (int iy = 0 ; iy < yDim; iy++) {
//                        for (int ix = 0 ; ix < xDim ; ix++) {
//                            pixRes[iz * xDim * yDim + iy * xDim + ix] = (byte) imp.getStack().getVoxel(ix, iy, iz);
//                        }
//                    }
//                }



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
