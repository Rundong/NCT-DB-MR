package sequential.matlabplugin;

import initracer.Class1;
import com.mathworks.toolbox.javabuilder.MWArray;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;


public class Inittracer_Test {
    public static void run() throws Exception {
        String path = "./testData/original-subimage-last3slices.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 3;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");

        MWNumericArray mwPixels = MWNumericArray.newInstance(new int[]{xDim, yDim, zDim}, pixels, MWClassID.DOUBLE);
        double c = 0, ransac_iteration = 1, ransac_ratio = 0.9, p_threshold = 0.9;

        Class1 initracer = new Class1();
        Object[] matlabResult = initracer.initracer(2, mwPixels, c, ransac_iteration, ransac_ratio, p_threshold);
        double[] r = ((MWNumericArray)matlabResult[0]).getDoubleData();
        double[] pixRes = ((MWNumericArray)matlabResult[1]).getDoubleData();
        System.out.println(r.length);
        for(double value : r) {
            System.out.print(value + ", ");
        }
    }

    public static void main(String[] args) throws Exception {
        /*String classpath = System.getProperty("java.class.path");
        System.out.println(classpath);*/
        run();
    }
}
