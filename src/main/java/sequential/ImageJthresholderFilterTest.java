package sequential;

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.Prefs;
import ij.process.ImageProcessor;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.util.Arrays;

public class ImageJthresholderFilterTest  {

    public static void applyTo3D() throws Exception {
        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
//        String path = "./testData/original-subimage-last3slices.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 64;
        int sliceSize = xDim * yDim;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");
        ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
        for (int iz = 1; iz <= zDim; iz++) {
            imgStack.setPixels(Arrays.copyOfRange(pixels, (iz-1) * sliceSize, (iz) * sliceSize), iz);
        }

        // apply filter
        ImagePlus imgPlus = new ImagePlus("pix", imgStack);

//        ij.process.ImageStatistics.getStatistics(ip);
//        ij.plugin.Thresholder thresholder = new ij.plugin.Thresholder();
//        thresholder.run();

        //IJ.run(imgPlus, "HSB Stack", "");
        for (int slice = 1; slice <= zDim; slice++) {
            /* https://stackoverflow.com/questions/30981006/imagej-plugin-java-auto-threshold-method-doesnt-work */
            imgPlus.setSlice(slice);
            IJ.setAutoThreshold(imgPlus, "Huang dark");
//            Prefs.blackBackground = true;
//            IJ.run(imgPlus, "Convert to Mask", "only"); //
            IJ.run(imgPlus, "Threshold", "only");
        }

        // convert 3D array into 1D array
        byte[] pixRes = new byte[zDim * sliceSize];
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

        // convert filtered 3D image to 2D image
        DataBuffer buffer = new DataBufferByte(pixRes, pixRes.length);
        WritableRaster raster = Raster.createInterleavedRaster(buffer,
                xDim, yDim * zDim, xDim, 1, new int[]{0}, null);
        ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
        ColorModel cm = new ComponentColorModel(cs, false, true,
                Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
        BufferedImage bufImgRes = new BufferedImage(cm, raster, false, null);
        System.out.println(" done constructing buffered image result");

        // save image
        File outputfile = new File("./testData/thresholder-result-last3slices.jpg");
        ImageIO.write(bufImgRes, "jpg", outputfile);
    }

    public static void main(String[] args) throws Exception {
        applyTo3D();
    }

}
