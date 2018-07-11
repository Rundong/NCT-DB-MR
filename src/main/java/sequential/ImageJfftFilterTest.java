package sequential;

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.Prefs;
import ij.plugin.filter.FFTFilter;
import ij.process.ImageProcessor;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;

public class ImageJfftFilterTest  {

    public static void callMyFFT(ImagePlus imgPlus, int zDim) {
        ImageJfftFilter fftFilter = new ImageJfftFilter();
        fftFilter.setup("arg?", imgPlus);
//        fftFilter.run(imgPlus.getProcessor());
        for (int slice = 1; slice <= zDim; slice++) {
//            imgPlus.setPosition(slice); // the same effect as setSlice()
            imgPlus.setSlice(slice); // the same effect as setPosition()
            ImageProcessor ip = imgPlus.getProcessor();
            fftFilter.run(ip);
        }
    }

    public static void callIJFFT(ImagePlus imgPlus, int zDim) {
        FFTFilter ijFFT = new FFTFilter();
        String args = "filterLargeDia=40 filterSmallDia=10 processStack";
        ijFFT.setup(args, imgPlus);
        ijFFT.run(imgPlus.getProcessor());
    }

    public static void applyTo3D() throws Exception {
//        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
        String path = "./testData/original-subimage-last3slices.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 3;
        int sliceSize = xDim * yDim;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");
        ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
        for (int iz = 1; iz <= zDim; iz++) {
            imgStack.setPixels(Arrays.copyOfRange(pixels, (iz-1) * sliceSize, (iz) * sliceSize), iz);
        }

        // apply filter
        ImagePlus imgPlus = new ImagePlus("pix", imgStack);
//        callIJFFT(imgPlus, zDim);
        callMyFFT(imgPlus, zDim);

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
        File outputfile = new File("./testData/myfft-result-last3slices.jpg");
        ImageIO.write(bufImgRes, "jpg", outputfile);
    }

    public static void applyTo2D() throws Exception {
//        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
//        BufferedImage bufImg = ImageIO.read(new File(path));
//
//        // crop and save original image
//        bufImg = bufImg.getSubimage(0, 8064, 128, 128);
//        File outputOriginal = new File("./testData/original-subimage.jpg");
//        ImageIO.write(bufImg, "jpg", outputOriginal);

        String path = "./testData/original-subimage.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // apply filter
        ImagePlus imgPlus = new ImagePlus("2D", bufImg);
        ImageProcessor ip = imgPlus.getProcessor();
        ImageJfftFilter fftFilter = new ImageJfftFilter();
        fftFilter.setup("arg?", imgPlus);
        fftFilter.filter(ip);
//        FFTFilter fftFilter = new FFTFilter();
//        fftFilter.setup("ijFFTarg?", imgPlus);
//        fftFilter.run(ip);

        BufferedImage bufImgRes = imgPlus.getBufferedImage();
        System.out.println(" done constructing buffered image result");

        // save image
        File outputfile = new File("./testData/fft-result.jpg");
        ImageIO.write(bufImgRes, "jpg", outputfile);

//        // show the result
//        ImageJfftFilterTest test = new ImageJfftFilterTest(bufImgRes);
//        JFrame f = new JFrame();
//        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        f.add(new JScrollPane(test));
//        f.setSize(400,400);
//        f.setLocation(200,200);
//        f.setVisible(true);
    }

    public static void main(String[] args) throws Exception {
        applyTo3D();
//        applyTo2D();

        // crop and save original image
//        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
//        BufferedImage bufImg = ImageIO.read(new File(path));
//        bufImg = bufImg.getSubimage(0, 7808, 128, 384);
//        File outputOriginal = new File("./testData/original-subimage-last3slices.jpg");
//        ImageIO.write(bufImg, "jpg", outputOriginal);
    }

}
