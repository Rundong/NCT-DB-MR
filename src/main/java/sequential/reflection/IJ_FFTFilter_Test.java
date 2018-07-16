package sequential.reflection;

import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ImageProcessor;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Objects;

public class IJ_FFTFilter_Test {

    public static String classURL = "file:///Users/RundongL/Softwares/ij.jar";
    public static String className = "ij.plugin.filter.FFTFilter";
    public static Class[] constructorArgTypes = new Class[]{};
    public static Object[] constructorArgValues = new Object[]{};
    public static String mSetupName = "setup";
    public static Class[] methodArgTypes = new Class[]{String.class, ImagePlus.class};

    // method run() in FFJFilter
    public static String mRunName = "run";
    public static Class[] mRunArgTypes = new Class[]{ImageProcessor.class};


    public static void callIJFFT(ImagePlus imgPlus, int zDim) {
        Object[] methodArgValues = new Object[]{"", imgPlus};
        Object[] mRunArgValues = new Object[]{imgPlus.getProcessor()};

        Object returnValue;
        try {
            Class<?> aClass = Utils.getClass(classURL, className);
            Object objFFT = Utils.getInstance(aClass, constructorArgTypes, constructorArgValues);
            Method methodSetup = Utils.getMethod(aClass, mSetupName, methodArgTypes);
            returnValue = Utils.callMethod(objFFT, Objects.requireNonNull(methodSetup), methodArgValues);
//            returnValue = CallAMethod.callMethodFromClass(classURL, className, constructorArgTypes, constructorArgValues,
//                    methodSetup, methodArgTypes, methodArgValues);
//            System.out.println("FFTFilter setup() returnValue = " + returnValue);
            if (Boolean.FALSE.equals(returnValue)) {
                System.err.println(methodSetup + " does not complete correctly!");
            }

            // filter each slice
            Method methodRun = Utils.getMethod(aClass, mRunName, mRunArgTypes);
            for (int slice = 1; slice <= zDim; slice++) {
                imgPlus.setSlice(slice); // the same effect as setPosition()
                mRunArgValues[0] = imgPlus.getProcessor();
                Utils.callMethod(objFFT, Objects.requireNonNull(methodRun), mRunArgValues);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        callIJFFT(imgPlus, zDim);

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
        File outputfile = new File("./testData/IJ_FFTFilter-result-last3slices.jpg");
        ImageIO.write(bufImgRes, "jpg", outputfile);
    }

    public static void main(String[] args) throws Exception {
        applyTo3D();
    }

}
