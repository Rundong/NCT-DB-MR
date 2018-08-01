package sequential.matlabplugin;

import Gaussian3dT.Class1;
import com.mathworks.toolbox.javabuilder.*;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;


public class Gaussian3dT_Test {
    public static void applyTo3D() throws Exception {
        double[] sigmas = new double[]{2.0, 1.0, 3.0};
//        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
        String path = "./testData/original-subimage-last3slices.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 3;
        int sliceSize = xDim * yDim;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");

        /*byte[][][] pix3d = new byte[zDim][xDim][yDim];
        for(int iz = 0; iz < zDim; iz++) {
            for(int iy = 0; iy < yDim; iy++) {
                System.arraycopy(pixels, iz * sliceSize + iy * xDim, pix3d[iz][iy], 0, xDim);
            }
        }
        MWNumericArray mwPixels = MWNumericArray.newInstance(new int[]{xDim, yDim, zDim}, pix3d, MWClassID.UINT8);*/

        // prepare the image and the filter paramters
        MWNumericArray mwPixels = MWNumericArray.newInstance(new int[]{xDim, yDim, zDim}, pixels, MWClassID.UINT8);
        MWNumericArray mwSigmas = MWNumericArray.newInstance(new int[]{3}, sigmas, MWClassID.DOUBLE);

        Class1 gaussian3dT = new Class1();
        MWNumericArray mwPixelsResult = new MWNumericArray(new int[]{xDim, yDim, zDim}, MWClassID.UINT8);
        gaussian3dT.Gaussian3dT(new Object[]{mwPixelsResult}, new Object[]{mwPixels, mwSigmas}); // todo: exception
        byte[] pixRes = (byte[])mwPixelsResult.toByteArray();
//        Object[] matlabResult = gaussian3dT.Gaussian3dT(1, mwPixels, mwSigmas);
//        byte[] pixRes = (byte[])matlabResult[0];

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
        File outputfile = new File("./testData/Matlab-Gaussian3dT.jpg");
        ImageIO.write(bufImgRes, "jpg", outputfile);

        // Free native resources
        MWArray.disposeArray(mwPixels);
        //MWArray.disposeArray(matlabResult);
        gaussian3dT.dispose();
    }

    public static void main(String[] args) throws Exception {
        applyTo3D();
    }
}
