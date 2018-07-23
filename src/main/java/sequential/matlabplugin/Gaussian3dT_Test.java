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
//        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
        String path = "./testData/original-subimage-last3slices.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 3;
        int sliceSize = xDim * yDim;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");
//        ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
//        for (int iz = 1; iz <= zDim; iz++) {
//            imgStack.setPixels(Arrays.copyOfRange(pixels, (iz-1) * sliceSize, (iz) * sliceSize), iz);
//        }
//
        // prepare the image and the filter paramters
        byte initByte = 0;
        MWNumericArray mwPixels = new MWNumericArray(new int[]{xDim, yDim, zDim}, pixels, MWClassID.UINT8);
        double sigma = 3.0;

        Class1 gaussian3dT = new Class1();
        byte[] pixRes = new byte[zDim * sliceSize];
        MWNumericArray mwPixelsResult = new MWNumericArray(new int[]{xDim, yDim, zDim}, MWClassID.UINT8);
        gaussian3dT.Gaussian3dT(new Object[]{mwPixelsResult}, new Object[]{mwPixels, sigma}); // todo: exception

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

    public static void main(String[] args) throws Exception {
        applyTo3D();
    }
}
