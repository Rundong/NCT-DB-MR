package sequential;

import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ImageProcessor;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.util.Arrays;

public class ImageJfftFilterTest extends JPanel  {

    BufferedImage image;
    Dimension size = new Dimension();

    public ImageJfftFilterTest(BufferedImage image) {
        this.image = image;
        size.setSize(image.getWidth(), image.getHeight());
    }

    /**
     * Drawing an image can allow for more
     * flexibility in processing/editing.
     */
    protected void paintComponent(Graphics g) {
        // Center image in this component.
        int x = (getWidth() - size.width)/2;
        int y = (getHeight() - size.height)/2;
        g.drawImage(image, x, y, this);
    }

    public Dimension getPreferredSize() { return size; }

    public static void main(String[] args) throws Exception {
        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
        BufferedImage bufImg = ImageIO.read(new File(path));
//        ImageJfftFilterTest original = new ImageJfftFilterTest(bufImg);
//        JFrame f0 = new JFrame();
//        f0.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        f0.add(new JScrollPane(original));
//        f0.setSize(400,400);
//        f0.setLocation(200,200);
//        f0.setVisible(true);

        // convert to 3D
        int xDim = 128, yDim = 128, zDim = 64;
        int sliceSize = xDim * yDim;
        byte[] pixels = ((DataBufferByte) bufImg.getRaster().getDataBuffer()).getData();
        System.out.println(" obtained pixels from buffered image");
        ImageStack imgStack = new ImageStack(xDim, yDim, zDim);
        for (int iz = 1; iz <= zDim; iz++) {
            imgStack.setPixels(Arrays.copyOfRange(pixels, iz * sliceSize, (iz + 1) * sliceSize), iz);
        }

        // apply filter
        ImagePlus imgPlus = new ImagePlus("pix", imgStack);
        ImageProcessor ip = imgPlus.getProcessor();
        ImageJfftFilter fftFilter = new ImageJfftFilter();
        fftFilter.setup("arg?", imgPlus);
        fftFilter.filter(ip);

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

        // convert filtered 3D image to 1D image
        DataBuffer buffer = new DataBufferByte(pixRes, pixRes.length);
        WritableRaster raster = Raster.createInterleavedRaster(buffer,
                xDim, yDim * zDim, xDim, 1, new int[]{0}, null);
        ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
        ColorModel cm = new ComponentColorModel(cs, false, true,
                Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
        BufferedImage bufImgRes = new BufferedImage(cm, raster, false, null);
        System.out.println(" done constructing buffered image result");

        // show the result
        ImageJfftFilterTest test = new ImageJfftFilterTest(bufImgRes);
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.add(new JScrollPane(test));
        f.setSize(400,400);
        f.setLocation(200,200);
        f.setVisible(true);
    }

}
