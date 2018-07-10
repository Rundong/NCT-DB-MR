package hadoop.wholefile;

import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.FFT;
import ij.plugin.GaussianBlur3D;
import ij.plugin.filter.FFTFilter;
import ij.process.ImageProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import sequential.ImageJfftFilter;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class WholeImageImageJFFT {

    public static class WholeImageFilterMapper extends
            Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private ImageJfftFilter fftFilter;
        private Text fileNameKey;

        protected void setup(Context c) throws IOException, InterruptedException {
            //System.out.println("mapper setup");
            fftFilter = new ImageJfftFilter();
            fftFilter.filterLargeDia = c.getConfiguration().getDouble("largeDia", 40.1);
            fftFilter.filterSmallDia = c.getConfiguration().getDouble("smallDia", 3.0);
            fftFilter.choiceDia = c.getConfiguration().get("choiceDia", "None");

            String filePathString = ((FileSplit) c.getInputSplit()).getPath().toString();
            System.out.println(" file path: " + filePathString);
            fileNameKey = new Text(filePathString);
//            String[] inputlocations = c.getInputSplit().getLocations();
//            System.out.println(" input locations length: " + inputlocations.length);
//            for (String str : inputlocations) {
//                System.out.println(" input split location: " + str);
//            }
            //System.out.println(" sigma values: " + sigmaX + "," + sigmaY + "," + sigmaZ);
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            //System.out.println("map function: key = " + key.toString() + ", image byte length = " + value.getLength());

            // interpret the image data: meta-info (x,y,z,dims) and pixels
            String[] path = fileNameKey.toString().split("/");
            String name = path[path.length - 1].split(".jpg")[0];
            String[] dims = name.split("_");
            String[] dimx = dims[0].split("-");
            int x = Integer.parseInt(dimx[0]);
            int xDim = Integer.parseInt(dimx[1]) - x;
            String[] dimy = dims[1].split("-");
            int y = Integer.parseInt(dimy[0]);
            int yDim = Integer.parseInt(dimy[1]) - y;
            String[] dimz = dims[2].split("-");
            int z = Integer.parseInt(dimz[0]);
            int zDim = Integer.parseInt(dimz[1]) - z;
            System.out.println(" meta data pased: " + name + " --> " + x + "," + y + "," + z
                    + " with dims = " + xDim + "," + yDim + "," + zDim);

            // parse the jpeg file into java image
            InputStream myInputStream = new ByteArrayInputStream(value.copyBytes());
            BufferedImage bufImg = ImageIO.read(myInputStream);

            // convert to 3D
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

            // write result image back to jpeg byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bufImgRes, "jpg", baos);
            baos.flush();
            byte[] imageInByte = baos.toByteArray();
            baos.close();

            context.write(fileNameKey, new BytesWritable(imageInByte));
            System.out.println(" num bytes: " + imageInByte.length);

        }//map function close

    }//SequenceImageFilterMapper close

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        double largeDia = Double.parseDouble(args[2]);
        conf.setDouble("largeDia", largeDia);
        Job job = Job.getInstance(conf, "WholeImageFilterImageJ");
        job.setJarByClass(WholeImageImageJFFT.class);

        job.setMapperClass(WholeImageFilterMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //close of driver class
    }

}
