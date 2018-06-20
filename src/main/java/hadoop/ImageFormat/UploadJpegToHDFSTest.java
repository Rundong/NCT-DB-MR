package hadoop.ImageFormat;

import junit.framework.TestCase;

public class UploadJpegToHDFSTest extends TestCase {

    public void testUpload() {
        UploadJpegToHDFS.upload("./testData/jpegs/", "./testData/jpegs_hadoop_format");
    }
}