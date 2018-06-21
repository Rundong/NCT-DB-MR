package hadoop.ImageFormat;

import junit.framework.TestCase;

public class JpegToHDFSTest extends TestCase {

    public void testUpload() {
        JpegToHDFS.upload("./testData/jpegs/", "./testData/jpegs_hadoop_format");
    }

    public void testLoad() {
//        JpegToHDFS.loadHdfsImage("./testData/jpegs_hadoop_format", "./testData/laoded_jpegs_from_HDFS/");
        JpegToHDFS.loadHdfsImage("./testData/jpegs_sequence_files_filtered", "./testData/jpegs_filtered");
    }
}