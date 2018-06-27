package hadoop.wholefile;

import junit.framework.TestCase;

public class WholeImageTranslatorTest extends TestCase {

    public void testLoadHdfsImage() {
        WholeImageTranslator.loadHdfsImage("./testData/whole-image-filtered/", "./testData/whole-image-filtered-jpegs");
//        WholeImageTranslator.loadHdfsImage("./testData/multi-image-filtered/", "./testData/multi-image-filtered-jpegs");
    }
}