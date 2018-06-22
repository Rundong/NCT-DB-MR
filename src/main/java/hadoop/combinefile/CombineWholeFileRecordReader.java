package hadoop.combinefile;

// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//vv WholeFileRecordReader
class CombineWholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private CombineFileSplit fileSplit;
    private Configuration conf;
    private Text key = new Text(); // current file name
    private BytesWritable value = new BytesWritable();
    private int numFiles;
    private int fileIndex;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fileSplit = (CombineFileSplit) split;
        this.conf = context.getConfiguration();
        this.numFiles = ((CombineFileSplit) split).getNumPaths();
        this.fileIndex = 0;
        //System.out.println(" record reader: numFiles = " + numFiles);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //System.out.println("-->nextKeyValue");
        if (fileIndex < numFiles) {
            //System.out.println(" file index: " + fileIndex);
            byte[] contents = new byte[(int) fileSplit.getLength(fileIndex)];
            Path file = fileSplit.getPaths()[fileIndex];
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                //System.out.println(" file: " + file.toString() + ", length = " + contents.length);
                key.set(fileSplit.getPath(fileIndex).toString());
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }
            fileIndex++;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return fileIndex / (float) numFiles;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
//^^ WholeFileRecordReader