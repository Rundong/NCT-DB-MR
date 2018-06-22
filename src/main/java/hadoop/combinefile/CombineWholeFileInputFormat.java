package hadoop.combinefile;

// cc CombineWholeFileInputFormat An InputFormat for reading a whole file as a record in CombineFileSplit

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

//vv CombineWholeFileInputFormat
public class CombineWholeFileInputFormat
        extends CombineFileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        CombineWholeFileRecordReader reader = new CombineWholeFileRecordReader();
        try {
            reader.initialize((CombineFileSplit)split, context);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reader;
    }
}
//^^ CombineWholeFileInputFormat
