package hadoop.ImageFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;

import java.io.Serializable;

@SuppressWarnings("WeakerAccess")
public class PixelArrayWritable extends ArrayWritable implements Serializable {

    public PixelArrayWritable(ByteWritable[] values) {
        super(ByteWritable.class, values);
    }

    @Override
    public ByteWritable[] get() {
        return (ByteWritable[]) super.get();
    }

    @Override
    public String toString() {
        ByteWritable[] values = get();
        StringBuilder builder = new StringBuilder();
        for (ByteWritable pixel : values) {
            builder.append(",").append(pixel.toString());
        }
        return builder.substring(1);
    }
}
