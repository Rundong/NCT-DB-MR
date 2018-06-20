package hadoop.ImageFormat;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Customized hadoop input format for image data.
 */

@SuppressWarnings("WeakerAccess")
public class PixelKey implements WritableComparable<PixelKey>, Serializable {
    public int x, y, z, x_dim, y_dim, z_dim;

    public PixelKey(int xx, int yy, int zz, int xDim, int yDim, int zDim) {
        x = xx;
        y = yy;
        z = zz;
        x_dim = xDim;
        y_dim = yDim;
        z_dim = zDim;
    }

    public boolean equals(Object o1) {
        if(! (o1 instanceof PixelKey)) {
            return false;
        } else {
            PixelKey key1 = (PixelKey) o1;
            return (this.x == key1.x) && (this.y == key1.y) && (this.z == key1.z)
                    && (this.x_dim == key1.x_dim) && (this.y_dim == key1.y_dim) && (this.z_dim == key1.z_dim);
        }
    }

    @Override
    public int compareTo(PixelKey o) {
        if (this.equals(o)) {
            return 0;
        } else {
            if (this.z != o.z) {
                return (this.z < o.z) ? -1 : 1;
            } else if (this.y != o.y) {
                return (this.y < o.y) ? -1 : 1;
            } else if (this.x != o.x) {
                return (this.x < o.x) ? -1 : 1;
            } else {
                return 0; // ignoring the dimensions of x,y,z for now
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
        dataOutput.writeInt(z);
        dataOutput.writeInt(x_dim);
        dataOutput.writeInt(y_dim);
        dataOutput.writeInt(z_dim);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readInt();
        y = dataInput.readInt();
        z = dataInput.readInt();
        x_dim = dataInput.readInt();
        y_dim = dataInput.readInt();
        z_dim = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "x,y,z = " + x + "," + y + "," + z + "\ndims = " + x_dim + "," + y_dim + "," + z_dim;
    }
}
