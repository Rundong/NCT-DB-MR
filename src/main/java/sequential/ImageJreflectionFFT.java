package sequential;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;

public class ImageJreflectionFFT extends JPanel {
    BufferedImage image;
    Dimension size = new Dimension();

    public ImageJreflectionFFT(BufferedImage image) {
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

    /**
     * Easy way to show an image: load it into a JLabel
     * and add the label to a container in your gui.
     */
    private static void showIcon(BufferedImage image) {
        ImageIcon icon = new ImageIcon(image);
        JLabel label = new JLabel(icon, JLabel.CENTER);
        JOptionPane.showMessageDialog(null, label, "icon", -1);
    }

    public static void main(String[] args) throws Exception {
        String path = "/Users/RundongL/MyWorkStack/repos/NCtracerWeb/NCT-Batch/plugins/image-filter/jpeg-subset/256-384_896-1024_0-64.jpg";
        BufferedImage image = ImageIO.read(new File(path));

        // apply FFT on this image
        Class clFFT = Class.forName("");

        ImageJreflectionFFT test = new ImageJreflectionFFT(image);
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.add(new JScrollPane(test));
        f.setSize(400,400);
        f.setLocation(200,200);
        f.setVisible(true);

    }
}
