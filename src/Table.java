import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Created by Igor on 17.10.2015.
 */
public class Table {
    String name;
    ArrayList<Atribute> attributes;
    int size;
    long sizePointer;
    long position;
    int offset;
    long offsetPointer;
    long freeSpace;
    long freeSpacePointer;

    Table(String name, ArrayList<Atribute> attributes) throws IOException {
        this.name = name;
        this.offset = 0;
        this.size = 0;
        this.position = 0;
        this.attributes = attributes;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public void updateSize(RandomAccessFile raf) throws IOException {
        raf.seek(sizePointer);
        // read current size
        StringBuilder size = new StringBuilder();
        char c =  (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            size.append(c);
            c = (char)raf.readByte();
        }
        // increase
        int newSize = Integer.parseInt(size.toString()) + 1;
        raf.seek(sizePointer);
        raf.writeBytes(String.valueOf(newSize));
    }

    public void updateOffset(RandomAccessFile raf, int amount) throws IOException {
        raf.seek(offsetPointer);
        // read current offset
        StringBuilder offset = new StringBuilder();
        char c =  (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            offset.append(c);
            c = (char)raf.readByte();
        }
        // increase
        int newOffset = Integer.parseInt(offset.toString()) + amount;
        raf.seek(offsetPointer);
        raf.writeBytes(String.valueOf(newOffset));
    }

    public void updateFreeSpace(RandomAccessFile raf, long position) throws IOException {
        raf.seek(freeSpacePointer);
        // write new position
        raf.writeBytes(String.valueOf(position));
    }


    public void vacuum (RandomAccessFile raf) {
        // TODO implement vacuum
    }

}
