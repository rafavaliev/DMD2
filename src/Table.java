import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Igor on 17.10.2015.
 */
public class Table {
    String name;
    ArrayList<Atribute> attributes;
    File file;
    int size;
    long position;
    int offset;
    long freeSpace;

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
}
