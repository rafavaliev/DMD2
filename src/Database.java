import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Igor on 17.10.2015.
 */
public class Database {


    Database() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        raf.writeBytes("Tables 0     e" + multiplyString(" ", 10000) + "\r\n");
        raf.close();
    }

    public void addTable(Table table) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        // go to the end of file
        raf.seek(raf.length());
        raf.writeBytes("\r\n");
        long tablePos = raf.getFilePointer();
        // write meta of table
        raf.writeBytes(table.name + " ");
        raf.writeBytes(String.valueOf(table.attributes.size()) + " ");
        for (Atribute attr : table.attributes) {
            if (attr.key) {
                raf.writeBytes("K$");
            }
            raf.writeBytes(attr.name + " ");
        }
        // writing size
        raf.writeBytes("s ");
        long sizePos = raf.getFilePointer();
        raf.writeBytes(String.valueOf(table.size) + " ");
        // writing offset
        raf.writeBytes("o ");
        long offsetPos = raf.getFilePointer();
        raf.writeBytes(String.valueOf(table.offset) + " ");
        // writing free space location
        raf.writeBytes("f ");
        long freePos = raf.getFilePointer();
        raf.writeBytes("                ");
        raf.writeBytes("e");
        long position = raf.getFilePointer()-1;
        raf.seek(freePos);
        raf.writeBytes(String.valueOf(position));

        // go to the beginning of the file
        raf.seek(7);
        StringBuilder amount = new StringBuilder();
        char c =  (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            amount.append(c);
            c = (char)raf.readByte();
        }
        // update amount of tables
        int newAmount = Integer.parseInt(amount.toString());
        raf.seek(7);
        raf.writeBytes(String.valueOf(newAmount+1));
        c =  (char)raf.readByte();
        while (c != 'e') {
            c = (char)raf.readByte();
        }
        raf.seek(raf.getFilePointer() - 1);
        raf.writeBytes(String.valueOf(tablePos) + " ");
        raf.writeBytes(table.name + "     e");
        raf.close();
    }


    public void insert(String tableName, ArrayList<String> values) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        // go to the table free space
        raf.seek(table.freeSpace);
        // write data
        for (String value : values) {
            raf.writeBytes(value + " ");
        }

        raf.seek(raf.getFilePointer() + 5 - 1);
        // mark free space
        raf.writeBytes("e");
        // update free space
        table.updateFreeSpace(raf, raf.getFilePointer()-1);
        // update size
        table.updateSize(raf);
    }


    public void delete(String tableName, String key) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");


        if (!searchForKey(raf, table, key)) {
            throw new IOException("No such record");
        }
        raf.writeBytes("d");
        int spaces = 0;
        while (spaces != 2) {
            if (raf.readByte() == ' ') {
                spaces++;
            } else {
                spaces = 0;
            }
            raf.seek(raf.getFilePointer() - 1);
            raf.writeBytes(" ");
        }
        // TODO update size
    }

    /*public String find(String tableName, String key) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(table.file, "rw");


        StringBuilder sb = new StringBuilder();

        if (!searchForKey(raf, offset, table, key)) {
            throw new IOException("No such record");
        }
        int spaces = 0;
        while (spaces != 2) {
            char c = (char)raf.readByte();
            if (c == ' ' || c == '\u0000') {
                spaces++;
            } else {
                spaces = 0;
            }
            //raf.seek(raf.getFilePointer() - 1);
            sb.append(c);

        }
        return sb.toString();
    }*/

    // TODO return result set?
    private boolean searchForKey(RandomAccessFile raf, Table table, String key) throws IOException {
        raf.seek(table.freeSpacePointer + 16);
        StringBuilder value = new StringBuilder();
        char c = (char) raf.readByte();
        while (c != 'e') {
            if (c == 'd') {
                c = (char) raf.readByte();
                while (c == ' ') {
                    c = (char) raf.readByte();
                }
            } else {
                for (int i = 0; i < table.attributes.size(); i++) {
                    value.append(String.valueOf(c));
                    char f = (char) raf.readByte();
                    while (f != ' ' && f != '\u0000') {
                        value.append(String.valueOf(f));
                        f = (char) raf.readByte();
                    }
                    if (key.equals(value)) {
                        // delete row


                        /*byte[] text = new byte[seek];
                        raf.read(text);
                        String value = new String(text);
                        if (key.equals(value)) {
                            // return to the beginning of the record
                            raf.seek(raf.getFilePointer() - seek - 1 - String.valueOf(seek).length());
                            return true;
                        }
                        raf.seek(raf.getFilePointer() + 1);*/
                    } else {
                        //raf.seek(raf.getFilePointer() + seek + 1);
                    }
                    c = (char) raf.readByte();
                }

                if (c == ' ' || c == '\u0000') {
                    raf.seek(raf.getFilePointer() + 5 - 2);
                    c = (char) raf.readByte();
                }
            }
        }
        return false;
    }

    private Table searchTable(String tableName) throws IOException {
        // scan first line with table position
        Scanner scanner = new Scanner(new File("Database.txt"));
        String line = scanner.nextLine();
        Scanner scan = new Scanner(line).useDelimiter("\\s+");
        scan.next();
        scan.nextInt();

        String searchName;
        long position = -1;
        // searching required table
        while(scan.hasNext()) {
            position = scan.nextLong();
            searchName = scan.next();
            if (searchName.equals(tableName)) {
                break;
            }
            position = -1;
        }

        if (position == -1) {
            throw new IOException("No such table");
        }

        // go to the table
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        raf.seek(position);
        // scanning name
        StringBuilder name = new StringBuilder();
        char c = (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            name.append(c);
            c = (char)raf.readByte();
        }
        // scanning amount of rows
        c = (char)raf.readByte();
        StringBuilder amount = new StringBuilder();
        while (c != ' ' && c != '\u0000') {
            amount.append(c);
            c = (char)raf.readByte();
        }
        int newAmount = Integer.parseInt(amount.toString());
        // scanning attributes
        boolean isKey;
        ArrayList<Atribute> atributes = new ArrayList<>(newAmount);
        StringBuilder attribute;
        for (int i = 0; i < newAmount; i++) {
            isKey = false;
            String at;
            attribute = new StringBuilder();
            c = (char)raf.readByte();
            while (c != ' ' && c != '\u0000') {
                attribute.append(c);
                c = (char)raf.readByte();
            }
            // if key
            if (attribute.toString().startsWith("K$")) {
                at = attribute.substring(2);
                isKey = true;
            }
            else {
                at = attribute.toString();
            }
            // add to the list
            atributes.add(new Atribute(at, "text", isKey));
        }
        // create table object
        Table table = new Table(name.toString(), atributes);
        table.setPosition(position);
        // set size pointer position
        table.sizePointer = raf.getFilePointer() + 2;
        // reading size
        table.size = readSize(raf);
        c = (char)raf.readByte();
        while (c == ' ' || c == '\u0000') {
            c = (char)raf.readByte();
        }
        raf.seek(raf.getFilePointer()-1);
        // set offset pointer position
        table.offsetPointer = raf.getFilePointer() + 2;
        // reading offset
        table.offset = readOffset(raf);
        c = (char)raf.readByte();
        while (c == ' ' || c == '\u0000') {
            c = (char)raf.readByte();
        }
        raf.seek(raf.getFilePointer()-1);
        // set free space pointer position
        table.freeSpacePointer = raf.getFilePointer() + 2;
        // reading free space location
        table.freeSpace = readFreeSpace(raf);
        return table;
    }


    private int readSize(RandomAccessFile raf) throws IOException {
        StringBuilder size = new StringBuilder();
        if (raf.readByte() == 's') {
            raf.seek(raf.getFilePointer()+1);
            char c = (char)raf.readByte();
            while (c != ' ' && c != '\u0000') {
                size.append(c);
                c = (char)raf.readByte();
            }
        } else {
            throw new IOException("Invalid file");
        }
        return Integer.parseInt(size.toString());
    }

    private int readOffset(RandomAccessFile raf) throws IOException {
        StringBuilder offset = new StringBuilder();
        if (raf.readByte() == 'o') {
            raf.seek(raf.getFilePointer()+1);
            char c = (char)raf.readByte();
            while (c != ' ' && c != '\u0000') {
                offset.append(c);
                c = (char)raf.readByte();
            }
        } else {
            throw new IOException("Invalid file");
        }
        return Integer.parseInt(offset.toString());
    }


    private long readFreeSpace(RandomAccessFile raf) throws IOException {
        StringBuilder freeSpace = new StringBuilder();
        if (raf.readByte() == 'f') {
            raf.seek(raf.getFilePointer()+1);
            char c = (char)raf.readByte();
            while (c != ' ' && c != '\u0000') {
                freeSpace.append(c);
                c = (char)raf.readByte();
            }
        } else {
            throw new IOException("Invalid file");
        }
        return Long.parseLong(freeSpace.toString());
    }

    /**
     * Multiplies string
     *
     * @param string string
     * @param amount multiplier
     *
     * @return new string of specified length
     * */
    private String multiplyString(String string, int amount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < amount; i++) {
            sb.append(string);
        }
        return sb.toString();
    }

    public void insert(long offset, byte[] content) throws IOException {
        RandomAccessFile r = new RandomAccessFile(new File("Database.txt"), "rw");
        RandomAccessFile rtemp = new RandomAccessFile(new File("Database.txt" + "~"), "rw");
        long fileSize = r.length();
        FileChannel sourceChannel = r.getChannel();
        FileChannel targetChannel = rtemp.getChannel();
        sourceChannel.transferTo(offset, (fileSize - offset), targetChannel);
        sourceChannel.truncate(offset);
        r.seek(offset);
        r.write(content);
        long newOffset = r.getFilePointer();
        targetChannel.position(0L);
        sourceChannel.transferFrom(targetChannel, newOffset, (fileSize - offset));
        sourceChannel.close();
        targetChannel.close();
    }
}
