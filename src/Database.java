import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Igor on 17.10.2015.
 */
public class Database {

    //TODO change insertion in table


    Database() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        raf.writeBytes("Tables 0     f" + multiplyString(" ", 10000) + "\r\n");
        raf.close();
    }

    public void addTable(Table table) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        RandomAccessFile meta = new RandomAccessFile(new File("Database.txt"), "rw");
        raf.seek(raf.length());
        raf.writeBytes("\r\n");
        long tablePos = raf.getFilePointer();
        raf.writeBytes(table.name + " ");
        raf.writeBytes(String.valueOf(table.attributes.size()) + " ");
        for (Atribute attr : table.attributes) {
            if (attr.key) {
                raf.writeBytes("K$");
            }
            raf.writeBytes(attr.name + " ");
        }
        //TODO free space location(Maybe 2 raf?)
        // writing size
        raf.writeBytes("s ");
        long sizePos = raf.getFilePointer();                        //+ table.size + "       " + "o " + table.offset + "           " + "f 15              ");
        raf.writeBytes(String.valueOf(table.size));
        raf.writeBytes("o ");
        raf.seek(7);
        StringBuilder amount = new StringBuilder();
        char c =  (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            amount.append(c);
            c = (char)raf.readByte();
        }
        int newAmount = Integer.parseInt(amount.toString());
        raf.seek(7);
        raf.writeBytes(String.valueOf(newAmount+1));
        c =  (char)raf.readByte();
        while (c != 'f') {
            c = (char)raf.readByte();
        }
        raf.seek(raf.getFilePointer() - 1);
        raf.writeBytes(String.valueOf(tablePos) + " ");
        raf.writeBytes(table.name + "     f");
        raf.close();
    }


    public void insert(String tableName, ArrayList<String> values) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");

        // search free space
        searchForFree(raf, table, values);

        raf.seek(raf.getFilePointer() - 1);
        // write data
        for (String value : values) {
            raf.writeBytes(value + " ");
        }

        raf.seek(raf.getFilePointer() + 5 - 1);
        // mark free space
        raf.writeBytes("f");
        raf.seek(2);
        // update size
        raf.writeBytes(String.valueOf(table.size + 1));
    }

    public void delete(String tableName, String key) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(table.file, "rw");

        int size = readSize(raf);
        int offset = readOffset(raf);

        if (!searchForKey(raf, offset, table, key)) {
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

        raf.seek(2);
        // update size
        raf.writeBytes(String.valueOf(size - 1));
    }

    public String find(String tableName, String key) throws IOException {
        Table table = searchTable(tableName);
        RandomAccessFile raf = new RandomAccessFile(table.file, "rw");

        int size = readSize(raf);
        int offset = readOffset(raf);

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
    }


    private void searchForFree(RandomAccessFile raf, Table table, ArrayList<String> values) throws IOException {
        raf.seek(table.position);
        char c = (char) raf.readByte();
        String str = "";
        while (c != 'f') {
            if (c == 'd') {
                c = (char) raf.readByte();
                while (c == ' ' || c == '\u0000') {
                    c = (char) raf.readByte();
                }
            } else {
                for (int i = 0; i < table.attributes.size(); i++) {
                    str += String.valueOf(c);
                    char f = (char) raf.readByte();
                    while (f != ' ') {
                        str += String.valueOf(f);
                        f = (char) raf.readByte();
                    }
                    int seek = Integer.parseInt(str);
                    if (table.attributes.get(i).key) {
                        byte[] text = new byte[seek];
                        raf.read(text);
                        String value = new String(text);
                        if (values.get(i).equals(value)) {
                            throw new IOException("The key is already in table");
                        }
                        raf.seek(raf.getFilePointer() + 1);
                    } else {
                        raf.seek(raf.getFilePointer() + seek + 1);
                    }
                    c = (char) raf.readByte();
                    str = "";
                }

                if (c == ' ' || c == '\u0000') {
                    raf.seek(raf.getFilePointer() + 5 - 2);
                    c = (char) raf.readByte();
                }
            }
        }

    }

    private boolean searchForKey(RandomAccessFile raf, int offset, Table table, String key) throws IOException {
        raf.seek(raf.getFilePointer() + offset);
        char c = (char) raf.readByte();
        String str = "";
        while (c != 'f') {
            if (c == 'd') {
                c = (char) raf.readByte();
                while (c == ' ') {
                    c = (char) raf.readByte();
                }
            } else {
                for (int i = 0; i < table.attributes.size(); i++) {
                    str += String.valueOf(c);
                    char f = (char) raf.readByte();
                    while (f != ' ') {
                        str += String.valueOf(f);
                        f = (char) raf.readByte();
                    }
                    int seek = Integer.parseInt(str);
                    if (table.attributes.get(i).key) {
                        byte[] text = new byte[seek];
                        raf.read(text);
                        String value = new String(text);
                        if (key.equals(value)) {
                            // return to the beginning of the record
                            raf.seek(raf.getFilePointer() - seek - 1 - String.valueOf(seek).length());
                            return true;
                        }
                        raf.seek(raf.getFilePointer() + 1);
                    } else {
                        raf.seek(raf.getFilePointer() + seek + 1);
                    }
                    c = (char) raf.readByte();
                    str = "";
                }

                if (c == ' ' || c == '\u0000') {
                    raf.seek(raf.getFilePointer() + offset - 2);
                    c = (char) raf.readByte();
                }
            }
        }
        return false;
    }

    private Table searchTable(String tableName) throws IOException {
        Scanner scanner = new Scanner(new File("Database.txt"));
        String line = scanner.nextLine();
        Scanner scan = new Scanner(line).useDelimiter("\\s+");
        scan.next();
        scan.nextInt();

        String searchName;
        long position = -1;
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

        RandomAccessFile raf = new RandomAccessFile(new File("Database.txt"), "rw");
        raf.seek(position);
        StringBuilder name = new StringBuilder();
        char c = (char)raf.readByte();
        while (c != ' ' && c != '\u0000') {
            name.append(c);
            c = (char)raf.readByte();
        }
        c = (char)raf.readByte();
        StringBuilder amount = new StringBuilder();
        while (c != ' ' && c != '\u0000') {
            amount.append(c);
            c = (char)raf.readByte();
        }
        int newAmount = Integer.parseInt(amount.toString());
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
        Table table = new Table(name.toString(), atributes);
        table.setPosition(position);
        table.size = readSize(raf);
        c = (char)raf.readByte();
        while (c == ' ' || c == '\u0000') {
            c = (char)raf.readByte();
        }
        raf.seek(raf.getFilePointer()-1);
        table.offset = readOffset(raf);
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
