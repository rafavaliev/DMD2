import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Igor on 18.10.2015.
 */
public class Test {
	//our test
    public static void main(String[] args) throws IOException {
        ArrayList<Atribute> atributes = new ArrayList<>();
        atributes.add(new Atribute("Id", "int", true));
        atributes.add(new Atribute("Name", "String", false));
        atributes.add(new Atribute("E-mail", "String", false));
        atributes.add(new Atribute("Address", "String", false));

        Table table1 = new Table("Students", atributes);
        Database database = new Database();
        database.addTable(table1);
        atributes = new ArrayList<>();
        atributes.add(new Atribute("Id", "int", true));
        atributes.add(new Atribute("Name", "String", false));
        atributes.add(new Atribute("Designation", "String", false));
        atributes.add(new Atribute("Address", "String", false));
        table1 = new Table("Employee", atributes);
        ArrayList<String> values = new ArrayList<>();
        values.add("1");
        values.add("Igor");
        values.add("igorzub93@gmail.com");
        values.add("exampleAddress");
        database.insert("Students", values);
        values = new ArrayList<>();
        values.add("2");
        values.add("Ivan");
        values.add("ivan@gmail.com");
        values.add("exampleAddress");
        database.insert("Students", values);
        values = new ArrayList<>();
        values.add("3");
        values.add("Igor");
        values.add("igorzub93@gmail.com");
        values.add("exampleAddress");
        database.insert("Students", values);
        database.delete("Students", "3");
        database.addTable(table1);
        values = new ArrayList<>();
        values.add("1");
        values.add("Igor");
        values.add("Designation");
        values.add("exampleAddress");
        database.insert("Employee", values);
        //System.out.println(database.find("Students", "2"));
        //database.delete("Students", "1");



    }
}
