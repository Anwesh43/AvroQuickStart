import com.demos.anwesh.avro.User;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public class AvroDemo {
    public static void main(String args[]) {
//        writeUsers();
//        readUsers();
        //writeAnimals();
        readAnimalsFromFile();
    }

    public static void writeUsers() {
        User user1 = UserFactory.createUser("Anwesh1", "red", 1);

        User user2 = UserFactory.createUser("Anwesh2", "blue", 2);

        User user3 = UserFactory.createUser("Anwesh3", "pink", 3);
        try {
            AvroUtil.serializeUser("users.avro", user1, user2, user3);
            System.out.println("created users.aro successfully");
        } catch (Exception ex) {
            System.out.println("exception occurred while creating users.avro");
        }
    }

    public static void readUsers() {
        try {
            List<User> users = AvroUtil.deserializeUser("users.avro");
            for (User user : users) {
                System.out.println(String.format("My name is %s, color is %s and age is %d",
                        user.getName(), user.getFavoriteColor(), user.getFavoriteNumber()));
            }
        } catch(Exception ex) {
            System.out.println("exception occurred while creating users.avro");
        }
    }

    public static void writeAnimals() {
        try {
            AvroUtil.writeGenericRecords("animals.avro", Constants.getAnimalSchema(),
                    AnimalFactory.createAnimal("dog1", 3),
                    AnimalFactory.createAnimal("dog2", 4),
                    AnimalFactory.createAnimal("dog3", 5));
            System.out.println("animals.avro is created");
        }
        catch (Exception ex) {
            System.out.println("error in creating animals.avsc");
        }
    }

    public static void readAnimalsFromFile() {
        try {
            List<GenericRecord> records = AvroUtil.readGenericRecords("animals.avro",
                    Constants.getAnimalSchema());
            records.forEach((record) -> {
                System.out.println(String.format("animal %s is %d years old", record.get("name"), record.get("age")));
            });
        } catch (Exception ex) {
            System.out.println("error in deserializing");
        }
    }

}


