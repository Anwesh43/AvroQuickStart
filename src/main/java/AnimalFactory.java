import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AnimalFactory {

    public static GenericRecord createAnimal(String name, Integer age) throws Exception{
        GenericRecord animal = new GenericData.Record(Constants.getAnimalSchema());
        animal.put("name", name);
        animal.put("age", age);
        return animal;
    }
}
