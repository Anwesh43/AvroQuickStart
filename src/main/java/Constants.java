import  org.apache.avro.Schema;

import java.io.File;

public class Constants {

    private final static String ANIMAL_SCHEMA_FILE = "animal.avsc";

    public final static Schema getAnimalSchema() {
        try {
            return new Schema.Parser().parse(new File(ANIMAL_SCHEMA_FILE));
        } catch (Exception ex) {

        }
        return null;
    }
}
