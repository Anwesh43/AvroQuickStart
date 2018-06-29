import com.demos.anwesh.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AvroUtil {

    public static void serializeUser(String fileName, User...users) throws Exception {
        DatumWriter dataumWriter = new SpecificDatumWriter(User.class);
        DataFileWriter<User> userDataFileWriter = new DataFileWriter<>(dataumWriter);
        if (users.length > 0) {
            userDataFileWriter.create(users[0].getSchema(), new File(fileName));
            for (User user : users) {
                userDataFileWriter.append(user);
            }
            userDataFileWriter.close();
        }
    }

    public static List<User> deserializeUser(String fileName) throws Exception{
        DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader(new File(fileName), datumReader);
        List<User> users = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            users.add(dataFileReader.next());
        }
        dataFileReader.close();
        return users;
    }

    public static void writeGenericRecords(String fileName, Schema schema, GenericRecord...records) throws Exception {
        DatumWriter datumWriter = new GenericDatumWriter(schema);
        DataFileWriter<GenericRecord> genericRecordDataFileWriter = new DataFileWriter(datumWriter);
        genericRecordDataFileWriter.create(schema, new File(fileName));
        for (GenericRecord record : records) {
            genericRecordDataFileWriter.append(record);
        }
        genericRecordDataFileWriter.close();
    }

    public static List<GenericRecord> readGenericRecords(String fileName, Schema schema) throws Exception{
        List<GenericRecord> records = new ArrayList<>();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> fileReader = new DataFileReader(new File(fileName), datumReader);
        while (fileReader.hasNext()) {
            records.add(fileReader.next());
        }
        fileReader.close();
        System.out.println(String.format("%s has been deserialized", fileName));
        return records;
    }
}
