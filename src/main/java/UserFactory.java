import com.demos.anwesh.avro.User;

public class UserFactory {
    public static User createUser(String name, String favColor, int favNumber) {
        return User.newBuilder().setName(name).setFavoriteColor(favColor).setFavoriteNumber(favNumber).build();
    }
}
