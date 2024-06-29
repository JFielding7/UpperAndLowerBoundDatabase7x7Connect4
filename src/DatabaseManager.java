import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DatabaseManager {

    public static void main(String[] args) {
        splitDatabase("upper_bound_database", 16);
    }

    static void splitDatabase(String binFile, int partitions) {
        try (FileInputStream in = new FileInputStream(binFile + ".bin")) {
            byte[] bytes = in.readAllBytes();
            int sectionLength = bytes.length / partitions;
            for (int i = 0; i < partitions; i++) {
                FileOutputStream out = new FileOutputStream(binFile + i + ".bin");
                if (i < partitions - 1) out.write(bytes, i * sectionLength, sectionLength);
                else out.write(bytes, i * sectionLength, sectionLength + bytes.length % partitions);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void mergeDatabase(String outFile, int partitions) {
        try (FileOutputStream out = new FileOutputStream(outFile + "Merged.bin")) {
            for (int i = 0; i < partitions; i++) {
                FileInputStream in = new FileInputStream(outFile + i + ".bin");
                out.write(in.readAllBytes());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
