import java.io.File;
import student.TestCase;

public class BinaryParserTest extends TestCase {

    public void setUp() throws Exception {
        
    }
    
    public void testSort() throws Exception {
        ByteFile file = new ByteFile("testDummy.bin", 8);
        file.writeRandomRecords();
        try {
            BinaryParser binary = new BinaryParser("testDummy.bin");
            binary.printRecords();
            assertTrue(file.isSorted());
        }
        finally {
            new File("testDummy.bin").delete();
        }
    }

}
