import java.io.IOException;
import student.TestCase;

/**
 * @author {Your Name Here}
 * @version {Put Something Here}
 */
public class ExternalsortTest extends TestCase {
    
    
    /**
     * set up for tests
     */
    public void setUp() {
        //nothing to set up.
    }
    
    /**
     * T
     * @throws IOException 
     */
    public void testExternalsort() throws IOException {
        String[] args = {"solutionTestData/sampleInput16_sorted.bin"};
        Externalsort.main(args);
    }

}
