import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DistributedGrepTest {

    /**
     * Assume grepResult and DistributedGrepResult already exist. grepResult is
     * the result of runnign grep locally on *one* file. DistributedGrepReulst
     * is the result of running grep distributedly on the same file, but split
     * across multiple servers.
     * 
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        File dgr = new File("distributedGrepResult");
        List<String> orderedLines = TestUtil.sortDistributedGrepResults(dgr);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        for (String line : orderedLines) {
            pw.println(line);
        }
        StringReader sr = new StringReader(sw.toString());
        
        FileReader fr = new FileReader("grepResult");
        Assert.assertTrue(TestUtil.compareTwoFiles(sr, fr));
        fr.close();
    }

}
