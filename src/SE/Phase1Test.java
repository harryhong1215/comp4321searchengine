package SE;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.IOException;
import java.util.Vector;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.FileWriter;


/**
 * Created by opw on 4/8/16.
 */
public class Phase1Test {
    public static void main(String[] args) throws IOException {

        System.out.println("No output in the console, please get the result from spider_result.txt");

        PrintStream out = new PrintStream(new FileOutputStream("spider_result.txt"));
        System.setOut(out);

        try {
            // load the db
            RecordManager recman = RecordManagerFactory.createRecordManager("data/database");

            // load indexes
            ForwardIndex fIndex = new ForwardIndex(recman, "forwardIndex");
            MappingIndex urlIndex = new MappingIndex(recman, "urlMappingIndex");
            PageProperty properyIndex = new PageProperty(recman, "pagePropertyIndex");
            ParentChildIndex parentChildIndex = new ParentChildIndex(recman, "parentChildIndex");

            Vector<Integer> urlList = fIndex.getExistingPageIdList();
//            System.out.println(urlList);
            for (int pageID : urlList) {
                Properties p = properyIndex.get(pageID);
                if(p == null) continue;
                System.out.println(p.getTitle());
                System.out.println(urlIndex.getKey(pageID));
                properyIndex.printWithPageID(pageID);
                fIndex.printPageTermFrequency(pageID);
                parentChildIndex.printWithPageID(pageID);
                System.out.println("------------------------------");
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
