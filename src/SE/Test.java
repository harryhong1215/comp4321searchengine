package SE;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import org.htmlparser.util.ParserException;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by opw on 27/3/2016.
 */
public class Test {
    public int test() {
        return 123321;
    }

    public static void main(String[] args) {
        System.out.println("Test Start");
        try {
//            Vector<String> input = new Vector<String>();
//            input.add("test");
//            input.add("page");
////            SearchEngine.search(input);
//
//            SearchEngine.phaseSearch(input);
            RecordManager recman1 = RecordManagerFactory.createRecordManager("data/database");
            InvertedIndex bodyInvertedIndex = new InvertedIndex(recman1, "bodyInvertedIndex");
            ForwardIndex fIndex = new ForwardIndex(recman1, "forwardIndex");
            System.out.println(fIndex.getExistingPageIdList());
            fIndex.printAll();
//            boolean aa = bodyInvertedIndex.containsWordPos(5,17,933);
//                    System.out.print(aa);
            System.exit(1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
