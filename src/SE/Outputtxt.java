package SE;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.*;
import java.util.Vector;


public class Outputtxt {
    public void output() {
        System.out.println("No output in the console, please get the result from spider_result.txt");
        try {
            PrintStream out = new PrintStream(new FileOutputStream("spider_result.txt"));
            System.setOut(out);
        }catch (FileNotFoundException e){
            System.out.println("FileNotFoundException, please restart the program");
        }

        try{
            // load the db
            RecordManager recman = RecordManagerFactory.createRecordManager("data/database");

            // load indexes
            ForwardIndex fIndex = new ForwardIndex(recman, "forwardIndex");
            MappingIndex urlIndex = new MappingIndex(recman, "urlMappingIndex");
            PageProperty properyIndex = new PageProperty(recman, "pagePropertyIndex");
            ParentChildIndex parentChildIndex = new ParentChildIndex(recman, "parentChildIndex");

            Vector<Integer> urlList = fIndex.getExistingPageIdList();
            for (int pageID : urlList) {
                Properties p = properyIndex.get(pageID);
                if (p == null) continue;
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

