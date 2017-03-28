package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.*;
import java.util.Vector;


public class Outputtxt {
    public void output() {

        System.out.println("The test program is started");
        System.out.println("Please open the spider_result.txt to check the result after the process finished");

        try{
            PrintStream out = new PrintStream(new FileOutputStream("spider_result.txt"));
            System.setOut(out); //open a file call spider_result

            // load the db
            RecordManager recman = RecordManagerFactory.createRecordManager("data/database");

            // load indexes
            ForwardIndex  forwardIndex = new ForwardIndex(recman, "forwardIndex");
            MappingIndex mappingIndex = new MappingIndex(recman, "urlMappingIndex");
            PageProperty  pagepropertyIndex = new PageProperty(recman, "pagePropertyIndex");
            ParentChildIndex parentchildIndex = new ParentChildIndex(recman, "parentChildIndex");

            //load the existing page list into UrList
            Vector<Integer> UrlList = forwardIndex.getExistingPageIdList();

            //Write all the data into spider_result.txt by using for loop
            for (int pageID : UrlList) {
                Properties characteristic = pagepropertyIndex.get(pageID);
                if (characteristic == null) {
                    continue;
                }
                System.out.println(characteristic.getTitle());
                System.out.println(mappingIndex.getKey(pageID));
                pagepropertyIndex.printWithPageID(pageID);
                forwardIndex.printPageTermFrequency(pageID);
                parentchildIndex.printWithPageID(pageID);
                System.out.println("---------------------------------");
            }
//            failed one
//            Iterator<> sizeofUrlist = UrlList.iterator();
//            int pageid = 0;
//            while(sizeofUrlist.hasNext()){
//                sizeofUrlist.next();
//                Properties characteristic = pagepropertyIndex.get(pageid);
//                if (characteristic == null) {
//                    continue;
//                }
//                System.out.println(characteristic.getTitle());
//                System.out.println(mappingIndex.getKey(pageid));
//                pagepropertyIndex.printWithPageID(pageid);
//                forwardIndex.printPageTermFrequency(pageid);
//                parentchildIndex.printWithPageID(pageid);
//                System.out.println("---------------------------------");
//                pageid++;
//                System.out.println(pageid);
//                //sizeofUrlist.next();
//            }

        } catch (IOException e) {
            System.out.println("IOException, please restart the program"); // using for catch exception
        }

    }
}

