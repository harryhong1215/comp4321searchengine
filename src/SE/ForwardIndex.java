package SE;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.*;

// PageID -> (WordID1 ... wordIDN)
public class ForwardIndex {

    private RecordManager recman;
    private HTree hashtable, hashtable_mtf;

    public ForwardIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        long recid = recman.getNamedObject(objectname);
        long recid_mtf = recman.getNamedObject(objectname + "_mtf");    // for storing max term frequency for each page

        if (recid != 0)
        {
            // if hashtable exist, load it
            hashtable = HTree.load(recman, recid);
            hashtable_mtf = HTree.load(recman, recid_mtf);
        }
        else
        {
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject( objectname, hashtable.getRecid() );

            hashtable_mtf = HTree.createInstance(recman);
            recman.setNamedObject( objectname + "_mtf", hashtable_mtf.getRecid() );

        }
    }


    public void finalize() throws IOException
    {
        recman.commit();
//        recman.close();
    }

    // allow duplicated wordIDs, e.g. 2 -> (2, 4, 3, 7)
    public void insert(int pageID, int wordID) throws IOException
    {
        // pageID -> wordPosList
        String key = Integer.toString(pageID);
        Vector<Integer> pages = null;

        if (hashtable.get(key) == null)
            pages = new Vector<Integer>();
        else
            pages = (Vector<Integer>) hashtable.get(key);

        pages.add(wordID);

        hashtable.put(key, pages);  // commit changes
//        recman.commit();
    }

    public void delete(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        hashtable.remove(key);
//        recman.commit();
    }


    public Vector<Integer> getList(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        Vector<Integer> list = new Vector<Integer>();
        if (hashtable.get(key) != null)
        {
            list = (Vector<Integer>) hashtable.get(key);
        }
        return list;
    }

    // get the total number of words inside a page
    public int getPageSize(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        Vector<Integer> list = new Vector<Integer>();
        list = (Vector<Integer>) hashtable.get(key);
        if(list != null)
            return list.size();
        return 0;
    }

    public int getTermFrequency(int pageID, int wordID) throws IOException
    {
        String key = Integer.toString(pageID);
        Vector<Integer> list = new Vector<Integer>();
        if (hashtable.get(key) != null)
        {
            list = (Vector<Integer>) hashtable.get(key);
        }
        return Collections.frequency(list, wordID);
    }

    // calculate the max term frequency and store in db
    public void calculateMaxTermFrequency(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        Vector<Integer> list = new Vector<Integer>();
        if (hashtable.get(key) == null)
        {
            System.out.println("ERROR: calculateMaxTermFrequency");
            return;
        }

        list = (Vector<Integer>) hashtable.get(key);
        Set<Integer> unique = new HashSet<Integer>(list);   // elminate duplicate terms
        int maxtf = 0;
        for(int wordID : unique) {
            int tf = Collections.frequency(list, wordID);
            if (tf > maxtf)
                maxtf = tf;
        }
        hashtable_mtf.put(key, maxtf);  // put the maxtf
    }

    public int getMaxTermFrequency(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        if(hashtable_mtf.get(key) == null)
            return 0;
        else
            return (Integer) hashtable_mtf.get(key);
    }

//    public List<Integer> getUniqueTerms(int pageID) throws IOException
//    {
//        String key = Integer.toString(pageID);
//        if(hashtable.get(key) == null)
//            return null;
//        Vector<Integer> list = (Vector<Integer>) hashtable.get(key);
//        List<Integer> uniqueList = new ArrayList<Integer>(list);
//        System.out.print(uniqueList);
//        return uniqueList;
//    }

//    public void printUniqueTermsFrequency(int pageID) throws IOException
//    {
//        String key = Integer.toString(pageID);
//        if(hashtable.get(key) == null)
//            return;
//
//        Vector<Integer> list = (Vector<Integer>) hashtable.get(key);
//
//        // wordID, frequency
//        Map<Integer, Integer> map = new HashMap<>();
//        for (int wordID : list) {
//            Integer freq = map.get(wordID);
//            freq = (freq == null) ? 1 : ++freq;
//            map.put(wordID, freq);
//        }
//
//        for (int k : map.keySet()) {
//            System.out.println(k + " " + map.get(k));
//        }
//
//    }

    // get Term Frequency map <wordID, Frequency>, return null is not exist
    public Map<Integer, Integer> getTermFrequencyMap(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        if(hashtable.get(key) == null)
            return null;

        Vector<Integer> list = (Vector<Integer>) hashtable.get(key);

        // get wordID, frequency map
        Map<Integer, Integer> map = new HashMap<>();
        for (int wordID : list) {
            Integer freq = map.get(wordID);
            freq = (freq == null) ? 1 : ++freq;
            map.put(wordID, freq);
        }

        return map;
    }

    public void printAll() throws IOException
    {
        // Print all the data in the hashtable

        // iterate through all keys
        FastIterator iter = hashtable.keys();
        String key;
        while( (key = (String)iter.next())!=null)
        {
            System.out.printf("PAGEID= %s, WORDIDS= %s\n" , key, hashtable.get(key));
        }
    }

    public void printPageTermFrequency(int pageID) throws IOException
    {
        Map<Integer, Integer> map = getTermFrequencyMap(pageID);
        MappingIndex wordIndex = new MappingIndex(recman, "wordMappingIndex");

        for (int k : map.keySet()) {
            System.out.printf("%s:%s; ", wordIndex.getKey(k), map.get(k));
        }
        System.out.println();
    }

    public String getPageTermFrequencyString(int pageID) throws IOException
    {
        Map<Integer, Integer> map = getTermFrequencyMap(pageID);
        MappingIndex wordIndex = new MappingIndex(recman, "wordMappingIndex");

        StringBuilder stringBuilder = new StringBuilder();
        for (int k : map.keySet()) {
            stringBuilder.append(wordIndex.getKey(k));
            stringBuilder.append(':');
            stringBuilder.append(map.get(k));
            stringBuilder.append("; ");
        }
        return stringBuilder.toString();
    }

    public Vector<Integer> getExistingPageIdList() throws IOException
    {
        Vector<Integer> v = new Vector<Integer>();
        FastIterator iter = hashtable.keys();
        String key;
        while( (key = (String)iter.next())!=null)
        {
            v.add(Integer.parseInt(key));
//            System.out.printf("PAGEID= %s, WORDIDS= %s\n" , key, hashtable.get(key));
        }
        return v;
    }


}
