package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.*;

public class ForwardIndex {

    private RecordManager recman;
    private HTree hashtable, hashtable_mtf;


    public ForwardIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        long recid = recman.getNamedObject(objectname);
        long recid_mtf = recman.getNamedObject(objectname + "_mtf");

        if (recid <= 0 )
        {
            // create a new hashtable
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject( objectname, hashtable.getRecid() );
            hashtable_mtf = HTree.createInstance(recman);
            recman.setNamedObject( objectname + "_mtf", hashtable_mtf.getRecid() );
        }
        else
        {
            // load the existing hashtable if exist
            hashtable = HTree.load(recman, recid);
            hashtable_mtf = HTree.load(recman, recid_mtf);
        }
    }

    // save all changes to database
    public void finalize() throws IOException
    {
        recman.commit();
    }

    // generate hashID through pageID and hash the page to the pagelist which allow duplicate wordID
    public void insert(int pageID, int wordID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        Vector<Integer> pageList = null;

        if (hashtable.get(hashKey) == null)
            pageList = new Vector<Integer>();
        else
            pageList = (Vector<Integer>) hashtable.get(hashKey);

        pageList.add(wordID);

        // hash the page list to the page table
        hashtable.put(hashKey, pageList);
    }

    // remove the pagelist from hashtable
    public void delete(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        hashtable.remove(hashKey);
    }

    // obtain the page list from hash table
    public Vector<Integer> getList(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        Vector<Integer> pageList = new Vector<Integer>();
        if (hashtable.get(hashKey) != null)
        {
            pageList = (Vector<Integer>) hashtable.get(hashKey);
        }
        return pageList;
    }

    // obtain number of words inside the page
    public int getPageSize(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        Vector<Integer> pageList = new Vector<Integer>();
        pageList = (Vector<Integer>) hashtable.get(hashKey);
        if(pageList != null)
            return pageList.size();
        return 0;
    }

    // obtain the number of appearance of word inside the document
    public int getTermFrequency(int pageID, int wordID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        Vector<Integer> pageList = new Vector<Integer>();
        if (hashtable.get(hashKey) != null)
        {
            pageList = (Vector<Integer>) hashtable.get(hashKey);
        }
        return Collections.frequency(pageList, wordID);
    }

    // calculate the maximum term frequency
    public void calculateMaxTermFrequency(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        Vector<Integer> pageList = new Vector<Integer>();
        if (hashtable.get(hashKey) == null)
        {
            System.out.println("Error: Calulating Maximum Term Frequency");
            return;
        }
        pageList = (Vector<Integer>) hashtable.get(hashKey);
        // removing duplicate term
        Set<Integer> unique = new HashSet<Integer>(pageList);
        int maximumTermFrequency = 0;
        for(int wordID : unique) {
            int termFrequency = Collections.frequency(pageList, wordID);
            if (termFrequency > maximumTermFrequency)
                maximumTermFrequency = termFrequency;
        }
        hashtable_mtf.put(hashKey, maximumTermFrequency);
    }

    // retrieve the term with the maximum number of term frequency
    public int getMaxTermFrequency(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        if(hashtable_mtf.get(hashKey) != null)
            return (Integer) hashtable_mtf.get(hashKey);
        else
            return 0;
    }

    // retrieve the term frequency map which consist the wordID and frequency of the term
    public Map<Integer, Integer> getTermFrequencyMap(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        if(hashtable.get(hashKey) != null) {
            Vector<Integer> wordList = (Vector<Integer>) hashtable.get(hashKey);
            // create a map which store the pair of word properties
            Map<Integer, Integer> hashMap = new HashMap<>();
            for (int wordID : wordList) {
                Integer frequency = hashMap.get(wordID);
                frequency = (frequency == null) ? 1 : ++frequency;
                hashMap.put(wordID, frequency);
            }
            return hashMap;
        }
        else {
            return null;
        }
    }

    // print out all the word and its corresponding pageID
    public void printAll() throws IOException
    {
        // scan through all keys
        FastIterator keyIterator = hashtable.keys();
        String key;
        while( (key = (String)keyIterator.next())!=null)
        {
            System.out.printf("PageID = %s, WordID = %s\n" , key, hashtable.get(key));
        }
    }

    // print the word and its frequency
    public void printPageTermFrequency(int pageID) throws IOException
    {
        Map<Integer, Integer> map = getTermFrequencyMap(pageID);
        MappingIndex wordIndex = new MappingIndex(recman, "wordMappingIndex");

        for (int k : map.keySet()) {
            System.out.printf("%s:%s; ", wordIndex.getKey(k), map.get(k));
        }
        System.out.println();
    }

    // standardize the printing statement for term frequency
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

    // retrieve the list of pageOD
    public Vector<Integer> getExistingPageIdList() throws IOException
    {
        Vector<Integer> tempVector = new Vector<Integer>();
        FastIterator keyIterator = hashtable.keys();
        String hashKey;
        while( (hashKey = (String)keyIterator.next())!=null)
        {
            tempVector.add(Integer.parseInt(hashKey));
        }
        return tempVector;
    }


}
