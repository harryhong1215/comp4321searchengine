package SE;

import jdbm.RecordManager;
import jdbm.htree.HTree;
import jdbm.helper.FastIterator;

import java.util.*;
import java.io.IOException;
import java.io.Serializable;

// In this system, it is for the position of the exsistance of word in the document
class Posting implements Serializable
{
    private int pageID;
    private Vector<Integer> wordPosList;
    private static final long serialVersionUID = 1L;    // define serialVersionUID

    public Posting(int pageID) {
        this.pageID = pageID;
        this.wordPosList = new Vector<Integer>();
    }

    @Override
    public String toString() {
        return "Posting{" +
                "pageID=" + pageID +
                ", wordPosList=" + wordPosList +
                '}' ;
    }

    public boolean insert(int wordPos) {
        if(wordPos >= 0){
            wordPosList.add(wordPos);
            return true;
        }
        return false;
    }

    public boolean contains(int wordPos) {
        if(wordPos >= 0){
            return wordPosList.contains(wordPos);
        }
        return false;
    }

    public boolean remove(int wordPos) {
        if(wordPos >= 0){
            wordPosList.remove((Integer) wordPos);
            return true;
        }
        return false;
    }

    public int getTermFrequency() {
        return wordPosList.size();
    }

    public Vector<Integer> getWordPosList() {
        return wordPosList;
    }

    public boolean containsWordPos(int wordPos) {
        return wordPosList.contains(wordPos);
    }

}

public class InvertedIndex
{
    private RecordManager recman;
    private HTree hashtable;

    InvertedIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        long recid = recman.getNamedObject(objectname);

        if (recid != 0)
        {
            // Load existing has table if exist
            hashtable = HTree.load(recman, recid);
        }
        else
        {
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject( objectname, hashtable.getRecid() );
        }
    }


    public void finalize() throws IOException
    {
        recman.commit();
    }

    //  insert the data pair into the hash map with wordID -> [pageID, word posistion]
    public void insert(int wordID, int pageID, int wordPos) throws IOException
    {
        // Create a hash key for the position of word in hash table
        String key = Integer.toString(wordID);

        // if there is no previous exsistance of word before
        if (hashtable.get(key) == null)
        {
            // initial new map and wordPosList
            Posting p = new Posting(pageID);
            p.insert(wordPos);

            HashMap<Integer, Posting> map = new HashMap<Integer, Posting>();
            // link hash map with pageID
            map.put(pageID, p);
            // put the hash map into correlated hash table position
            hashtable.put(key, map);
        }
        else
        {
            // if word already exist
            HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
            if(contains(wordID, pageID))
            {
                // get the posting of specific pageID
                Posting posting = map.get(pageID);

                // ensure the insertion is unique
                if(posting.contains(wordPos))
                {
                    return;
                }
                else
                {
                    posting.insert(wordPos);
                    hashtable.put(key, map);
                }

            }
            else
            {
                // merge the old posting with new wordPos and put into a new position in map with new pageID
                Posting posting = new Posting(pageID);
                posting.insert(wordPos);
                map.put(pageID, posting);
                hashtable.put(key, map);
            }
        }
    }

    // check if the hash map contain the specific word's posting list
    public boolean contains(int wordID, int pageID) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
        {
            return false;
        }
        else
        {
            HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
            return map.containsKey(pageID);
        }
    }

    public boolean delete(int wordID) throws IOException
    {
        // Delete the word and its list from the hashtable
        // remove the index word and its posting list
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return false;
        hashtable.remove(key);
        return true;
    }

    // Delete the posting(including all the wordpos) with specific pageID
    public boolean delete(int wordID, int pageID) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return false;
        HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
        map.remove(pageID);
        hashtable.put(key, map);    // commit changes
        return true;
    }

    // Delete wordpost of specifc pageid
    public boolean delete(int wordID, int pageID, int wordPos) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return false;
        HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
        Posting posting = map.get(pageID);
        posting.remove(wordPos);
        hashtable.put(key, map);    // commit changes
        return true;
    }

    // get the term frequency by counting # of wordPos in posting
    public int getTermFrequency(int wordID, int pageID) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return -1;
        HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
        Posting posting = map.get(pageID);
        return posting.getTermFrequency();
    }

    // get page id map with word ID
    public HashMap<Integer, Posting> get(int wordID) throws IOException
    {
        String key = Integer.toString(wordID);
        return (HashMap<Integer, Posting>) hashtable.get(key);
    }

    // number of documents containing the term wordID
    public int getDocumentFrequency(int wordID) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return -1;
        HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
        return map.size();
    }

    // check if input wordId and word pos is exsit in centain page
    public boolean containsWordPos(int pageID, int wordID, int wordPos) throws IOException
    {
        String key = Integer.toString(wordID);
        if (hashtable.get(key) == null)
            return false;
        HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
        Posting posting = map.get(pageID);
        return posting.containsWordPos(wordPos);
    }


    // Print element in hashtable
    public void printAll() throws IOException
    {
        // From Lab2 solution
        FastIterator iter = hashtable.keys();

        String key;
        System.out.println("Inverted Index:");

        while( (key = (String)iter.next()) != null)
        {
            // Retrieve and print the element
            HashMap<Integer, Posting> map = (HashMap<Integer, Posting>) hashtable.get(key);
            Set<Integer> keys = map.keySet();  // Obtain all key for get the element in hash map
            for(Integer i: keys)
            {
                System.out.println("wordID:" + key + " " + map.get(i));
            }
        }

    }

}

