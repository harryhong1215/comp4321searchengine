package StruggleSearchEngine;

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
        String hashKey = Integer.toString(wordID);

        // if there is no previous exsistance of word before
        if (hashtable.get(hashKey) == null)
        {
            // initial new map and wordPosList
            Posting p = new Posting(pageID);
            p.insert(wordPos);

            HashMap<Integer, Posting> hashMap = new HashMap<Integer, Posting>();
            // link hash map with pageID
            hashMap.put(pageID, p);
            // put the hash map into correlated hash table position
            hashtable.put(hashKey, hashMap);
        }
        else
        {
            // if word already exist
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            if(contains(wordID, pageID))
            {
                // get the posting of specific pageID
                Posting postingList = hashMap.get(pageID);

                // ensure the insertion is unique
                if(postingList.contains(wordPos))
                {
                    return;
                }
                else
                {
                    postingList.insert(wordPos);
                    hashtable.put(hashKey, hashMap);
                }

            }
            else
            {
                // merge the old posting with new wordPos and put into a new position in map with new pageID
                Posting postingList = new Posting(pageID);
                postingList.insert(wordPos);
                hashMap.put(pageID, postingList);
                hashtable.put(hashKey, hashMap);
            }
        }
    }

    // check if the hash map contain the specific word's posting list
    public boolean contains(int wordID, int pageID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) == null)
        {
            return false;
        }
        else
        {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            return hashMap.containsKey(pageID);
        }
    }

    // delete the data of dedicate webpage and its coressponding wordlist
    public boolean delete(int wordID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) != null)
        {
            hashtable.remove(hashKey);
            return true;
        }
        else {
            return false;
        }
    }

    // delete the word pair which consist the word and its corresponding posting wordlist
    public boolean delete(int wordID, int pageID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) != null) {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            hashMap.remove(pageID);
            hashtable.put(hashKey, hashMap);
            return true;
        }
        else {
            return false;
        }
    }

    // given the pageID and wordID, delete the dedicate word's position inside the document
    public boolean delete(int wordID, int pageID, int wordPos) throws IOException
    {
        String hashkey = Integer.toString(wordID);
        if (hashtable.get(hashkey) != null)
        {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashkey);
            Posting postingList = hashMap.get(pageID);
            postingList.remove(wordPos);
            hashtable.put(hashkey, hashMap);
            return true;
        }
        else {
            return false;
        }
    }

    // retrieve the term frequency of the word in the document
    public int getTermFrequency(int wordID, int pageID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) != null) {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            Posting posting = hashMap.get(pageID);
            return posting.getTermFrequency();
        }
        else {
            return -1;
        }
    }

    // get the pageID from the hash map
    public HashMap<Integer, Posting> get(int wordID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        return (HashMap<Integer, Posting>) hashtable.get(hashKey);
    }

    // retrieve the number of document have the word
    public int getDocumentFrequency(int wordID) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) != null) {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            return hashMap.size();
        }
        else {
            return -1;
        }
    }

    // check if input wordId and word pos is exist in certain page
    public boolean containsWordPosition(int pageID, int wordID, int wordPos) throws IOException
    {
        String hashKey = Integer.toString(wordID);
        if (hashtable.get(hashKey) != null) {
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            Posting posting = hashMap.get(pageID);
            return posting.containsWordPos(wordPos);
        }
        else {
            return false;
        }
    }


    // print all element in the hash table
    public void printAll() throws IOException
    {
        // solution of lab2's work
        FastIterator keyIterator = hashtable.keys();

        String hashKey;
        System.out.println("Inverted Index:");

        while( (hashKey = (String)keyIterator.next()) != null)
        {
            // retrieve and print the element in the hash table
            HashMap<Integer, Posting> hashMap = (HashMap<Integer, Posting>) hashtable.get(hashKey);
            Set<Integer> keys = hashMap.keySet();  // Obtain all key for get the element in hash map
            for(Integer i: keys)
            {
                System.out.println("wordID:" + hashKey + " " + hashMap.get(i));
            }
        }

    }

}

