package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class MappingIndex {
    private String key;
    private int value;
    private int lastID;

    private RecordManager recman;
    private HTree hashtable;
    private HTree hashtable_r;

    private long recid;
    private long recid_r;
    private long lastIDRecid;

    public MappingIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        recid = recman.getNamedObject(objectname);
        recid_r = recman.getNamedObject(objectname+"Reverted");

        lastIDRecid = recman.getNamedObject(objectname+"ID");

        if (recid <= 0 )
        {
            //for test and debug
            //System.out.println("Initial hastable");
            // create a new hashtable
            hashtable = HTree.createInstance(recman);
            hashtable_r = HTree.createInstance(recman);

            recman.setNamedObject(objectname, hashtable.getRecid());
            recman.setNamedObject(objectname+"Reverted", hashtable_r.getRecid());
            recman.setNamedObject(objectname+"ID", recman.insert(new Integer(0)));
            lastIDRecid = recman.getNamedObject(objectname+"ID");
            lastID = 0;
        }
        else
        {
            // load the existing hashtable if exist
            hashtable = HTree.load(recman, recid);
            hashtable_r = HTree.load(recman, recid_r);
            lastID = (Integer)recman.fetch(lastIDRecid);
        }
    }

    @Override
    public String toString() {
        return "MappingIndex{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }

    public int getLastID() {
        return lastID;
    }

    // insert the data into the hash tree according to the key and lastID
    public boolean insert(String key) throws IOException
    {
        if(hashtable.get(key) == null)
        {
            lastID++;
            hashtable.put(key, lastID);
            hashtable_r.put(lastID, key);
            // write the lastID to the database
            recman.update(lastIDRecid, new Integer(lastID));
            return true;
        }
        return false;
    }

    // check if document's ID is exist
    public int getValue(String key) throws IOException
    {
        if(hashtable.get(key) == null) {
            return -1;
        }
        else {
            return (int) hashtable.get(key);
        }
    }

    // check if the document is exist
    public String getKey(int value) throws IOException
    {
        if(hashtable_r.get(value) == null) {
            return "No such string is exist";
        }
        else {
            return (String) hashtable_r.get(value);
        }
    }


    public void finalize() throws IOException
    {
        recman.update(lastIDRecid, new Integer(lastID));
        recman.commit();
    }

    // retrieve the list of URL
    public Vector<String> getUrlList() throws IOException
    {
        FastIterator keyIterator = hashtable.keys();
        Vector<String> tempVector = new Vector<String>();
        while( (key = (String)keyIterator.next())!=null)
        {
            tempVector.add(key);
        }
        return tempVector;
    }

    // print all the data in the hashtable
    public void printAll() throws IOException
    {
        FastIterator keyIterator = hashtable.keys();
        String key;
        while( (key = (String)keyIterator.next())!=null)
        {
            System.out.printf("Key = %s, ID = %s\n" , key, hashtable.get(key));
        }

    }

}
