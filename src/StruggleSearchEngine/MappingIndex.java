package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
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
//        recman = RecordManagerFactory.createRecordManager(DB_ROOT_FOLDER + recordmanager);
        recman = recordmanager;
        recid = recman.getNamedObject(objectname);
        recid_r = recman.getNamedObject(objectname+"Reverted");

        lastIDRecid = recman.getNamedObject(objectname+"ID");

        if (recid != 0)
        {
            // if hashtable exist, load it
            hashtable = HTree.load(recman, recid);
            hashtable_r = HTree.load(recman, recid_r);

            lastID = (Integer)recman.fetch(lastIDRecid);
        }
        else
        {
            //for test
            //System.out.println("Initial hashtable");
            // initial hashtables
            hashtable = HTree.createInstance(recman);
            hashtable_r = HTree.createInstance(recman);

            recman.setNamedObject(objectname, hashtable.getRecid());
            recman.setNamedObject(objectname+"Reverted", hashtable_r.getRecid());
            recman.setNamedObject(objectname+"ID", recman.insert(new Integer(0)));
            lastIDRecid = recman.getNamedObject(objectname+"ID");
            lastID = 0;
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

    public boolean insert(String key) throws IOException
    {
        if(hashtable.get(key) == null)
        {
            // increase the last id before insert
            lastID++;
            hashtable.put(key, lastID);
            hashtable_r.put(lastID, key);
            recman.update(lastIDRecid, new Integer(lastID)); // write the last id to db
//            recman.commit();

            return true;
        }
        return false;
    }

    // -1 : value not found
    public int getValue(String key) throws IOException
    {
        if(hashtable.get(key) != null)
            return (int)hashtable.get(key);
        else
            return -1;
    }

    // -1 : value not found
    public String getKey(int value) throws IOException
    {
//        String valueStr = Integer.toString(value);
        if(hashtable_r.get(value) != null)
            return (String) hashtable_r.get(value);
        else
            return "NO STRING";
    }

//    public String getKey(int value) throws IOException
//    {
//        // iterate through all keys
//        FastIterator iter = hashtable.keys();
//        String key;
//        while( (key = (String)iter.next())!=null)
//        {
//            if(hashtable.get(key) != null && (int) hashtable.get(key) == value)
//            {
//                return key;
//            }
//        }
//        return null;
//    }

    public void finalize() throws IOException
    {
//        System.out.println(lastIDRecid);
        recman.update(lastIDRecid, new Integer(lastID)); // write the last id to db
        recman.commit();
//        recman.close();
    }

    public Vector<String> getUrlList() throws IOException
    {
        FastIterator iter = hashtable.keys();
        Vector<String> v = new Vector<String>();
        while( (key = (String)iter.next())!=null)
        {
            v.add(key);
//            System.out.printf("KEY= %s, ID= %s\n" , key, hashtable.get(key));
        }
        return v;
    }
    public void printAll() throws IOException
    {
        // Print all the data in the hashtable

        // iterate through all keys
        FastIterator iter = hashtable.keys();
        String key;
        while( (key = (String)iter.next())!=null)
        {
            System.out.printf("KEY= %s, ID= %s\n" , key, hashtable.get(key));
        }

    }

}
