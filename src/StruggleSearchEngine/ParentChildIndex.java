package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.Vector;

public class ParentChildIndex {

    private RecordManager recman;
    private HTree hashtable;

    public ParentChildIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        long recid = recman.getNamedObject(objectname);

        if (recid == 0)
        {
            // create a new hashtable
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject( objectname, hashtable.getRecid() );
        }
        else
        {
            // load the existing hashtable if exist
            hashtable = HTree.load(recman, recid);
        }
    }

    // save all changes to database
    public void finalize() throws IOException
    {
        recman.commit();
    }

    // generate hashID through pageID and hash the page to the pagelist
    public void insert(int firstPageID, int secondPageID) throws IOException
    {
        String hashkey = Integer.toString(firstPageID);
        Vector<Integer> pageList = null;

        if (hashtable.get(hashkey) != null)
            pageList = (Vector<Integer>) hashtable.get(hashkey);
        else
            pageList = new Vector<Integer>();

        // ensure the insertion is not duplicated
        if (! pageList.contains(secondPageID))
        {
            pageList.add(secondPageID);
        }

        // hash the page list to the page table
        hashtable.put(hashkey, pageList);
    }

    // remove the pagelist from hashtable
    public void delete(int pageID) throws IOException
    {
        String hashKey = Integer.toString(pageID);
        hashtable.remove(hashKey);
    }

    // remove the element in the pagelist
    public void delete(int firstPageID, int secondPageID) throws IOException
    {
        String hashKey = Integer.toString(firstPageID);
        if (hashtable.get(hashKey) == null)
        {
            return;
        }
        else
        {
            Vector<Integer> pageList = (Vector<Integer>) hashtable.get(hashKey);
            pageList.remove((Integer) secondPageID);
            hashtable.put(hashKey, pageList);
        }
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

    // print out all the page and its corresponding key
    public void printAll() throws IOException
    {
        // scan through all keys
        FastIterator keyIterator = hashtable.keys();
        String key;
        while( (key = (String)keyIterator.next()) != null)
        {
            System.out.printf("Key = %s, Pages = %s\n" , key, hashtable.get(key));
        }
    }

    // print the URL and its index
    public void printWithPageID(int pageID) throws IOException
    {
        // scan through all keys
        MappingIndex urlIndex = new MappingIndex(recman, "urlMappingIndex");
        Vector<Integer> v = getList(pageID);
        for(Integer i : v)
        {
            System.out.println(urlIndex.getKey(i));
        }
    }
}
