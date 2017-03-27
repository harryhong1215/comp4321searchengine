package SE;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.util.Vector;


/**
 * Created by opw on 4/6/16.
 */

// ParentPageID -> (ChildPageID1 ... ChildPageIDN)
// ChildPageID -> (ParentPageID1 ... ParentPageIDN)

public class ParentChildIndex {

    private RecordManager recman;
    private HTree hashtable;

    public ParentChildIndex(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        long recid = recman.getNamedObject(objectname);

        if (recid != 0)
        {
            // if hashtable exist, load it
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
//        recman.close();
    }

    public void insert(int pageID1, int pageID2) throws IOException
    {
        // pageID -> wordPosList
        String key = Integer.toString(pageID1);
        Vector<Integer> pages = null;

        if (hashtable.get(key) == null)
            pages = new Vector<Integer>();
        else
            pages = (Vector<Integer>) hashtable.get(key);

        // ensure unique insert
        if (! pages.contains(pageID2))
        {
            pages.add(pageID2);
        }

        hashtable.put(key, pages);  // commit changes
    }

    public void delete(int pageID1) throws IOException
    {
        String key = Integer.toString(pageID1);
        hashtable.remove(key);
    }

    public void delete(int pageID1, int pageID2) throws IOException
    {
        String key = Integer.toString(pageID1);
        if (hashtable.get(key) != null)
        {
            Vector<Integer> pages = (Vector<Integer>) hashtable.get(key);
            pages.remove((Integer) pageID2);
            hashtable.put(key, pages);  // commit changes
        }
    }

    public Vector<Integer> getList(int pageID1) throws IOException
    {
        String key = Integer.toString(pageID1);
        Vector<Integer> list = new Vector<Integer>();
        if (hashtable.get(key) != null)
        {
            list = (Vector<Integer>) hashtable.get(key);
        }
        return list;
    }

    public void printAll() throws IOException
    {
        // Print all the data in the hashtable

        // iterate through all keys
        FastIterator iter = hashtable.keys();
        String key;
        while( (key = (String)iter.next())!=null)
        {
            System.out.printf("KEY= %s, PAGES= %s\n" , key, hashtable.get(key));
        }
    }


    public void printWithPageID(int pageID) throws IOException
    {
        // iterate through all keys
        MappingIndex urlIndex = new MappingIndex(recman, "urlMappingIndex");
        Vector<Integer> v = getList(pageID);
        for(Integer i : v)
        {
            System.out.println(urlIndex.getKey(i));
        }

    }
}
