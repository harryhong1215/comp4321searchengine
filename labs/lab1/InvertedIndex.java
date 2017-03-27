/* --
COMP336 Lab1 Exercise
Student Name:
Student ID:
Section:
Email:
*/

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.htree.HTree;
import jdbm.helper.FastIterator;
import java.util.Vector;
import java.io.IOException;
import java.io.Serializable;

class Posting implements Serializable
{
    public String doc;
    public int freq;
    Posting(String doc, int freq)
    {
        this.doc = doc;
        this.freq = freq;
    }
}

public class InvertedIndex
{
    private RecordManager recman;
    private HTree hashtable;

    InvertedIndex(String recordmanager, String objectname) throws IOException
    {
        recman = RecordManagerFactory.createRecordManager(recordmanager);
        long recid = recman.getNamedObject(objectname);

        if (recid != 0)
            hashtable = HTree.load(recman, recid);
        else
        {
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject( "ht1", hashtable.getRecid() );
        }
    }


    public void finalize() throws IOException
    {
        recman.commit();
        recman.close();
    }

    public void addEntry(String word, int x, int y) throws IOException
    {
        // Add a "docX Y" entry for the key "word" into hashtable
        // ADD YOUR CODES HERE

        String new_posting = String.format("doc%d %d", x, y);

        // if the index word does not exist
        if(hashtable.get(word)==null)
        {
            hashtable.put(word, new_posting);
        }
        else
        {
            String cur_posting = hashtable.get(word).toString();

            // ensure unique insert
            if(! cur_posting.contains(new_posting))
            {
                // append the new entry to the last of posting list
                hashtable.put(word, cur_posting + ' ' + new_posting);
            }
        }

    }
    public void delEntry(String word) throws IOException
    {
        // Delete the word and its list from the hashtable
        // ADD YOUR CODES HERE
        // remove the index word and its posting list
        hashtable.remove(word);
    }
    public void printAll() throws IOException
    {
        // Print all the data in the hashtable
        // ADD YOUR CODES HERE

        // iterate through all keys
        FastIterator iter = hashtable.keys();

        String key;
        while( (key = (String)iter.next())!=null)
        {
            // get and print the content of each key
            System.out.println(key + " = " + hashtable.get(key));
        }

    }

}

