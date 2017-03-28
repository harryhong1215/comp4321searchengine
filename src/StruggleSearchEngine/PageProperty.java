package StruggleSearchEngine;

import jdbm.RecordManager;
import jdbm.htree.HTree;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;


class Properties implements Serializable {
    private String title;
    private String url;
    private Date modDate;
    private int size;

    public Properties(String title, String url, Date Lastmodificationdate, int sizeofpage) {
        this.title = title;
        this.url = url;
        this.modDate = Lastmodificationdate;
        this.size = sizeofpage;
    }

    @Override
    public String toString() {
        return "properties{" +
                "title='" + title + '\'' +
                ", url='" + url + '\'' +
                ", modDate=" + modDate +
                ", size=" + size +
                '}';
    }

    public String getTitle() {
        return title;
    }

    public String getUrl() {
        return url;
    }

    public Date getModDate() {
        return modDate;
    }

    public int getSize() {
        return size;
    }
}

public class PageProperty {


    private RecordManager recman;
    private HTree hashtable;
    private long recmanid;

    public PageProperty(RecordManager recordmanager, String name) throws IOException
    {
        this.recman = recordmanager;
        this.recmanid = recman.getNamedObject(name);

        if (recmanid != 0)
        {
            this.hashtable = HTree.load(recman, recmanid); //if the hashtable is existing, just load the hashtable
        }
        else
        {
            //fo
            //System.out.println("Initial new PageProperty Hashtable");
            // create a new  hashtable to store data
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject(name, hashtable.getRecid());
        }
    }

    // check if there are existing a key value
    public boolean isContains(int key) throws IOException
    {
        if(key < 0) {
            return false; //if key is < 0, is is error; return false;
        }

        String keyvalue = Integer.toString(key); //chagne integer to string
        return (hashtable.get(keyvalue) != null); // if it contains the object, return true; otherwise, return false;
    }


    // the keyvalue is key, the value is the properties(object)
    public boolean insert(int key, String title, String url, Date modDate, int size) throws IOException
    {
        if(key < 0) {
            return false; //if key is < 0, is is error; return false;
        }

        String keyvalue = Integer.toString(key); //chagne integer to string
        if(!isContains(key)) // if it contains the object, return true; otherwise, return false;
        {
            Properties characteristic = new Properties(title, url, modDate, size); //create a new properties to store those data.
            hashtable.put(keyvalue, characteristic); // put the new properties which created by the previous line to the hashtable
            return true;
        }
        return false;
    }


    // return the properties by using the key
    public Properties get(int key) throws IOException
    {
        String keyvalue = Integer.toString(key); //change integer to string
        return (Properties) hashtable.get(keyvalue); //return the properties which get by the key

    }

    public void printWithPageID(int pageID) throws IOException
    {
        String key = Integer.toString(pageID); //change integer to string
        Properties characteristic = (Properties) hashtable.get(key);  //return the properties which get by the key
        // System.out.println(characteristic.getModDate() + " Size:" +characteristic.getSize());
    }

    public void finalize() throws IOException
    {
        recman.commit();
        recman.close();
    }

}