package SE;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

class Properties implements Serializable {
    private String title;
    private String url;
    private Date modDate;
    private int size;

    public Properties(String title, String url, Date modDate, int size) {
        this.title = title;
        this.url = url;
        this.modDate = modDate;
        this.size = size;
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

    private int pageID;
    private RecordManager recman;
    private HTree hashtable;
    private long recid;

    public PageProperty(RecordManager recordmanager, String objectname) throws IOException
    {
        recman = recordmanager;
        recid = recman.getNamedObject(objectname);

        if (recid != 0)
        {
            // if hashtable exist, load it
            hashtable = HTree.load(recman, recid);
        }
        else
        {
            System.out.println("Initial new PageProperty Hashtable");
            // initial hashtable
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject(objectname, hashtable.getRecid());
        }
    }

    // KEY: pageID, VALUE: Properties(Object)
    public boolean insert(int pageID, String title, String url, Date modDate, int size) throws IOException
    {
        String key = Integer.toString(pageID);
        if(!isContains(pageID))
        {
            Properties p = new Properties(title, url, modDate, size);
            hashtable.put(key, p);
            return true;
        }
        return false;
    }

    // check exist using the key
    public boolean isContains(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        return (hashtable.get(key) != null);
    }

    // get the Properties obj using the key
    public Properties get(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        return (Properties) hashtable.get(key);
    }

    public void finalize() throws IOException
    {
        recman.commit();
        recman.close();
    }


    public void printWithPageID(int pageID) throws IOException
    {
        String key = Integer.toString(pageID);
        Properties ppt = (Properties) hashtable.get(key);
        System.out.println(ppt.getModDate() + " Size:" +ppt.getSize());
    }
}
