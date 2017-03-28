package SE;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Vector;

public class Indexer {
    // Open Global Mapping Index (pageid & docid mapping)
    // Remove stop words and do stemming
    // manipulate invertedIndex (body & title)

    private static MappingIndex urlIndex;
    private static MappingIndex wordIndex;
    private static InvertedIndex titleInvertedIndex;
    private static InvertedIndex bodyInvertedIndex;
    private static PageProperty properyIndex;
    private static ForwardIndex forwardIndex;

    private static ParentChildIndex parentChildIndex;   // parentPageId -> {childPageIdList}
    private static ParentChildIndex childParentIndex;   // childPageId -> {parentPageIdList}

    private static StopStem stopStem;

    private static RecordManager recman;
    private static String url;
    private static int pageID;

    public Indexer(String dbRootPath, String url) throws IOException
    {
        // create main database
        recman = RecordManagerFactory.createRecordManager(dbRootPath);

        // initialization of all required index for the database
        urlIndex = new MappingIndex(recman, "urlMappingIndex");
        wordIndex = new MappingIndex(recman, "wordMappingIndex");
        titleInvertedIndex = new InvertedIndex(recman, "titleInvertedIndex");
        bodyInvertedIndex = new InvertedIndex(recman, "bodyInvertedIndex");
        properyIndex = new PageProperty(recman, "pagePropertyIndex");
        parentChildIndex = new ParentChildIndex(recman, "parentChildIndex");
        childParentIndex = new ParentChildIndex(recman, "childParentIndex");
        forwardIndex = new ForwardIndex(recman, "forwardIndex");

        stopStem = new StopStem("stopwords.txt");

        this.url = url;
        urlIndex.insert(url);
        this.pageID = urlIndex.getValue(url);
    }

    // perform stopword analysis and remove stopword -> perform stemming -> insert word to the mapping index
     private int insertWordToMappingIndex(String word)
    {
        // check if the word is empty
        if(word == null || word.length() <= 0 || word.equals(""))
        {
            System.out.println("ERROR: Insert Title invalid word");
            return -2;
        }

        // check if the word is a stop
        if (stopStem.isStopWord(word))
        {
            return -1;
        }

        // obtain stemmed word
        String stem = stopStem.stem(word);
        if(stem == null || stem.length() <= 0 || stem.equals(""))
        {
            return -2;
        }

        try {
            // insert into word mapping indexer
            wordIndex.insert(stem);
            int stemWordID = wordIndex.getValue(stem);
            return stemWordID;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -3;
    }

    // remove stopword and stem -> inserted to inverted index and use forward index
    public void insertWords(Vector<String> words) throws IOException
    {
        if(words.isEmpty())
            return;

        // delete the previous content in forward index
        forwardIndex.delete(this.pageID);

        // scan through all element in the wordlist
        int wordPos = 0;
        while(wordPos < words.size())
        {
            int wordID = insertWordToMappingIndex(words.get(wordPos));    // put new word to mapping index

            // discard all the not suitable word
            if(wordID > 0)
            {
                bodyInvertedIndex.insert(wordID, this.pageID, wordPos);
                forwardIndex.insert(this.pageID, wordID);
            }
            wordPos++;
        }

        // Calculate the maximum term frequency and insert it to forwardIndex
        forwardIndex.calculateMaxTermFrequency(this.pageID);
    }

    // insert title to inverted index file to store the detail
    public void insertTitle(Vector<String> words) throws IOException
    {
        if(!words.isEmpty())
        {
            // scan through all element in the wordlist
            int wordPos = 0;
            while(wordPos < words.size())
            {
                // retrieve index of the of the title by insert word to mapping index
                int wordID = insertWordToMappingIndex(words.get(wordPos));

                // insert the inverted index if it is not stopword
                if(wordID > 0)
                {
                    titleInvertedIndex.insert(wordID, this.pageID, wordPos);
                }
                wordPos++;
            }
        }
        else {
            return;
        }
    }

    // insert all properties of the page to property index
    public void insertPageProperty(String title, String url, Date modDate, int size) throws IOException
    {
        properyIndex.insert(this.pageID, title, url, modDate, size);
    }

    // check if the page has been updated and require to fetch again
    public boolean pageLastModDateIsUpdated(Date newDate) throws IOException
    {
        if(properyIndex.get(this.pageID) == null)
            return true;

        Properties p = properyIndex.get(this.pageID);
        return (! newDate.equals(p.getModDate()));
    }

    // return true if page url exist in url mapping index
    public boolean pageIsContains() throws IOException
    {
        return (urlIndex.getValue(this.url) > 0);
    }

    // insert child pages to parentChildIndex, call by the parent page
    public void insertChildPage(String url) throws IOException
    {
        int childPageId = urlIndex.getValue(url);
        if(childPageId < 0) {
            urlIndex.insert(url);
            childPageId = urlIndex.getValue(url);
        }
        parentChildIndex.insert(this.pageID, childPageId);
    }

    // insert parent pages to childParent, call by the child page
    public void insertParentPage(String url) throws IOException
    {
        int parentPageId = urlIndex.getValue(url);
        if(parentPageId < 0) {
            urlIndex.insert(url);
            parentPageId = urlIndex.getValue(url);
        }
        childParentIndex.insert(this.pageID, parentPageId);
    }

    public void printWordMappingIndex() throws IOException
    {
        wordIndex.printAll();
    }

    public void printUrlMappingIndex() throws IOException
    {
        urlIndex.printAll();
    }

    public void printForwardIndex() throws IOException
    {
        forwardIndex.printAll();
    }

    public void printTitleInvertedIndex() throws IOException
    {
        titleInvertedIndex.printAll();
    }

    public void printBodyInvertedIndex() throws IOException
    {
        bodyInvertedIndex.printAll();
    }

    public void printChildPages() throws IOException
    {
        System.out.println("----- Child Pages -----");
        if(parentChildIndex.getList(this.pageID) == null)
        {
            System.out.println("ERROR: no child page found");
            return;
        }
        Vector<Integer> list = parentChildIndex.getList(this.pageID);
        for (int pid : list) {
            System.out.println(urlIndex.getKey(pid));
        }
    }

    public void printParentPages() throws IOException
    {
        System.out.println("----- Parent Pages -----");
        if(childParentIndex.getList(this.pageID) == null)
        {
            System.out.println("ERROR: no parent page found");
            return;
        }
        Vector<Integer> list = childParentIndex.getList(this.pageID);
        for (int pid : list) {
            System.out.println(urlIndex.getKey(pid));
        }
    }

    public void printPageTermFrequency() throws IOException
    {
        if(forwardIndex.getTermFrequencyMap(this.pageID) == null)
        {
            System.out.println("ERROR: no term frequency map found");
            return;
        }
        Map<Integer, Integer> map = forwardIndex.getTermFrequencyMap(this.pageID);

        for (int k : map.keySet()) {
            System.out.printf("%s:%s; ", wordIndex.getKey(k), map.get(k));
        }
        System.out.println();
    }

    public void printPageProperty() throws IOException
    {
        if(properyIndex.get(this.pageID) == null)
        {
            System.out.println("ERROR: no page property found");
            return;
        }
        Properties ppt = (Properties) properyIndex.get(this.pageID);
        System.out.println(ppt.getUrl());
        System.out.println(ppt.getModDate() + " Size:" +ppt.getSize());

    }

    public Vector<String> getUrlLinkList() throws IOException
    {
        return urlIndex.getUrlList();
    }

    public ForwardIndex getForwardIndex() throws IOException{
        return forwardIndex;
    }

    public void finalize() throws IOException
    {
        recman.commit();
        recman.close();
    }


}
