package SE;

//jdbm
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

//java
import java.io.IOException;
import java.util.Date;
import java.util.Vector;

public class Indexer {
    // Open Global Mapping Index (pageid & docid mapping)
    // Remove stop words and do stemming
    // manipulate invertedIndex (body & title)

    private static MappingIndex urlMappingIndex;
    private static MappingIndex wordMappingIndex;
    private static InvertedIndex titleInvertedIndex;
    private static InvertedIndex bodyInvertedIndex;
    private static PageProperty properyIndex;
    private static ForwardIndex forwardIndex;

    private static ParentChildIndex parentChildIndex;   // parentPageId -> {childPageIdList}
    private static ParentChildIndex childParentIndex;   // childPageId -> {parentPageIdList}

    private static StopStem stopStem;

    private static RecordManager recordManager;
    private static String targetURL;
    private static int pageID;

    public Indexer(String dbRootPath, String url) throws IOException{
        // create main database
        recordManager = RecordManagerFactory.createRecordManager(dbRootPath);
        // initialization of all required index for the database
        urlMappingIndex = new MappingIndex(recordManager, "urlMappingIndex");
        wordMappingIndex = new MappingIndex(recordManager, "wordMappingIndex");

        titleInvertedIndex = new InvertedIndex(recordManager, "titleInvertedIndex");
        bodyInvertedIndex = new InvertedIndex(recordManager, "bodyInvertedIndex");

        parentChildIndex = new ParentChildIndex(recordManager, "parentChildIndex");
        childParentIndex = new ParentChildIndex(recordManager, "childParentIndex");

        forwardIndex = new ForwardIndex(recordManager, "forwardIndex");

        properyIndex = new PageProperty(recordManager, "pagePropertyIndex");

        stopStem = new StopStem("stopwords.txt");

        this.targetURL = url;

        urlMappingIndex.insert(url);

        this.pageID = urlMappingIndex.getValue(url);
    }

    // perform stopword analysis and remove stopword -> perform stemming -> insert word to the mapping index
     private int insertWordIntoMappingIndex(String word){
        // check if the word is empty
        if(word == null || word.equals("")|| !(word.length() > 0) ){
            System.out.println("Invalid title word");
            return -2;
        }

        // check if the word is a stop
        if (stopStem.isStopWord(word)){
            return -1;
        }

        // obtain stemmed word
        String stem = stopStem.stem(word);
        if(stem == null || stem.length() <= 0 || stem.equals("")){
            return -2;
        }

        try {
            // insert into word mapping indexer
            wordMappingIndex.insert(stem);
            int stemWordID = wordMappingIndex.getValue(stem);
            return stemWordID;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -3;
    }

    // remove stopword and stem -> inserted to inverted index and use forward index
    public void insertWords(Vector<String> words) throws IOException{
        if(words.isEmpty()){
            return;
        }

        // delete the previous content in forward index
        forwardIndex.delete(this.pageID);

        // scan through all element in the wordlist
        int wordPos = 0;
        while(wordPos < words.size()){
            int wordID = insertWordIntoMappingIndex(words.get(wordPos));// put new word to mapping index
            // discard all the not suitable word
            if(wordID > 0){
                bodyInvertedIndex.insert(wordID, this.pageID, wordPos);
                forwardIndex.insert(this.pageID, wordID);
            }
            wordPos++;
        }

        // Calculate the maximum term frequency and insert it to forwardIndex
        forwardIndex.calculateMaxTermFrequency(this.pageID);
    }

    // insert title to inverted index file to store the detail
    public void insertTitle(Vector<String> words) throws IOException{
        if(!words.isEmpty()){
            // scan through all element in the wordlist
            int wordPos = 0;
            while(wordPos < words.size()){
                // retrieve index of the of the title by insert word to mapping index
                int wordID = insertWordIntoMappingIndex(words.get(wordPos));
                // insert the inverted index if it is not stopword
                if(wordID > 0){
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
    public void insertPageProperty(String title, String url, Date modDate, int size) throws IOException{
        properyIndex.insert(this.pageID, title, url, modDate, size);
    }

    // check if the page has been updated and require to fetch again
    public boolean pageLastModDateIsUpdated(Date newDate) throws IOException{
        if(properyIndex.get(this.pageID) == null){
            return true;
        }

        Properties p = properyIndex.get(this.pageID);
        return (! newDate.equals(p.getModDate()));
    }

    // insert child pages to parentChildIndex, call by the parent page
    public void insertChildPage(String url) throws IOException{
        int childPageId = urlMappingIndex.getValue(url);
        if(childPageId < 0) {
            urlMappingIndex.insert(url);
            childPageId = urlMappingIndex.getValue(url);
        }
        parentChildIndex.insert(this.pageID, childPageId);
    }

    public void finalize() throws IOException{
        recordManager.commit();
        recordManager.close();
    }
}
