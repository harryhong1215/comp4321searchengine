package StruggleSearchEngine;

//java
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.net.*;
import java.sql.Date;

//html parser
import org.htmlparser.Parser;
import org.htmlparser.util.ParserException;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.beans.StringBean;
import org.htmlparser.beans.LinkBean;
import org.htmlparser.util.NodeList;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.tags.*;

public class Crawler {
    private static Vector<String>  Finishedlist = new Vector<String>(); //store final output link
    private static Queue<String> temp = new LinkedList(); //store child link
    private static int MaximunPagenumber = 29; //number of grabbed pages
    private static final String Databasepath = "data/database"; // ouput folder path

    public static int counter = 0; //  count how many pages is done;

    private String crawlerTarget_URL_String; //store the link for crawl

    Crawler(String URL) {
        crawlerTarget_URL_String = URL;
    } //constructor for set the url

    //get private url
    public String getUrl() {
        return crawlerTarget_URL_String;
    }

    //get parser
    //from internet tutorial
    public Parser getParser() throws ParserException {
        try {
            return (new Parser(crawlerTarget_URL_String));
        } catch (ParserException parserException) {
            parserException.printStackTrace();
        }
        return null;
    }

    //store the word of title
    public Vector<String> extractTitle() throws ParserException {
        //from an simplified chinese online tutorial
        Parser parser = getParser();
        NodeFilter nodeFilter = new NodeClassFilter(TitleTag.class);
        NodeList nodeList = parser.parse(nodeFilter);
        String stringTitle = "";//store title
        for (int i = 0; i < nodeList.size(); i++) {
            Node node = nodeList.elementAt(i);
            if (node instanceof TitleTag) {
                TitleTag titleTag = (TitleTag) node;
                stringTitle = titleTag.getTitle();
            }
        }
        String[] stringSplit = stringTitle.split(" ");
        Vector<String> titleList = new Vector<>();
        for (int i = 0; i < stringSplit.length; i++){
            titleList.add(stringSplit[i]);
        }
        return titleList;
    }

    //get last update
    public Date lastUpdate() throws IOException {
         URL urlLastUpdate = new URL(getUrl());
        //reference from an online tutorial
        //check the connection and get the date
        URLConnection urlConnection = urlLastUpdate.openConnection();
        Date date = new Date(urlConnection.getLastModified());
        if (date == null){
            date.setTime(urlConnection.getDate());
        }else if (date.toString().equals("1970-01-01")) { //asd
            date.setTime(urlConnection.getDate());
        }
        return date;
    }

    //get all the words
    //from lab2 and parser
    public Vector<String> extractWords() throws ParserException {
        //from lab2
        Vector<String> wordList = new Vector<String>();
        StringBean stringBean = new StringBean();
        //from html parser example
        stringBean.setLinks(false);
        stringBean.setReplaceNonBreakingSpaces(true);
        stringBean.setCollapse(true);
        stringBean.setURL(crawlerTarget_URL_String);
        //from lab2
        String stringTemp = stringBean.getStrings();//change stringbean to string
        stringTemp = stringTemp.replaceAll("[,:!/.%|()-+&^#@*']", "");//delete the useless symbol
        StringTokenizer stringTokenizer = new StringTokenizer(stringTemp);//chop word
        while (stringTokenizer.hasMoreTokens())
            wordList.add(stringTokenizer.nextToken());
        return wordList;
    }

    //calculate the page size
    //from oracle
    public int getPageSize() throws IOException {

        URL urlPage = new URL(crawlerTarget_URL_String);
        URLConnection urlConnection = urlPage.openConnection();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        String stringTemp ="";
        String stringCounter ="";

        while ((stringTemp = bufferedReader.readLine()) != null) {
            stringCounter += stringTemp;
        }

        bufferedReader.close();
        return stringCounter.length();
    }

    //get link in page
    //from lab2
    public Vector<String> extractLinks() throws ParserException {
        //from lab2
        
        LinkBean bean = new LinkBean();
        bean.setURL(crawlerTarget_URL_String);

        URL[] urls = bean.getLinks();
        Vector<String> result = new Vector<String>();
        for (URL s : urls) {
            result.add(s.toString());
        }
        return result;
    }

    public static void grab(String url) throws IOException, ParserException {

        if ((Finishedlist.size() < MaximunPagenumber) == true) {

            Indexer indexer = new Indexer(Databasepath, url);
            Crawler crawler = new Crawler(url);

            if (temp.peek() != null)
            {
                temp.remove();
            }

            //get Last modification date of the url
            java.util.Date Lastmodificationdate = crawler.lastUpdate();

            //check lastupdate of the url
            if (indexer.pageLastModDateIsUpdated(Lastmodificationdate)) {

                //crawler part
                Vector<String> storedchildlinks = crawler.extractLinks();

                //add all the child link into temp
                int tempcounter = 0 ;
                while (tempcounter < storedchildlinks.size()){
                    temp.add(storedchildlinks.elementAt(tempcounter));
                    tempcounter ++ ;
                }

                //count how many pages is done
                System.out.println("This is page " + ++counter);

                //get title of the url
                Vector<String> pagetitle = crawler.extractTitle();
                System.out.println("Pagetitle: " + pagetitle);

                //print out url and last modification date
                System.out.println("URL: " + url);
                System.out.println("Last modification date: " + Lastmodificationdate);

                //print out the size of page
                int sizeofpage = crawler.getPageSize();
                System.out.println("Size of page :"+ sizeofpage);

                //print out all the stored words
                Vector<String> storedwords = crawler.extractWords();
                System.out.println("word: " + storedwords);

                //print out all the stored Child links
                System.out.println("Child links: "+ storedchildlinks);
                System.out.println("-----------------------------------------------------------------------------------------------------");

                //convert title vector to String
                StringBuilder builder = new StringBuilder();
                String prefix = "";
                int tempcounter0 = 0;
                while (tempcounter0 < pagetitle.size()){
                    String pagetitletemp = pagetitle.get(tempcounter0);
                    builder.append(prefix);
                    prefix = " ";
                    builder.append(pagetitletemp);
                    tempcounter0 ++ ;
                }
                String titleStr = builder.toString();

                //indexer part
                //insert Words and title into indexer
                indexer.insertWords(storedwords);
                indexer.insertTitle(pagetitle);

                //insert Page Property with title, url, last modification date and size of page into indexer
                indexer.insertPageProperty(titleStr, url, Lastmodificationdate, sizeofpage);

                //insert childUrl into indexer
                int tempcounter1 = 0;
                while (tempcounter1 < storedchildlinks.size()){
                    String childUrl ;
                    childUrl = storedchildlinks.elementAt(tempcounter1);
                    indexer.insertChildPage(childUrl);
                    tempcounter1 ++ ;
                }

                //add url into finishedlist if it does not contain a url
                if(!Finishedlist.contains(url))
                {
                    Finishedlist.add(url);
                }
            }

            //call indexer finalize function
            indexer.finalize();

            //recursion until temp equal null, if exception happen, do the next url
            if (temp.peek() != null) {
                try {
                    grab(temp.peek());
                } catch (Exception e) {
                    temp.remove();
                    grab(temp.peek());
                }
            }
        }
    }

    public static void main(String[] args) {

        final long startTime = System.currentTimeMillis();

        try {
            System.out.println("Spider Start");
            grab("http://www.cse.ust.hk/");
        } catch (IOException e) {
            System.out.println("IOException, Please restart the program");
        } catch (ParserException e) {
            System.out.println("ParserException, Please restart the program");
        } finally {
            System.out.printf("PROGRAM RUN FOR %s s\n", (System.currentTimeMillis() - startTime) / 1000d);
            System.out.println("Spider End");
        }
    }
}


