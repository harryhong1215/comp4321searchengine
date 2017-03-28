package SE;

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
    private static Vector<String>  Finishedlist = new Vector<String>();
    private static Queue<String> temp = new LinkedList();
    private static int MaximunPagenumber = 29; //index 30 pages
    private static final String Databasepath = "data/database"; // Database path

    public static int counter = 0; //  count how many pages is done;

    private String crawlerTarget_URL_String; //store the link for crawl

    Crawler(String URL) {
        crawlerTarget_URL_String = URL;
    }

    //get private url
    public String getUrl() {
        return crawlerTarget_URL_String;
    }

    //get parser
    //from internet tutorial
    public Parser getParser() throws ParserException {
        try {
            return (new Parser(crawlerTarget_URL_String));
        } catch (ParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    //unknown
    public Vector<String> extractTitle() throws ParserException {
        Parser parser = getParser();

        NodeFilter filter = new NodeClassFilter(TitleTag.class);
        NodeList nodelist = parser.parse(filter);
        String str = "";

        for (int i = 0; i < nodelist.size(); i++) {
            Node node = nodelist.elementAt(i);
            if (node instanceof TitleTag) {
                TitleTag titletag = (TitleTag) node;
                str = titletag.getTitle();
            }
        }
        String[] strsplit = str.split(" ");

        Vector<String> title = new Vector<>();
        for (int i = 0; i < strsplit.length; i++) title.add(strsplit[i]);

        return title;
    }


    //unknown
    public Date lastUpdate() throws IOException {
        // String[] urlstr = url.split("://");
        //URL inputLink = new URL("http", urlstr[1], 80, "/");

        URL u = new URL(getUrl());
        URLConnection linkConnect = u.openConnection();

        Date date = new Date(linkConnect.getLastModified());
        //SimpleDateFormat ft = new SimpleDateFormat("yyyy.MM.dd");
        if (date == null)
            date.setTime(linkConnect.getDate());
        else if (date.toString().equals("1970-01-01"))
            date.setTime(linkConnect.getDate());

        return date;
    }


    //get all the words
    //from lab2 and parser
    public Vector<String> extractWords() throws ParserException {
        //from lab2
        Vector<String> v_word = new Vector<String>();
        StringBean sb = new StringBean();
        //from html parser example
        sb.setLinks(false);
        sb.setReplaceNonBreakingSpaces(true);
        sb.setCollapse(true);
        sb.setURL(crawlerTarget_URL_String);
        //from lab2
        String s = sb.getStrings();
        s = s.replaceAll("[,:!/.%|()-+&^#@*']", "");
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreTokens())
            v_word.add(st.nextToken());
        return v_word;
    }


    //calculate the page size
    //from oracle
    public int getPageSize() throws IOException {

        URL inputLink = new URL(crawlerTarget_URL_String);
        URLConnection linkConnect = inputLink.openConnection();
        BufferedReader newIn = new BufferedReader(new InputStreamReader(linkConnect.getInputStream()));
        String inputln, temp = "";

        while ((inputln = newIn.readLine()) != null) {
            temp += inputln;
        }

        newIn.close();
        return temp.length();
    }

    //get link in page
    //from lab2
    public Vector<String> extractLinks() throws ParserException {
        //from lab2
        Vector<String> result = new Vector<String>();
        LinkBean bean = new LinkBean();
        bean.setURL(crawlerTarget_URL_String);
        URL[] urls = bean.getLinks();
        for (URL s : urls) {
            result.add(s.toString());
        }
        return result;
    }

    public static void grab(String url) throws  IOException, ParserException {

        if (Finishedlist.size() < MaximunPagenumber) {

            Indexer indexer = new Indexer(Databasepath, url);
            Crawler crawler = new Crawler(url);

            if (temp.peek() != null)
            {
                temp.remove();
            }

            //get Lastmodificationdate of the url
            java.util.Date Lastmodificationdate = crawler.lastUpdate();

            //check lastupdate of the url
            if (indexer.pageLastModDateIsUpdated(Lastmodificationdate)) {

                //crawlwer part
                Vector<String> storedchildlinks = crawler.extractLinks();
                for (int i = 0; i < storedchildlinks.size(); i++) {
                    temp.add(storedchildlinks.elementAt(i));
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

                //indexer part
                indexer.insertWords(storedwords);
                indexer.insertTitle(pagetitle);

               //convert title vector to String
                 String titleStr = pagetitle.toString();

                indexer.insertPageProperty(titleStr, url, Lastmodificationdate, sizeofpage);

                for (String childUrl : storedchildlinks) {
                    indexer.insertChildPage(childUrl);
                }

                if(!Finishedlist.contains(url))
                {
                    Finishedlist.add(url);
                }
            }

            indexer.finalize();

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
        }catch (IOException e) {
            System.out.println("IOException, Please restart the program");
        } catch (ParserException e) {
            System.out.println("ParserException, Please restart the program");
        }finally{
            System.out.printf("PROGRAM RUN FOR %s s\n", (System.currentTimeMillis() - startTime) / 1000d);
            System.out.println("Spider End");
        }

    }

}


