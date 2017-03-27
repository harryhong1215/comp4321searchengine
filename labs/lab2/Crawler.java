/* --
COMP4321 Lab2 Exercise
Student Name:
Student ID:
Section:
Email:
*/
import java.util.Vector;
import org.htmlparser.beans.StringBean;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import java.util.StringTokenizer;
import org.htmlparser.beans.LinkBean;
import java.net.URL;


public class Crawler
{
    private String url;
    Crawler(String _url)
    {
        url = _url;
    }

    public String getUrl() {
        return url;
    }

    public Vector<String> extractWords() throws ParserException

    {
        // extract words in url and return them
        // use StringTokenizer to tokenize the result from StringBean
        // ADD YOUR CODES HERE
        StringBean sb = new StringBean();

        // StringBean settings
        sb.setLinks (false);
        sb.setReplaceNonBreakingSpaces (true);
        sb.setCollapse (true);
        sb.setURL ("http://www.cs.ust.hk/~dlee/4321/");

        String s = sb.getStrings ();

        Vector<String> v_word = new Vector<String>();
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreTokens()) {
            v_word.add(st.nextToken());
        }
        return v_word;
    }

    public Vector<String> extractLinks() throws ParserException

    {
        // extract links in url and return them
        // ADD YOUR CODES HERE
        LinkBean lb = new LinkBean();
        lb.setURL(this.url);
        URL[] URL_array = lb.getLinks();

        Vector<String> v_link = new Vector<String>();
        for(int i=0; i<URL_array.length; i++){
            v_link.add(URL_array[i].toString());
        }
        return v_link;
    }

}

