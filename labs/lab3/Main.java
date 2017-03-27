import org.htmlparser.util.ParserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;


public class Main {
    public static void main (String[] args) {

        StopStem stopStem = new StopStem("stopwords.txt");
        String input="";
        try{
            do
            {
                System.out.print("Please enter a single English word: ");
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                input = in.readLine();
                if(input.length()>0)
                {
                    if (stopStem.isStopWord(input))
                        System.out.println("It should be stopped");
                    else
                        System.out.println("The stem of it is \"" + stopStem.stem(input)+"\"");
                }
            }
            while(input.length()>0);
        }
        catch(IOException ioe)
        {
            System.err.println(ioe.toString());
        }
    }
}
