package StruggleSearchEngine;

import java.io.*;
import java.io.BufferedReader;

public class StopStem
{
	private Porter porter;
	private java.util.HashSet stopWords;
	public boolean isStopWord(String str)
	{
		return stopWords.contains(str);
	}
	public StopStem(String str)
	{
		super();
		porter = new Porter();
		stopWords = new java.util.HashSet();

		try {
			InputStreamReader reader =  new InputStreamReader(this.getClass().getResourceAsStream(str));
			BufferedReader br = new BufferedReader(reader);

			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
//				System.out.println(sCurrentLine);
				stopWords.add(sCurrentLine);
			}

			reader.close();
		} catch (Exception e){
			System.err.println("ERROR: " + e.getMessage());
		}

	}
	public String stem(String str)
	{
		return porter.stripAffixes(str);
	}
}
