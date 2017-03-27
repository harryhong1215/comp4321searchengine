package SE;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by opw on 1/5/2016.
 */
public class SearchEngine {
    // final String DB_PATH = "data/database";
    // TODO: Change DB_PARAM to your own path
//    static final public String DB_PATH = "/home/ubuntu/comp4321/database"; // my remote server
    static final public String DB_PATH = "C:\\Users\\opw\\Documents\\comp4321-project\\data\\database"; // change this the path where database.db located
    static final int TOTAL_NUM_PAGES = 300;    // the total number of page
    static final double TITLE_BONUS_WEIGHT = 1.0;  // the title bonus weight here
    static StopStem stopStem = new StopStem("stopwords.txt");

    // ===============================================================
    // Simple Search (Union the query result), e.g. Copmuter Science
    // ===============================================================
    public static Map<Integer, Double> search(Vector<String> inputQuery) throws IOException{
        // TODO: comment out the sys out to improve the search performance
        RecordManager recman = RecordManagerFactory.createRecordManager(DB_PATH);
        ForwardIndex forwardIndex = new ForwardIndex(recman, "forwardIndex");
        MappingIndex wordIndex = new MappingIndex(recman, "wordMappingIndex");
        InvertedIndex bodyInvertedIndex = new InvertedIndex(recman, "bodyInvertedIndex");
        InvertedIndex titleInvertedIndex = new InvertedIndex(recman, "titleInvertedIndex");

        Vector<String> query = new Vector<String>();
        // process the input Query
        for (String w : inputQuery) {
            // stop word removal
            if (stopStem.isStopWord(w)) {
                System.out.println("Stop word: " + w);
                continue;
            }
            // stemming
            String stem = stopStem.stem(w);
            query.add(stem);
        }
        System.out.println(query);


        // [PAGEID -> value]
        Map<Integer, Double> sumWeightMap = new HashMap<Integer, Double>();
        Map<Integer, Double> scoreMap = new HashMap<Integer, Double>();

        // normal search, union terms : e.g. Computer Games,
        // for each query term
        for (String q : query) {
            System.out.println("Calculating " + q);
            int wordID = wordIndex.getValue(q);

            //--------------
            // TITLE SEARCH
            //--------------
            // if query tem is found in title inverted index, add bonus weight

            if (titleInvertedIndex.get(wordID) != null) {
                HashMap<Integer, Posting> map = titleInvertedIndex.get(wordID);  // hashmap <wordID, Posting>
                for (Map.Entry<Integer, Posting> entry : map.entrySet()) {
                    int pageID = entry.getKey();
                    Posting posting = entry.getValue();

                    Double sumWeight = sumWeightMap.get(pageID);
                    sumWeight = (sumWeight == null) ? 0 : sumWeight;    // if not exist, initialize with 0
                    sumWeight += TITLE_BONUS_WEIGHT;
                    sumWeightMap.put(pageID, sumWeight); // store the new value to map

//                        System.out.printf("Title bonus: %s sum_weight: %f\n", q, sumWeight);
                }
            }

            //--------------
            // BODY SEARCH
            //--------------
            // if query term is found in body inverted index
            if (bodyInvertedIndex.get(wordID) != null) {
                HashMap<Integer, Posting> map = bodyInvertedIndex.get(wordID);  // hashmap <wordID, Posting>

                // loop the map entries
                // for each pageID
                for (Map.Entry<Integer, Posting> entry : map.entrySet()) {
                    int pageID = entry.getKey();
                    Posting posting = entry.getValue();

                    // calculate the weight
                    double tf = (double) posting.getTermFrequency();
                    double max_tf = (double) forwardIndex.getMaxTermFrequency(pageID);
                    double df = (double) bodyInvertedIndex.getDocumentFrequency(wordID);

                    double idf = Math.log(TOTAL_NUM_PAGES / df) / Math.log(2);

                    if(max_tf == 0.0) {
                        continue;
                    }
                    double weight = (tf / max_tf) * idf;

                    // sum of weight
                    Double sumWeight = sumWeightMap.get(pageID);
                    sumWeight = (sumWeight == null) ? 0 : sumWeight;    // if not exist, initialize with 0
                    sumWeight += weight;
                    sumWeightMap.put(pageID, sumWeight); // store the new value to map

//                        System.out.printf("PAGE:%s tf:%s max_tf:%s df:%s idf:%s weight:%s sumWeight:%f\n", pageID, tf, max_tf, df, idf, weight, sumWeight);
                }
//                    System.out.println(map.toString());
            }
        }

        // compute the score of each matched pages
        for (Map.Entry<Integer, Double> entry : sumWeightMap.entrySet()) {
            int pageID = entry.getKey();
            double sumWeight = entry.getValue();

            int numWord = forwardIndex.getPageSize(pageID);
            double documentLength = Math.sqrt(numWord);

            double queryLength = Math.sqrt(query.size());
            double dotProduct = sumWeight;  // for binary vector, sum of weight = dot product

//                    System.out.println("sumwe" + dotProduct + " " + numWord + " " + documentLength);

            double score = dotProduct / (documentLength * queryLength);
//                System.out.printf("Page:%d socre:%f\n", pageID, score);
            scoreMap.put(pageID, score);    // put inside a score map
        }


//            System.out.println("----------sorted");
        Map<Integer, Double> sortedScoreMap = sortByValue(scoreMap);    // sort the result according desc. by the score

        // Debug Use, do not delete
        /*
        for (Map.Entry<Integer, Double> entry : sortedScoreMap.entrySet()) {
            int pageID = entry.getKey();
            double score = entry.getValue();
            System.out.printf("pageID:%d score:%f\n", pageID, score);
        }
        */
        return sortedScoreMap;

    }

    // ====================================================================
    // Phase Search, terms must be exactly match,  e.g. "Computer Science"
    // ====================================================================
    public static Map<Integer, Double> phaseSearch(Vector<String> inputQuery) throws IOException{
        RecordManager recman = RecordManagerFactory.createRecordManager(DB_PATH);
        ForwardIndex forwardIndex = new ForwardIndex(recman, "forwardIndex");
        MappingIndex wordIndex = new MappingIndex(recman, "wordMappingIndex");
        InvertedIndex bodyInvertedIndex = new InvertedIndex(recman, "bodyInvertedIndex");
        InvertedIndex titleInvertedIndex = new InvertedIndex(recman, "titleInvertedIndex");
//        Vector<String> inputQuery = new Vector<>();
//        inputQuery.add("AKA");
//        inputQuery.add("Title ");
//        inputQuery.add("Search ");
//        inputQuery.add("Test");
//        inputQuery.add("page");

        Vector<String> query = new Vector<String>();
        // process the input Query
        for (String w : inputQuery) {
            // stop word removal
            if (stopStem.isStopWord(w)) {
                System.out.println("Stop word: " + w);
                continue;
            }
            // stemming
            String stem = stopStem.stem(w);
            query.add(stem);
        }
        System.out.println(query);

        // [PAGEID -> value]
        Map<Integer, Double> sumWeightMap = new HashMap<Integer, Double>();
        Map<Integer, Double> scoreMap = new HashMap<Integer, Double>();

        // for the first search term
        int wordID = wordIndex.getValue(query.get(0));
        if (bodyInvertedIndex.get(wordID) == null)  return null; // phase not found

        // get intersect page maps
        HashMap<Integer, Posting> firstMap = bodyInvertedIndex.get(wordID);
        HashMap<Integer, Posting> intersectMap = new HashMap<Integer, Posting>(firstMap);
//        System.out.println(query.get(0) + "(id:"+ wordID + "): " + firstMap);

        for (String q : query) {
            if (q == query.get(0)) continue;    // skip the first
            wordID = wordIndex.getValue(q);
            if (bodyInvertedIndex.get(wordID) == null)  return null; // phase not found
            HashMap<Integer, Posting> tmpMap = bodyInvertedIndex.get(wordID);
//            System.out.println(q + "(id:"+ wordID + ": " + tmpMap);
            intersectMap.keySet().retainAll(tmpMap.keySet());   // intersect the map
        }

//        System.out.println(intersectMap);


        // TODO: now got the intersect  map
        // TODO: loop each page and see the position are consecutive
        // for each pageID, wordID = id of first query
        wordID = wordIndex.getValue(query.get(0));
        Set<Integer> pagesContainExtactPhase = new HashSet<>();
        Map<Integer, Double> tfOverMaxtf = new HashMap<>();
        for (Map.Entry<Integer, Posting> entry : intersectMap.entrySet()) {
            int pageID = entry.getKey();
            Posting posting = entry.getValue();
            Vector<Integer> wordPosList = posting.getWordPosList();
            double tf = 0;
            // for each word postiion
            for (int pos : wordPosList) {
                int posIncrement = 1;   // check next word pos
                boolean found = true;   // all the term form continue sequence

                for (String q : query) {    // for each of the query terms
                    if (q == query.get(0)) continue;    // skip the first
                    wordID = wordIndex.getValue(q);
                    boolean contain = bodyInvertedIndex.containsWordPos(pageID, wordID, pos + posIncrement);
                    if (!contain) {
                        found = false;
                        break;
                    }
                    posIncrement++;
                }
                if (found) {
                    // match terms found
//                    System.out.println(query + "Found !!!!!!!!!! PageID:" + pageID);
                    // TODO: cal tf idf,
                    pagesContainExtactPhase.add(pageID);
                    ++tf;
                }
            }
            // calculate tf/max(tf) for each page
            if( tf > 0) {
                double max_tf = (double) forwardIndex.getMaxTermFrequency(pageID);
                if(max_tf <= 0.0) continue;
//                System.out.printf("page:%d tf:%f maxtf: %f\n", pageID, tf, max_tf);
                tfOverMaxtf.put(pageID, tf/max_tf); // store in the map
            }
        }
        // calculate idf
        double df = pagesContainExtactPhase.size();
        double idf = Math.log(TOTAL_NUM_PAGES / df) / Math.log(2);

//        System.out.printf("df:%f; idf:%f\n" , df, idf);
//        System.out.println(pagesContainExtactPhase);
//        System.out.println(tfOverMaxtf);

        // calculating (tf/tfmax)*idf for each matched term
        Map<Integer, Double> weightMap = new HashMap<>();
        for (Map.Entry<Integer, Double> entry : tfOverMaxtf.entrySet()) {
            int pageID = entry.getKey();
            double tfOverMaxrf = entry.getValue();
            double weight = tfOverMaxrf * idf;
            weightMap.put(pageID, weight);
//            System.out.printf("page:%d weight:%f\n" , pageID, weight);
        }

        // sort the weight
        Map<Integer, Double> sortedWeightMap = sortByValue(weightMap);
//        System.out.println(sortedWeightMap);
        return sortedWeightMap;
    }



    // sort in descending order
    public static Map sortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });

        Collections.reverse(list); // sort in descending order

        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry)it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    // intersect two map, if two with same key, sum their values, and sorted
    public static Map intersectSum(Map<Integer, Double> map1, Map<Integer, Double> map2) {
        Set<Integer> intersectKeySet = new HashSet<Integer>(map1.keySet());
        Map<Integer, Double> resultMap = new HashMap<Integer, Double>();
        intersectKeySet.retainAll(map2.keySet());
        // loop the intersection key
        for(int key : intersectKeySet) {
            double sumScore = map1.get(key) + map2.get(key);
            resultMap.put(key, sumScore);
        }
        return sortByValue(resultMap);
    }


}
