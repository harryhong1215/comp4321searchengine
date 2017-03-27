<%--
  Created by IntelliJ IDEA.
  User: opw
  Date: 27/3/2016
  Time: 12:35
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%--<%@ page language="java" contentType="application/json; charset=UTF-8" pageEncoding="UTF-8"%>--%>
<%@ page import="SE.*" %>
<%@ page import="jdbm.RecordManager" %>
<%@ page import="jdbm.RecordManagerFactory" %>
<%@ page import="java.util.*" %>
<%@ page import="java.util.regex.Matcher" %>
<%@ page import="java.util.regex.Pattern" %>

<html>

<head>
    <title>Result</title>
</head>
<!-- Compiled and minified CSS -->
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.6/css/materialize.min.css">
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
<!-- Compiled and minified JavaScript -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.6/js/materialize.min.js"></script>

<link rel="stylesheet" href="style.css">

<body>
<div>
    <a href="javascript:history.go(-1)" class="waves-effect waves-light btn back-btn">Back to search page</a>

</div>
<container class="con-pad">
<%
    // Handle invalid input
    if(request.getParameter("q") == null || request.getParameter("q") == "") {
        out.println("ERROR: NO INPUT");
        return;
    }

    // ------------------
    // Query Extraction
    // ------------------
    String rawQuery = request.getParameter("q");
    out.println("Original Query: "+ rawQuery);

    Vector<String> normalQuery = new Vector<String>();  // for storing normal query, e.g. Happy Boy
    Vector<Vector<String>> extactQuery = new Vector<Vector<String>>();  // 2d vector for extact phase search, e.g. "Good Day"

    Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(rawQuery);
    while (m.find()) {
        String stringMatched = m.group(1);
        if( stringMatched.contains("\"")) {
            // for double quote phase
            String replaced = stringMatched.replace("\"", ""); // remove double quote
            String[] splited = replaced.split("\\s+");

            Vector<String> v = new Vector<String>();
            for( String s : splited)
                v.add(s);

            extactQuery.add(v);
        }
        else{
            normalQuery.add(stringMatched);
        }
    }

    out.println(" |Simple Search: " + normalQuery);
    out.println(" |Phase Search: " + extactQuery);

    out.println("<hr>");


    SearchEngine se = new SearchEngine();
    Map<Integer, Double> resultMap = new HashMap<Integer, Double>();
    Map<Integer, Double> normalResult = new HashMap<Integer, Double>();
    Map<Integer, Double> extactResult = new HashMap<Integer, Double>();

    if (!normalQuery.isEmpty())
        normalResult  = se.search(normalQuery);
    if (!extactQuery.isEmpty()) {
        // for each "Search terms"
        for (Vector<String> v : extactQuery) {
            Map<Integer, Double> tmp = new HashMap<Integer, Double>();
            tmp = se.phaseSearch(v);
            // intersect the results, "computer science" && "jump mushroom"
            if(extactResult.isEmpty())
                extactResult = tmp;
            else
                extactResult = se.intersectSum(extactResult, tmp);
        }
    }

    if(extactResult == null|| extactResult.isEmpty())
        resultMap = normalResult;   // normal search
    else if (normalResult == null || normalResult.isEmpty())
        resultMap = extactResult;   // phrase search
    else {
        // normal search + phrase search, all the result must contain the extact phrase, i.e. "KEYWORD"
        resultMap = se.intersectSum(normalResult, extactResult);
    }


    if (resultMap == null || resultMap.isEmpty()) {
        out.println("<p class='con-pad'> Sorry, no matched result :( (Please try another query) </p>");
        return;
    }

    final int MAX_NUM_PAGES_RETURN = 50;
    int i = 1;

    RecordManager recman = RecordManagerFactory.createRecordManager(SearchEngine.DB_PATH);
    PageProperty ppt = new PageProperty(recman, "pagePropertyIndex");
    ForwardIndex forwardIndex = new ForwardIndex(recman, "forwardIndex");
    ParentChildIndex parentChildIndex = new ParentChildIndex(recman, "parentChildIndex");
    MappingIndex urlIndex = new MappingIndex(recman, "urlMappingIndex");

    // loop result map
    for (Map.Entry<Integer, Double> entry : resultMap.entrySet()) {
        if( i > 50 )    break;  // get the top 50 results
        int pageID = entry.getKey();
        double score = entry.getValue();
        out.println(i + ") ");
        out.println("<strong>" + ppt.getTitle(pageID) + "</strong>");
        out.println("<br>");

        out.println("<a href=\"" + ppt.getUrl(pageID) + "\">" + ppt.getUrl(pageID) + "</a>");
        out.println("<pre>" + "score:" + score);
        out.println("Date: " + ppt.getModDate(pageID));
        out.println("Size: " + ppt.getPageSize(pageID));
        out.println(forwardIndex.getPageTermFrequencyString(pageID));

        // print child pages
        Vector<Integer> child = parentChildIndex.getList(pageID);
        for( int pid : child) {
            out.println( urlIndex.getKey(pid));
        }
        out.println("</pre> <br>");
        ++i;
    }



%>
</container>
</body>
</html>
