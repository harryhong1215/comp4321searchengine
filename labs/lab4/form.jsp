<%--
  Created by IntelliJ IDEA.
  User: opw
  Date: 27/3/2016
  Time: 12:35
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%--<%@ page language="java" contentType="application/json; charset=UTF-8" pageEncoding="UTF-8"%>--%>
<html>
<head>
    <title>Title</title>
</head>
<body>
<%
    if(request.getParameter("q")!=null)
    {
        String query = request.getParameter("q");
        out.println("Original Query: "+ query);
        String[] queryArr = query.split("\\s+");
        out.println("<hr>");
        for (int i = 0; i < queryArr.length; i++) {
            out.println("<br>");
            out.println(queryArr[i]);
        }
    }
    else
    {
        out.println("ERROR: NO INPUT");
    }

%>
</body>
</html>
