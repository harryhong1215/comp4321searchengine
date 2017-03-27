# COMP4321 Project #

## Project Setup ##

1. Install IntelliJ IDEA in https://www.jetbrains.com/idea/ 

2. Select open project: comp4321-project

## Run Spider ##

1. Open Spider.java in comp4321-projetc

2. Right click and run Spider.java 

You can find the log in the console output

## Phase 1 Test Program##

1. To retrive the result from the db, open Phase1Test.java and right click "run"

The result will output to spider_result.txt 


## Database Design and report ##
Please visit /docs 

## JDBM ##
Please visit /data

## Phase 1 Workload Distribution ##
O Pui Wai, 20198827, 40%
CHAN Wing Yan Vannesa, 20212130, 30%
TSUI Ka Wai, 20197524, 30%

## Phase 2 ##

### Prepare db environment ###
In SearchEngine.java, change the path to the where your db file will locate, e.g. /home/ubuntu/comp4321/database
```
#!java
// TODO: Change DB_PARAM to your own path
final String DB_PATH = "/home/ubuntu/comp4321/database"; // change this the path where database.db located

```

### Deploy WAR File ###
Upload the WAR file using tomcat server admin panel
![TDc9CsE.png](https://bitbucket.org/repo/j87rBy/images/1654489622-TDc9CsE.png)

### Upload the database.db ###
1. Copy /data/database.db in project to the server, e.g. /home/ubuntu
2. Change the file permission to public read write execute 777