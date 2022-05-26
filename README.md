# Introduction
Lab of advanced operating system.

# Requirement
- JDK 1.8
- hadoop file system

# Build && Run
1. start hdfs
    ```
   start-dfs.sh
   ```
2. start yarn
    ```
   start-yarn.sh
   ```
3. build
   ```
    mvn clean && mvn package   
   ```
4. run
    ```
   java -jar target/oslab-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```
