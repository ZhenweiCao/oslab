# Introduction
Lab of advanced operating system.

# Requirement
- JDK 1.8
- hadoop file system

# Build && Run

函数入口在pom.xml的mainClass中，输入包名.类名即可，示例：org.example.HDFSTest, org.test.Main

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
