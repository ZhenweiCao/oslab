# Introduction
Lab of advanced operating system.

# Requirement
- JDK 1.8
- hadoop file system

# Build && Run

函数入口在pom.xml的mainClass中，输入包名.类名即可，示例：org.example.HDFSTest, org.test.Main

0. 格式化硬盘(Namenode)  
只有在重新格式化hdfs之后，对应的namenode才会正常启动，具体原因不明。
   ```
   hdfs namenode -format
   ```
1. start hdfs
    ```
   start-dfs.sh
   ```
2. start yarn
    ```
   start-yarn.sh
   ```
3. copy data to hdfs
   ```
   hdfs dfs -mkdir /user
   hdfs dfs -mkdir /user/oslab
   hdfs dfs -put src/test/fp-tree-test /user/test1
   hdfs dfs -put data/TestCase/ /user/oslab/TestCase
   ```
3. build
   ```
    mvn clean && mvn package   
   ```
4. run
    ```
   java -jar target/oslab-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```
