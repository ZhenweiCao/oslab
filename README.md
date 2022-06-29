# Introduction
Lab of advanced operating system.

# Requirement
- JDK 1.8
- hadoop file system

# Build && Run

函数入口在pom.xml的mainClass中，输入包名.类名即可，示例：org.example.HDFSTest, org.test.Main

## 启动集群
 
0. 格式化硬盘(Namenode)  
在未设置hdfs临时目录时，机器每次重启都会删除临时目录，导致hdfs启动失败，此时需要重新格式化硬盘。
   ```
   hdfs namenode -format
   ```
1. 使用以下命令启动hdfs文件系统，注意需要将hadoop安装目录下的bin和sbin目录加入系统PATH。
    ```
   start-dfs.sh
   ```
2. 使用yarn进行资源管理，注意需要将hadoop安装目录下的bin和sbin目录加入系统PATH。
    ```
   start-yarn.sh
   ```
3. 需要将本地文件拷贝到hdfs系统中，其中src/test/fp-tree-test目录为测试单机版fp-growth算法时所用到的测试文件。
   ```
   hdfs dfs -mkdir /user
   hdfs dfs -mkdir /user/oslab
   hdfs dfs -put src/test/fp-tree-test /user/test1
   hdfs dfs -put data/TestCase/ /user/oslab/TestCase
   ```
## 编译运行
1. 使用maven对包进行管理，需要提前安装maven，并加入系统PATH变量。使用mvn clean命令清理编译文件，mvn package命令将所有依赖和资源打包进同一个jar包中。
   ```
    mvn clean && mvn package   
   ```
2. 本地运行生成的jar包
    ```
   java -jar target/oslab-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```
3. 在集群中提交任务的命令格式如下，其中arg1、arg2、arg3分别代表输入目录，输出目录，临时文件目录。可执行的jar包必须使用绝对路径。选项name表示为运行时程序名称，可以通过Spark的webui界面查看运行情况。
   ```
   spark-submit --master spark://Master:7077  --class org.example.SparkTest --name "SecondTest" --executor-memory 40G --driver-memory 40G /home/oslab/Codes/oslab/target/oslab-1.0-SNAPSHOT-jar-with-dependencies.jar arg1 arg2 arg3
   ```
   org.example.SparkTest为类名，程序读取输入目录下trans.txt作为交易信息的输入文件，user.txt作为用户信息。结果写入输出目录下pattern和result子目录，分别对应频繁模式文件和最后推荐结果文件。
   在集群内的运行命令如下（spark-submit）已加入PATH变量：
   ```
   spark-submit --master spark://Master:7077  --class org.example.SparkTest --name "SecondTest" --executor-memory 40G --driver-memory 40G /home/oslab/Codes/oslab/target/oslab-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://Master:9000/user/oslab/TestCase hdfs://Master:9000/user/oslab/TestCase hdfs://Master:9000/user/oslab/TestCase 
   ```