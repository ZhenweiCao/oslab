package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import org.apache.commons.lang3.StringUtils;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Set;

import java.net.URI;

import static org.apache.spark.sql.functions.callUDF;

public class SparkTest {
    public class Config {
        public String inputDir;
        public String outputDir;
        public String tmpDir;
        public FileSystem fs;
        public JavaSparkContext sc;
        public SparkSession ss;
        public String transFilePath;
        public String userFilePath;
        public String patternPath;
        public String resultPath;

        public Config(String inputDir, String outputDir, String tmpDir, FileSystem fs, JavaSparkContext sc,
                SparkSession ss) {
            this.inputDir = inputDir;
            this.outputDir = outputDir;
            this.tmpDir = tmpDir;
            this.fs = fs;
            this.sc = sc;
            this.ss = ss;
            // 数据文件和用户文件
            // this.transFilePath = "hdfs://Master:9000/user/test1";
            this.transFilePath = inputDir + "/trans.txt";
            this.userFilePath = inputDir + "/user.txt";
            this.patternPath = outputDir + "/pattern.txt";
            this.resultPath = outputDir + "/result.txt";
        }
    }

    private static List<Row> simpleRules;

    public Config initConfig(String[] args) {
        // 配置hdfs信息
        String host = "hdfs://Master:9000";
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(host), config);
        } catch (Exception e) {
            System.out.println("connect to hdfs failed, err: " + e.toString());
            return null;
        }

        boolean outputDirExists = false;

        // 配置spark集群信息
        SparkConf sparkConf = new SparkConf();
        // 调试模式下设置Master地址，命令行向集群提交时可以不设置
        // sparkConf.setMaster("spark://Master:7077"); // 这里不设置地址，可以通过命令行传入--master参数决定
        // 用IDEA调试时，这里设置local时，只能在用client模式运行，在集群上跑时注释此句
        // sparkConf.setMaster("local");
        sparkConf.set("spark.submit.deployMode", "cluster"); // 以集群模式运行
        sparkConf.setAppName("SparkRun");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SparkSession ss = SparkSession.builder().getOrCreate();
        Config envConfig;
        if (args.length >= 3) {
            envConfig = new Config(args[0], args[1], args[2], fs, sc, ss);
        } else {
            envConfig = new Config(
                    "hdfs://Master:9000/user/oslab/TestCase/trans_10W.txt",
                    "hdfs://Master:9000/user/oslab/TestCase/outputDir",
                    "hdfs://Master:9000/user/oslab/TestCase/tmpDir",
                    fs,
                    sc,
                    ss);
        }
        try {
            // 运行时，要求输出文件目录不存在
            if (fs.exists(new Path(envConfig.patternPath))) {
                fs.delete(new Path(envConfig.patternPath), true);
            }
            if (fs.exists(new Path(envConfig.resultPath))) {
                fs.delete(new Path(envConfig.patternPath), true);
            }
        } catch (Exception e) {
            System.out.println("hdfs io error, err: " + e.toString());
            return null;
        }
        return envConfig;
    }

    public static void main(String[] args) throws Exception {
        SparkTest sparkTest = new SparkTest();
        Config envConfig = sparkTest.initConfig(args);
        if (envConfig == null) {
            System.out.println("Init environment config error");
            return;
        }
        Dataset<Row> transDF;
        try {
            transDF = sparkTest.readFile(envConfig, envConfig.transFilePath);
        } catch (Exception e) {
            System.out.println("read transcation file failed, err: " + e.toString());
            return;
        }
        transDF.show();
        org.apache.spark.ml.fpm.FPGrowthModel model = new org.apache.spark.ml.fpm.FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.2)
                .setMinConfidence(0)
                .setNumPartitions(1)
                .fit(transDF);
        Dataset<Row> freqItems = model.freqItemsets();
        sparkTest.generatePattern(freqItems, envConfig);
        simpleRules = sparkTest.simplifyAssociationRules(envConfig, model.associationRules()).collectAsList();

        Dataset<Row> userDF;
        try {
            userDF = sparkTest.readFile(envConfig, envConfig.userFilePath);
        } catch (Exception e) {
            System.out.println("read user file failed, err: " + e.toString());
            return;
        }

        envConfig.ss.udf().register("predictUDF", predictUDF, DataTypes.StringType);
        userDF = userDF.withColumn("predict", callUDF("predictUDF", userDF.col("items")));
        // userDF.show(false);
        userDF.toJavaRDD().map(line -> (line.getString(1).trim().length() != 0) ? line.getString(1) : "0")
                .saveAsTextFile(envConfig.resultPath);
        System.out.println("finished");
    }

    private static UDF1 predictUDF = new UDF1<Seq, String>() {
        public String call(Seq item) throws Exception {
            String result = "";
            Set itemset = item.toSet();
            // System.out.println("=====");
            // System.out.println(itemset);

            for (Row rule : simpleRules) {
                Boolean flag = true;
                List<String> A = rule.getList(0);
                for (String A_item : A) {
                    if (!itemset.contains(A_item)) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    // 关联规则中前项被用户数据包含
                    String temp = rule.getList(1).toString();
                    String consequent = temp.substring(1, temp.length() - 1);
                    if (!itemset.contains(consequent)) {
                        // 关联规则后项未被用户数据包含，找到结果
                        result = consequent;
                        // System.out.println("Rule matched!");
                        // System.out.println(rule.getList(0).toString() + "=>" +
                        // rule.getList(1).toString() +
                        // " Conf: " + rule.getDouble(2));
                        break;
                    }
                }
            }
            return result;
        }
    };

    public Dataset<Row> generatePattern(Dataset<Row> freqItems, Config config) {
        // 生成频繁模式，并写入文件
        freqItems.createOrReplaceGlobalTempView("FreqItems");
        // 将频繁模式排序
        Dataset<Row> orderedDataset = config.ss.sql("select items, freq from global_temp.FreqItems order by items asc");
        // orderedDataset.show(43);

        // 写入频繁模式，可以通过设置repartition(1)，让结果输出到单个文件中
        orderedDataset.toJavaRDD().map(line -> StringUtils.join(line.getList(0), " "))
                .saveAsTextFile(config.patternPath);
        return orderedDataset;
    }

    public Dataset<Row> simplifyAssociationRules(Config config, Dataset<Row> originRules) {
        // 去掉关联规则中到重复前项
        originRules.show(false);
        Dataset<Row> s1 = originRules.groupBy("antecedent").max("confidence").withColumnRenamed("max(confidence)",
                "confidence");
        List<String> list = new ArrayList<>();
        list.add("antecedent");
        list.add("confidence");
        Dataset<Row> simpleRules = originRules
                .join(s1, JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq())
                .select(originRules.col("antecedent"), originRules.col("consequent"), originRules.col("confidence"));
        simpleRules.createOrReplaceGlobalTempView("AssociationRules");
        System.out.println("Simplified Association Rules: ");
        Dataset<Row> orderedSimpleRules = config.ss.sql(
                "select antecedent, consequent, confidence from global_temp.AssociationRules sort by confidence, consequent");
        System.out.println("rules");
        simpleRules.show();
        orderedSimpleRules.show();
        return simpleRules;
    }

    public Dataset<Row> readFile(Config config, String uri) throws Exception {
        FSDataInputStream is = config.fs.open(new Path(uri));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        String lineText;
        List<Row> data = new ArrayList<>();
        int index = 0;
        while ((lineText = bufferedReader.readLine()) != null) {
            data.add(index, RowFactory.create(Arrays.asList(lineText.split(" "))));
            index += 1;
        }

        StructType schema = new StructType(new StructField[] { new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        return config.ss.createDataFrame(data, schema);
    }

}
