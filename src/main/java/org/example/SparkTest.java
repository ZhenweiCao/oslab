/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.net.URI;

public class SparkTest {
    public class Config{
        public String inputDir;
        public String outputDir;
        public String tmpDir;
        public FileSystem fs;
        public JavaSparkContext sc;
        public Config(String inputDir, String outputDir, String tmpDir, FileSystem fs, JavaSparkContext sc){
            this.inputDir = inputDir;
            this.outputDir = outputDir;
            this.tmpDir = tmpDir;
            this.fs = fs;
            this.sc = sc;
        }
    }

    public Config initConfig(String[] args){
        // 配置hdfs信息
        String host = "hdfs://Master:9000";
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(host), config);
        }catch (Exception e){
            System.out.println("connect to hdfs failed, err: " + e.toString());
            return null;
        }

        boolean outputDirExists = false;
        try {
            outputDirExists = fs.exists(new Path(args[1]));
            if(!outputDirExists){  // 如果输出目录不存在，则创建一个目录
                fs.mkdirs(new Path(args[1]));
            }
        } catch (Exception e){
            System.out.println("hdfs io error, err: " + e.toString());
            return null;
        }

        // 配置spark集群信息
        SparkConf sparkConf = new SparkConf();
        // 调试模式下设置Master地址，命令行向集群提交时可以不设置
        sparkConf.setMaster("spark://Master:7077");  // 这里不设置地址，可以通过命令行传入--master参数决定
        //用IDEA调试时，这里设置local时，只能在用client模式运行，在集群上跑时注释此句
        sparkConf.setMaster("local");
        sparkConf.set("spark.submit.deployMode", "cluster");  // 以集群模式运行
        sparkConf.setAppName("FirstTest");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        if(args.length >= 3){
            return new Config(args[0], args[1], args[2], fs, sc);
        } else {
            return new Config(
                    "hdfs://Master:9000/user/oslab/TestCase/trans_10W.txt",
                    "hdfs://Master:9000/user/oslab/TestCase/outputDir",
                    "hdfs://Master:9000/user/oslab/TestCase/tmpDir",
                    fs,
                    sc
            );
        }
    }

    public static void main(String[] args) throws Exception {
        SparkTest sparkTest = new SparkTest();
        Config envConfig = sparkTest.initConfig(args);
        if (envConfig == null){
            System.out.println("Init environment config error");
            return;
        }
        sparkTest.GetPattern(envConfig);
        //JavaRDD<String> data = sc.textFile("hdfs://Master:9000/user/test1", 1);

        //model. (sc, "file://home/oslab/result.txt");

        //AssociationRules arules = new AssociationRules().setMinConfidence(0.092);
        //JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);
        //results.saveAsTextFile("hdfs://202.38.72.23:9000/user/oslab/result.txt");
        //JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = sc.parallelize(Arrays.asList(data));
        System.out.println("finished");
    }
    public void GetPattern(Config config){
        JavaRDD<String> data = config.sc.textFile(config.inputDir, 10);
        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.trim().split(" ")));
//        JavaRDD<FPGrowth.FreqItemset<String>> transactions = data.map(
//                new Function<String, FPGrowth.FreqItemset<String>>(){
//                    public FPGrowth.FreqItemset<String> call (String x){
//                        return new FPGrowth.FreqItemset<String>(x.split(" "), 20L);
//                    }
//                }
//        );
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.092)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);
        //model.freqItemsets().toJavaRDD().saveAsTextFile("hdfs://Master:9000/user/test1_result");
        JavaRDD<FPGrowth.FreqItemset<String>> freqItems = model.freqItemsets().toJavaRDD();
        List<FPGrowth.FreqItemset<String>> s = freqItems.take(10);
        JavaRDD<String> sortedFreqItems = freqItems.map(
                new Function<FPGrowth.FreqItemset<String>, String>(){
                    public String call (FPGrowth.FreqItemset<String> x){
                        return "test";
                    }
                }
        );
        sortedFreqItems.saveAsTextFile(config.outputDir);
    }
}
