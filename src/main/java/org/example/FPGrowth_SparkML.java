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

// $example on$
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import scala.collection.Seq;
import scala.collection.immutable.Set;
import scala.collection.mutable.WrappedArray;


import static jodd.datetime.JDateTimeDefault.format;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import org.apache.spark.sql.types.DataTypes;

import static jodd.datetime.JDateTimeDefault.format;
// $example off$

/**
 * An example demonstrating FPGrowth.
 * Run with
 * <pre>
 * bin/run-example ml.JavaFPGrowthExample
 * </pre>
 */
public class FPGrowth_SparkML {
    private static List<Row> rules;
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("FPGrowth_SparkML")
                .master("local")
                .getOrCreate();

        String host = "hdfs://Master:9000";
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(URI.create(host), config);

        // $example on$
        String uri = "/user/test1";
        Dataset<Row> transDF = readFile(spark, fs, uri);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.2)
                .setMinConfidence(0)
                .setNumPartitions(1)
                .fit(transDF);

        // Display frequent itemsets.
        System.out.println("this is count");
        System.out.println(model.freqItemsets().count());
        Dataset<Row> dataset = model.freqItemsets().orderBy("items");
        //dataset.orderBy("freq").show(43);
        dataset.show(43);

        System.out.println("this is to string");

        // Display generated association rules.
        model.associationRules().createOrReplaceGlobalTempView("databss");
        rules = spark.sql("select antecedent, consequent, confidence from global_temp.databss order by confidence desc")
                .collectAsList();
        model.associationRules().show(43);
//
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        uri = "/user/test1";
        Dataset<Row> testDF = readFile(spark, fs, uri, 200);
//        model.transform(testDF).show();

        spark.udf().register("predictUDF", predictUDF, DataTypes.StringType);
        testDF = testDF.withColumn("predict", callUDF("predictUDF", testDF.col("items")));

        testDF.show(false);
        testDF.printSchema();

        spark.stop();
    }

    private static UDF1 predictUDF = new UDF1<Seq, String>() {
        public String call(Seq item) throws Exception {
            String result = "";
            Set itemset = item.toSet();
            System.out.println("=====");
            System.out.println(itemset);

            for (Row rule: rules) {
                Boolean flag = true;
                List<String> A = rule.getList(0);
                for (String A_item: A) {
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
                        System.out.println("Rule matched!");
                        System.out.println(rule.getList(0).toString() + "=>" + rule.getList(1).toString() +
                                " Conf: " + rule.getDouble(2));
                        break;
                    }
                }
            }
            return result;
        }
    };

    public static Dataset<Row> readFile(SparkSession spark, FileSystem fs, String uri, int... threshold) throws Exception {
        FSDataInputStream is = fs.open(new Path(uri));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        String lineText;
        List<Row> data = new ArrayList<>();
        int index = 0;
        int threshold1 = threshold.length > 0 ? threshold[0] : Integer.MAX_VALUE;
        while ((lineText = bufferedReader.readLine()) != null) {
            data.add(index, RowFactory.create(Arrays.asList(lineText.split(" "))));
            index += 1;
            if (index >= threshold1)
                break;
        }

        StructType schema = new StructType(new StructField[]{ new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        return spark.createDataFrame(data, schema);
    }
}
