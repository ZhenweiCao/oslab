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

import javax.xml.crypto.Data;

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
        // String uri = "/user/oslab/TestCase/trans_10W.txt";
        String uri = "/user/oslab/TestCase/trans_1w.txt";
        Dataset<Row> transDF = readFile(spark, fs, uri);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.092)
                .setMinConfidence(0)
                .setNumPartitions(2)
                .fit(transDF);

        // Display frequent itemsets.
        System.out.println(model.freqItemsets().count());
      model.freqItemsets()
                .select("items")
                .write().mode("append")
                .format("text")
                .save("hdfs://Master:9000/user/oslab/tmp/pattern.txt");
	    // model.freqItemsets().rdd().saveAsTextFile("/tmp/pattern.csv");

        // Display generated association rules.
//        model.associationRules().show();
//
//        // transform examines the input items against all the association rules and summarize the
//        // consequents as prediction
//        uri = "/user/oslab/TestCase/test_2W.txt";
//        Dataset<Row> testDF = readFile(spark, fs, uri, 200);
//        model.transform(testDF).show();
        // $example off$

        spark.stop();
    }
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
