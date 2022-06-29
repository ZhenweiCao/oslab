package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public class FPGrowthTest {
    public FPGrowthTest() {
    }

    public ArrayList<ArrayList<Integer>> trans;  // a list of transaction
    public ArrayList<Integer> sortedItems;
    public Map<Integer, Integer> map;  // key: itemId, value: count

    public Map<Integer, Node> headTable;
    public Node root;
    public int maxLevel;


     public int originTransNum = 0;
     public long minNum;
//    public double minSupp = 0.092;

    public static void main(String[] args) {
        FPGrowthTest fpGrowth = new FPGrowthTest();
        try {
            String host = "hdfs://Master:9000/";
            //String inputFileUri = "/user/oslab/data/TestCase/test_2W.txt";
            String inputFileUri = "/user/test1";
            double minSupp = 0.2;
            fpGrowth.readFile(host, inputFileUri);
            fpGrowth.getHeadTable(minSupp);
            fpGrowth.getFPTree();
            fpGrowth.getFreq();
        } catch (Exception e) {
            System.out.println("catch an exception" + e);
            return;
        } finally {
            System.out.println("this is finally");
        }

    }

    class Node{
        public Node(Integer val, Integer id){
            value = val;
            itemId = id;
            parentNode = null;
            childNodes = new ArrayList<Node>();
            itemNode = null;
        }
        public Node getChildNode(Integer id) {
            for (Node childNode : childNodes) {
                if (childNode.itemId == id) {
                    return childNode;
                }
            }
            return null;
        }
        public boolean deleteChildNode(Integer id) {
            for (Node childNode: childNodes){
                if (childNode.itemId == id){
                    childNode.parentNode = null;
                    childNodes.remove(childNode);
                    return true;
                }
            }
            return false;
        }
        public int value;
        public int itemId;
        public Node parentNode;  // pointer to its parent for node in fp-tree
        public List<Node> childNodes ;  // pointer to its child
        public Node itemNode;  // pointer to next same item in fp-tree
    }
public void readFile(String host, String inputFileUri) throws Exception{
        // get origin map and transaction
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(URI.create(host), config);
        FSDataInputStream is = fs.open(new Path(inputFileUri));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        map = new HashMap<Integer, Integer>();
        // transactions file, every line represent a record
        String lineText = null;
        trans = new ArrayList<>();
        while ((lineText = bufferedReader.readLine()) != null) {
            originTransNum += 1;
            String[] items = lineText.split(" ");
            ArrayList<Integer> transaction = new ArrayList<>();
            for (String item : items) {
                Integer itemId = Integer.parseInt(item);
                transaction.add(itemId);
                if (map.containsKey(itemId)) {
                    map.put(itemId, map.get(itemId) + 1);
                } else {
                    map.put(itemId, 1);
                }
            }
            trans.add(transaction);
        }
    }

    public void getHeadTable(double minSupp){
        minNum = Math.round(originTransNum * minSupp);
//        Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
        Iterator<ArrayList<Integer>> transIterator = trans.iterator();
        maxLevel = 0;
        while(transIterator.hasNext()){
            // delete item does not satisfy min support threshold
            ArrayList<Integer> transaction = transIterator.next();
            Iterator<Integer> iterator2 = transaction.iterator();
            while(iterator2.hasNext()){
                Integer itemId= iterator2.next();
                if(!map.containsKey(itemId)){
                    iterator2.remove();
                    continue;
                }
                if(map.get(itemId) < minNum){
                    iterator2.remove();
                    map.remove(itemId);
                }
            }
            if (transaction.size() > maxLevel){
                maxLevel = transaction.size();
            }
            if(transaction.isEmpty()){
                transIterator.remove();
            }
        }
        for(ArrayList<Integer> transaction: trans){
            // sort every transaction by occurrence desc
            transaction.sort((o1, o2)-> map.get(o2) - map.get(o1));
        }

        sortedItems = new ArrayList<>(map.keySet());
        sortedItems .sort((o1, o2) -> map.get(o2) - map.get(o1));  // order by occurrence times desc
//        list.forEach(System.out::println);
        headTable = new HashMap<>(map.size());
        for(Integer key: map.keySet()){
            headTable.put(key, new Node(-1, -1));
        }
        System.out.println("end");
    }

    public void getFPTree(){
        root = new Node(-1, -1);
        for(ArrayList<Integer> transaction: trans){
            Node parent = root;
            int i = 0;
            boolean newTree = false;
            while(i < transaction.size()){
                Integer itemId = transaction.get(i);
                Node tmpNode = parent.getChildNode(itemId);
                if(!newTree && tmpNode != null){  // node i already exist in level i
                    tmpNode.value += 1;
                    parent = tmpNode;
                    i++;
                    continue;
                } else {
                    newTree = true;
                }
                Node newNode = new Node(1, itemId);
                parent.childNodes.add(newNode);
                newNode.parentNode = parent;
                if (headTable.get(itemId).parentNode == null){
                    headTable.get(itemId).parentNode = newNode;
                    headTable.get(itemId).itemNode = newNode;
                } else{
                    headTable.get(itemId).parentNode.itemNode = newNode;
                    headTable.get(itemId).parentNode = newNode;
                }
                parent = newNode;
                i++;
            }
        }
        printTree(root);
        System.out.println("end of tree");
    }


    public void getFreq(){
        HashMap<Integer, ArrayList<Map.Entry<Integer, Integer>>> freqMap = new HashMap<>();
        // conditional base, for top item, is empty
        for(int i = sortedItems.size() - 1; i >= 1; i--){
            Integer itemId = sortedItems.get(i);
            HashMap<Integer, Integer> tmpMap = new HashMap<Integer, Integer>();
            Node itemNode = headTable.get(itemId).itemNode;
            while(itemNode != null){  // 找到以itemId为叶子节点的树
                Node parent = itemNode;  // include leaf node
                // Node parent = itemNode.parentNode;  // does not include leaf node
                while(parent != root){  // 直到根节点
                    if(tmpMap.containsKey(parent.itemId)){
                        tmpMap.put(parent.itemId, tmpMap.get(parent.itemId) + itemNode.value);
                    } else{
                        tmpMap.put(parent.itemId, itemNode.value);
                    }
                    parent = parent.parentNode;
                }
                itemNode = itemNode.itemNode;
            }
            Iterator<Map.Entry<Integer, Integer>> iterator = tmpMap.entrySet().iterator(); 
            while(iterator.hasNext()){
                // delete item does not satisfy min_supp
                Map.Entry<Integer, Integer> entry = iterator.next();
                if(entry.getValue() < minNum){
                    iterator.remove();
                }
            }
            freqMap.put(itemId, new ArrayList<>(tmpMap.entrySet()));
            System.out.println(tmpMap);
        }
        System.out.println(freqMap);
    }

    public void printTree(Node root){
        if(root.childNodes.isEmpty()){
            System.out.print(" " + root.value + " ");
        }else {
            for (Node pointer : root.childNodes) {
                printTree(pointer);
            }
            System.out.print(" " + root.value + " ");
        }
    }

}


