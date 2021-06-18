package com.target.generalize;

import com.target.generalize.kafkaBase.FullpayloadConsumer;
import com.target.generalize.kafkaBase.GeneraliseloadProducer;
import com.target.generalize.operators.FilterTransform;
import com.target.generalize.operators.MapTransform;
import com.target.generalize.operators.selectTransform;
import com.target.generalize.pojo.FilterPojo;
import com.target.generalize.util.CommonFunctions;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.json.JsonObjectDecoder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;


public class QueryBuilder implements Serializable {


    private static Object FilterTransform;
    private static Map<String,String> filterMap;
    private static DataStream<Tuple2<String, Integer>> filterDataFrame;
    private static int stitchOperator;
    public static DataStream<Tuple2<String, String>> messageStream;
    private static DataStream<Tuple2<String, String>> messageStream1;
    private static DataStream<Tuple2<String, String>> messageStream2;
    private static final long serialVersionUID = -1723722950731109198L;
    private static List<Map<String, FilterPojo>> finalFilterObjects  = new ArrayList<Map<String, FilterPojo>>();
    private static List<Map<String, String>> selectObjects  = new ArrayList<Map<String, String>>();
    private  static int count = 0;
    private static String  previousmap="";
    private static String flinkOperator="";
    private static String logicalOperatorFlag="Noflag" ;
    private static  JSONObject jsonObject;



    public static void main(String[] args) throws Exception {


        //String str2="{ \"select\":[ {\"field\":\"experimentId\"},{\"field\":\"testid\"},{\"field\":\"orderid\"},{\"field\":\"customerid\"},{\"field\":\"event_ts\"}],\"filter\":[ {\"clause\":{\"logicalOperator\":\"OR\",\"conditionfileds\":[{\"field\":\"vistorid\",\"operation\": \"startWith\",\"value\":\"R\"}, {\"field\":\"orderid\",\"operation\": \"startWith\",\"value\":\"R\"}]},\"logicalOperatorNext\":\"OR\"},{\"clause\":{\"logicalOperator\":\"AND\",\"conditionfileds\":[{\"field\":\"customerid\",\"operation\": \"startWith\",\"value\":\"R\"}]},\"logicalOperatorNext\":\"NONE\"}]}";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         env.setRestartStrategy(RestartStrategies.failureRateRestart(
                 3,                         // max failures per interval
                 Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
                 Time.of(10, TimeUnit.SECONDS) // delay
         ));
         env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         /*ParameterTool readArgs = ParameterTool.fromArgs(args);
         String propFilePath = readArgs.get("input", "conf/generalized.json");
         ParameterTool params = ParameterTool.fromPropertiesFile(propFilePath);*/
        File file = new File("conf/generalized.json");
        String str = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        System.out.println("the value of str:"+str);

        QueryBuilder qb = new QueryBuilder();
        qb.buildQuery(str);

         qb.buildObject(finalFilterObjects,env);

        env.execute("generalized query");

     }




    public void buildQuery(String metaData) throws NoSuchMethodException, ClassNotFoundException {


        JSONObject jsonObject = new JSONObject(metaData);


        finalFilterObjects = new ArrayList<Map<String, FilterPojo>>();



        if (getArraysetVal(jsonObject.getJSONArray("source"))){
            JSONArray sourceArray = jsonObject.getJSONArray("source");
            for (int k = 0; k < sourceArray.length(); k++) {

                if ( sourceArray.getJSONObject(k).getString("inputdatasource").equals("kafka")){

                    flinkOperator = "sourcekafka-" + count;
                    System.out.println("the value is :"+sourceArray.getJSONObject(k).getJSONObject("inputs"));
                    FilterPojo p = new FilterPojo(sourceArray.getJSONObject(k).getJSONObject("inputs").getString("inputbootstrap"),sourceArray.getJSONObject(k).getJSONObject("inputs").getString("inputtopic"),sourceArray.getJSONObject(k).getJSONObject("inputs").getString("inputgroupid") );
                    HashMap<String, FilterPojo> finalFilterMap = new HashMap<>();
                    finalFilterMap.put(flinkOperator, p);
                    finalFilterObjects.add(finalFilterMap);

            }

                }
            }


            if (getArraysetVal(jsonObject.getJSONArray("filter"))) {
            JSONArray filterArray = jsonObject.getJSONArray("filter");
            for (int k = 0; k < filterArray.length(); k++) {
                JSONObject clausObject = filterArray.getJSONObject(k);
                String logicalOperatorNext = filterArray.getJSONObject(k).getString("logicalOperatorNext").toString();

                if (clausObject.getJSONObject("clause").getString("logicalOperator").equals("OR") && logicalOperatorNext.equals("OR")) {
                    JSONArray conditionfileds = clausObject.getJSONObject("clause").getJSONArray("conditionfileds");

                    for (int t = 0; t < conditionfileds.length(); t++) {
                        JSONObject clausObject1 = conditionfileds.getJSONObject(t);
                        if (t == conditionfileds.length() - 1) {
                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;
                        } else {

                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;

                        }
                    }
                }

                if (clausObject.getJSONObject("clause").getString("logicalOperator").equals("AND") && logicalOperatorNext.equals("OR")) {
                    JSONArray conditionfileds = clausObject.getJSONObject("clause").getJSONArray("conditionfileds");

                    for (int t = 0; t < conditionfileds.length(); t++) {
                        JSONObject clausObject1 = conditionfileds.getJSONObject(t);
                        if (t == conditionfileds.length() - 1) {
                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;

                        } else {

                            if (t == 0 && previousmap.equals("OR")) {
                                flinkOperator = "map-" + count;
                                logicalOperatorFlag = "OR";
                                addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                                previousmap = logicalOperatorNext;
                                count++;

                            } else {
                                flinkOperator = "map-" + count;
                                logicalOperatorFlag = "AND";
                                addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                                previousmap = logicalOperatorNext;
                                count++;
                            }
                        }

                    }
                }


                if (clausObject.getJSONObject("clause").getString("logicalOperator").equals("OR") && logicalOperatorNext.equals("AND")) {
                    JSONArray conditionfileds = clausObject.getJSONObject("clause").getJSONArray("conditionfileds");

                    for (int t = 0; t < conditionfileds.length(); t++) {
                        JSONObject clausObject1 = conditionfileds.getJSONObject(t);

                        if (t == conditionfileds.length() - 1) {
                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "AND";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;

                        } else {

                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;


                        }
                    }
                }


                if (clausObject.getJSONObject("clause").getString("logicalOperator").equals("AND") && logicalOperatorNext.equals("NONE")) {
                    JSONArray conditionfileds = clausObject.getJSONObject("clause").getJSONArray("conditionfileds");

                    for (int t = 0; t < conditionfileds.length(); t++) {
                        JSONObject clausObject1 = conditionfileds.getJSONObject(t);
                        //System.out.println("the value is :"+clausObject1);
                        if (t == conditionfileds.length() - 1) {

                            flinkOperator = "filter-" + count;
                            logicalOperatorFlag = "AND";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;
                        } else {
                            if (previousmap.equals("OR") && t == 0) {
                                flinkOperator = "map-" + count;
                                logicalOperatorFlag = "OR";
                                addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                                previousmap = logicalOperatorNext;
                                count++;
                            } else {
                                flinkOperator = "map-" + count;
                                logicalOperatorFlag = "AND";
                                addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                                previousmap = logicalOperatorNext;
                                count++;

                            }
                        }
                    }
                }


                if (clausObject.getJSONObject("clause").getString("logicalOperator").equals("OR") && logicalOperatorNext.equals("NONE")) {
                    JSONArray conditionfileds = clausObject.getJSONObject("clause").getJSONArray("conditionfileds");

                    for (int t = 0; t < conditionfileds.length(); t++) {
                        JSONObject clausObject1 = conditionfileds.getJSONObject(t);
                        //System.out.println("the value is :"+clausObject1);
                        if (t == conditionfileds.length() - 1) {

                            flinkOperator = "filter-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;
                        }
                        if (previousmap.equals("AND") && t == 0) {
                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "AND";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;
                        } else {
                            flinkOperator = "map-" + count;
                            logicalOperatorFlag = "OR";
                            addObjects(clausObject1, flinkOperator, logicalOperatorFlag);
                            previousmap = logicalOperatorNext;
                            count++;

                        }
                    }
                }



            }
        }



        if (getArraysetVal(jsonObject.getJSONArray("select"))) {
            JSONArray selectArray = jsonObject.getJSONArray("select");
            List<String> selectList = new ArrayList<>();
            HashMap<String, FilterPojo> finalFilterMap = new HashMap<>();

            for (int k = 0; k < selectArray.length(); k++) {

                JSONObject selectObject = selectArray.getJSONObject(k);
                System.out.println("the select objects are:" + selectObject.getString("field"));
                selectList.add(selectObject.getString("field"));

            }

                FilterPojo p = new FilterPojo(selectList);
                finalFilterMap.put("select-"+count,p);
                finalFilterObjects.add(finalFilterMap);
                count++;



        }



        if (getArraysetVal(jsonObject.getJSONArray("destination"))){
            JSONArray sourceArray = jsonObject.getJSONArray("destination");
            for (int k = 0; k < sourceArray.length(); k++) {

                if ( sourceArray.getJSONObject(k).getString("outputdatasource").equals("kafka")){

                    flinkOperator = "destkafka-" + count;
                    FilterPojo p = new FilterPojo(sourceArray.getJSONObject(k).getJSONObject("output").getString("outputbootstrap"),sourceArray.getJSONObject(k).getJSONObject("output").getString("outputtopic")," ") ;
                    HashMap<String, FilterPojo> finalFilterMap = new HashMap<>();
                    finalFilterMap.put(flinkOperator, p);
                    finalFilterObjects.add(finalFilterMap);

                }

            }
        }
        System.out.println("the value of operator finalFilterObjects" + finalFilterObjects);
    }




    public static void addObjects(JSONObject clausObject1,String flinkOperator,String logicalOperatorFlag) throws NoSuchMethodException, ClassNotFoundException {

        FilterPojo p = new FilterPojo(clausObject1.getString("field"), clausObject1.getString("operation"), clausObject1.getString("value"), logicalOperatorFlag);

        HashMap<String, FilterPojo> finalFilterMap = new HashMap<>();
        finalFilterMap.put(flinkOperator, p);
        finalFilterObjects.add(finalFilterMap);


    }



    private static boolean getsetVal(Object object) {
            if (object != null && !object.toString().isEmpty() && !(object.toString().contentEquals("undefined")))
                return true;
            else
                return false;
        }

        private static boolean getArraysetVal(JSONArray object) {
            if (object != null && !object.toString().isEmpty() && !(object.toString().contentEquals("undefined")))
                return true;
            else
                return false;
        }

    public void buildObject( List<Map<String, FilterPojo>>  operator,StreamExecutionEnvironment env) throws Exception {
        int i =0;
        int startOff =0;

        if(i < operator.size() ){

            Map <String, FilterPojo> map1 = operator.get(i);
            String key =map1.keySet().toString();
            String[] s = key.split("-");
            String[] t = s[0].split("\\[");

            if(t[1].equals("map"))
            {
                int b = 1;
                String[] a=s[1].split("]");
                //String[] b=s[0].split()
                if(a[0].equals("0")) {

                    startOff=1;
                }
                messageStream=messageStream.map(new MapTransform(map1.values(),startOff));


            }
           if(t[1].equals("filter"))
                {
                    int b = 1;
                    String a=s[1].split("\\}").toString();
                    if(a.equals("0")) {
                        startOff=1;
                    }
                    messageStream= messageStream.filter(new FilterTransform(map1.values(),startOff));
            }

            if(t[1].equals("select"))
            {
                messageStream= messageStream.map(new selectTransform(map1.values()));
            }

            if(t[1].equals("sourcekafka"))
            {

                FlinkKafkaConsumer010<Tuple2<String, String>> loadKafkaConsumer = FullpayloadConsumer.createStringConsumerForTopic(map1.values());
                messageStream = env.addSource(loadKafkaConsumer);
            }

            if(t[1].equals("destkafka"))
            {
                FlinkKafkaProducer010<String> loadKafkaProducer= GeneraliseloadProducer.creatkafkaProducerInstance(map1.values());
                messageStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<String> collector) throws Exception {
                        collector.collect(stringStringTuple2.f0);
                    }
                }).addSink(loadKafkaProducer);

            }



            operator.remove(0);
            buildObject(operator,env);

        }

    }


}
