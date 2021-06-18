package com.target.generalize.operators;

import com.target.generalize.pojo.FilterPojo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.*;


public class selectTransform implements MapFunction<Tuple2<String, String>, Tuple2<String,String >>, Serializable {

    private List<String> selectFields ;
    Map<String,List<String>>  mapFields = new HashMap<>();

    public selectTransform(Collection<FilterPojo> inputargs) {
        Iterator<FilterPojo> iterator = inputargs.iterator();
            this.selectFields=iterator.next().getSelectfielda();
        ArrangeHierarchyLevel(this.selectFields);


        }

    @Override
    public Tuple2<String, String> map(Tuple2<String, String> stringStringTuple2) throws Exception {
        JSONObject json = new JSONObject(stringStringTuple2.f0);
        JSONObject jsonObject = new JSONObject();
        String value1=" ";
        for (Map.Entry<String,List<String>> entry : mapFields.entrySet()) {

            for (int j = 0; j < entry.getValue().size(); j++) {

                if (j == entry.getValue().size() - 1) {
                    value1 = json.getString(entry.getValue().get(j));

                } else {

                    json = json.getJSONObject(entry.getValue().get(j));
                }

            }

            jsonObject.put(entry.getKey(), value1);


        }


            return new Tuple2<>(jsonObject.toString()," ");




        }






    public  Map<String,List<String>> ArrangeHierarchyLevel(List<String> fieldName){


        for (int i =0 ; i < selectFields.size(); i++) {

            List<String> level = new ArrayList<String>();
            String[] a = selectFields.get(i).split(".");
            if (a.length == 0) {
                level.add(selectFields.get(i));

            } else {
                for (int j = 0; j < a.length; j++) {
                    level.add(a[j]);

                }

            }
            mapFields.put(selectFields.get(i),level);

        }

        return mapFields;
    }
}
