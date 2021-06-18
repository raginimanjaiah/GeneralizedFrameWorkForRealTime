package com.target.generalize.operators;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.target.generalize.pojo.FilterPojo;
import com.target.generalize.util.CommonFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

import javax.swing.*;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

import org.json.JSONArray;


public class FilterTransform   implements FilterFunction<Tuple2<String, String>> ,Serializable {


    public String fieldName;
    public String operation;
    public String value;
    public List<String> countOfHierarchy;
    public  Method method1=null;
    public String flag;
    public FilterPojo f;
    public  Integer startOff;
    private static final long serialVersionUID = -1723722950731109198L;


    public FilterTransform(Collection<FilterPojo> inputargs, Integer startOff){
        Iterator<FilterPojo> iterator = inputargs.iterator();

        while (iterator.hasNext()) {
            f=iterator.next();
            this.fieldName = f.getField();
            this.operation =f.getOperation();
            this.value = f.getValue();
            this.flag = f.getFlag();
          //  this.method1=f.getMethod1();
            this.startOff =startOff;



           // typeOfOperator(this.operation);
            this.countOfHierarchy = ArrangeHierarchyLevel(this.fieldName);

        }
    }


        @Override
        public boolean filter(Tuple2<String, String> stringStringTuple2) throws Exception {

            String value1 = " ";
            String condition="";
            CommonFunctions cf = new CommonFunctions();

            JSONObject json = new JSONObject(stringStringTuple2.f0);

               for (int j = 0; j < countOfHierarchy.size(); j++) {

                   if (j == countOfHierarchy.size() - 1) {
                       value1 = json.getString(countOfHierarchy.get(j));
                   } else {
                       json = json.getJSONObject(countOfHierarchy.get(j));
                   }


               }


          /*  if (this.flag.equals("OR")){

                if(stringStringTuple2.f1.equals("1"))
                    condition="1";
                else if ((Boolean ) this.method1.invoke(cf,value1,this.value))
                    condition="1";
                else  condition="0";
            }


            if (this.flag.equals("AND")){
                if (this.startOff==1 && (Boolean) this.method1.invoke(cf,value1,this.value))
                    condition="1";
                else if( stringStringTuple2.f1.equals("1") && (Boolean) this.method1.invoke(cf,value1,this.value))
                    condition="1";
                else  condition="0";
            }*/


          //

            /*if (this.flag.equals("OR")){
                if(stringStringTuple2.f1.equals("1"))
                    condition="1";
                else  if (stringStringTuple2.f1.equals("0") ){
                    if ( this.operation.equals("equal")){
                        if( cf.compareResultWithEqual(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if ( this.operation.equals("startWith"))  {

                        if( cf.compareResultWithStart(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if (this.operation.equals("length")){
                        if( cf.compareResultWithEqualLength(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                }
                else condition="0";
            }

            if (this.flag.equals("AND")){
                if(this.startOff ==1){
                    if ( this.operation.equals("equal")){
                        if( cf.compareResultWithEqual(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if ( this.operation.equals("startWith"))  {

                        if( cf.compareResultWithStart(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if (this.operation.equals("length")){
                        if( cf.compareResultWithEqualLength(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                }
                else  if(stringStringTuple2.f1.equals("1")){
                    if ( this.operation.equals("equal")){
                        if( cf.compareResultWithEqual(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if ( this.operation.equals("startWith"))  {

                        if( cf.compareResultWithStart(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                    else if (this.operation.equals("length")){
                        if( cf.compareResultWithEqualLength(value1,this.value)){
                            condition="1";
                        }
                        else { condition="0";}
                    }
                }
                else  condition="0";

            }*/




            System.out.println("The condition is :"+condition);


            if(condition.equals("1"))
                return true;
            else
                return false;




          /*  if (method.apply(value1))

                   return true;
               else
                   return false;*/



        }





        public List<String> ArrangeHierarchyLevel(String fieldName){

            List<String> level = new ArrayList<String>();
            String[] a = fieldName.split(".");
            if(a.length == 0){
                level.add(fieldName);

            }
            else {
                for (int i = 0; i < a.length; i++) {
                    level.add(a[i]);

                }

            }
            return level;
        }


       /* public void typeOfOperator(String operation)  {


        if(operation.equals("startWith")) {

            method1 = ( Function<String,Boolean> & Serializable) i -> startWithComparision(i) ;



        }
        }*/



    private Boolean startWithComparision(String  i) {

        if(i.startsWith(this.value)){

            return true;
        }
        else {

            return false;
        }
    }



}
