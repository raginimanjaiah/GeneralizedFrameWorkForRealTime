package com.target.generalize.operators;

import com.target.generalize.pojo.FilterPojo;
import com.target.generalize.util.CommonFunctions;
import org.apache.activemq.transport.nio.SelectorSelection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.expressions.In;
import org.json.JSONObject;
import sun.management.MethodInfo;

import java.io.*;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.security.PublicKey;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class MapTransform  implements MapFunction<Tuple2<String, String>, Tuple2<String, String>>, Serializable {

    private FilterPojo filterObjects;
    public String fieldName;
    public String operation;
    public String value;
    public List<String> countOfHierarchy;
    public String flag;
    public FilterPojo f;
    public Method method ;
    public MethodInfo methodInfo;
    public  CommonFunctions cf;
    private static final long serialVersionUID = -1L;
    private Integer startOff;


        public MapTransform(Collection<FilterPojo> inputargs,Integer startOff )  {
            super();
            cf = new CommonFunctions();
        Iterator<FilterPojo> iterator = inputargs.iterator();
        while (iterator.hasNext()) {

            f=iterator.next();
            this.fieldName = f.getField();
            this.operation =f.getOperation();
            this.value = f.getValue();
            this.flag = f.getFlag();
             //method= typeOfOperator(this.operation);
            this.method=f.getMethod1();

            //this.cf=f.getCf();

            this.countOfHierarchy = ArrangeHierarchyLevel(this.fieldName);

            this.startOff=startOff;

        }

    }




    @Override
    public Tuple2<String, String> map(Tuple2<String, String> stringStringTuple2) throws Exception {

        String value1 = " ";
        String condition ="";

        JSONObject json = new JSONObject(stringStringTuple2.f0);


        for (int j = 0; j < countOfHierarchy.size(); j++) {

            if (j == countOfHierarchy.size() - 1) {
                value1 = json.getString(countOfHierarchy.get(j));

            } else {

                json = json.getJSONObject(countOfHierarchy.get(j));
            }


        }


        System.out.println("the value of method:"+ this.method);

      if (this.flag.equals("OR")){
            if(stringStringTuple2.f1.equals("1"))
               condition="1";
            else if ((Boolean) this.method.invoke(this.cf,value1,this.value)) {
                condition = "1";
            }
            else  condition="0";
            }




        if (this.flag.equals("AND")){
            if (this.startOff==1 && (Boolean) this.method.invoke(this.cf,value1,this.value))
                condition="1";
            else if(stringStringTuple2.f1.equals("1") && (Boolean)  this.method.invoke(this.cf,value1,this.value))
                condition="1";
            else  condition="0";
        }

     /*   if (this.flag.equals("OR")){
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

         return new Tuple2<>(stringStringTuple2.f0,condition);


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





    public Function<String,Boolean > typeOfOperator(String operation) {


        Function<String, Boolean> method = null;
        if (this.operation.equals("equal")) {

           method =  i -> i.equals(value);

        }

        if (this.operation.equals("startWith")) {

            method =   i -> i.startsWith(value);

        }

        return method;
    }


    public String compareResultWithEqual(String value ){

      if(this.value.equals(value)){
          System.out.println("entered into method");
        return   "true";
      }
      else {
          System.out.println("entered into method false");
          return "false" ;}
    }

    private String  compareResultWithStartWith(String  i) {


        if(i.startsWith(this.value)){


            return "true";
        }
        else {

            return "false";
        }
    }



}
