package com.target.generalize.pojo;

import com.target.generalize.util.CommonFunctions;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;

public class FilterPojo implements Serializable {

    private String field;
    private String operation;
    private String value;
    private String flag;
    private List<String> selectfields;
    private String bootstrap;
    private String topic;
    private String groupid;
    private int ind =0;
    private transient Method method1=null;
    private CommonFunctions cf= null;

    public FilterPojo(String field, String operation, String value,String flag) throws NoSuchMethodException, ClassNotFoundException {
        this.field = field;
        this.operation = operation;
        this.value = value;
        this.flag=flag;
        this.ind=1;
        //this.cf=new CommonFunctions();
        this.method1=typeOfOperator();
    }

    public CommonFunctions getCf() {
        return cf;
    }

    public void setCf(CommonFunctions cf) {
        this.cf = cf;
    }

    public void setInd(int ind) {
        this.ind = ind;
    }

    public Method getMethod1() {
        return method1;
    }

    public void setMethod1(Method method1) {
        this.method1 = method1;
    }

    public  Method  typeOfOperator() throws NoSuchMethodException, ClassNotFoundException {

        Method a =null;
        Method b =null;

        if (this.operation.equals("equal")) {


            a =  CommonFunctions.class.getDeclaredMethod("compareResultWithEqual", String.class,String.class);
         //a= Class.forName("CommonFunctions").getDeclaredMethod("compareResultWithEqual",Class.forName("CommonFunctions"),String.class,String.class);

        }
        else if (this.operation.equals("startWith")){

            b=  CommonFunctions.class.getDeclaredMethod("compareResultWithStart", String.class,String.class);
            //b= Class.forName("CommonFunctions").getDeclaredMethod("compareResultWithStart",Class.forName("CommonFunctions"),String.class,String.class);
        }

        if (a != null)
            return a;
        else
            return b;



    }




    public Boolean compareResultWithEqual(String value ){

        if(this.value.equals(value)){
            return   true;
        }
        else return false ;
    }

    private Boolean compareResultWithStartWith(String  i) {


        if(i.startsWith(this.value)){
            System.out.println("entered into method");

            return true;
        }
        else {

            return false;
        }
    }



    public FilterPojo(List<String> field) {
        this.selectfields = field;


    }


    public FilterPojo(String bootstrap , String topic,String groupid) {
        this.bootstrap = bootstrap;
        this.topic=topic;
        this.groupid=groupid;
        this.ind=2;


    }

    public void setSelectfielda(List<String> selectfielda) {
        this.selectfields = selectfielda;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setGroupid(String groupid) {
        this.groupid = groupid;
    }

    public String getGroupid() {
        return groupid;
    }

    public String getTopic() {
        return topic;
    }

    public String getField() {
        return field;
    }

    public List<String> getSelectfielda() {
        return selectfields;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {

        if(ind==1)
        return "{" +
                "field='" + field + '\'' +
                ", operation='" + operation + '\'' +
                ", value='" + value + '\'' +
                ", flag='" + flag + '\'' +
                ", method1='" + method1 + '\'' +
                '}';
        else if(ind==2)
            return "{" +
                    "bootstrap='" + bootstrap + '\'' +
                    ", topic='" + topic + '\'' +
                    ", groupid='" + groupid + '\'' +
                    '}';
        else return "{" +
                "selectfields='" + selectfields + '\'' +
                '}';

    }
}
