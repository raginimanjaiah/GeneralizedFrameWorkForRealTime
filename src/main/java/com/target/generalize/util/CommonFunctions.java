package com.target.generalize.util;




import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import java.util.List;


public  class  CommonFunctions implements Serializable, Cloneable {



    private static final long serialVersionUID = -1723722950731109198L;


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






    public Boolean compareResultWithEqual( String value,String value1 ){
        if(value1.equals(value)){
            return   true;
        }
        else return false ;
    }

    public Boolean compareResultWithStart(String  i,String value1){

        if(i.startsWith(value1)){
            return true;
        }
        else {

            return false;
        }
    }

    public Boolean compareResultWithEqualLength(String  i,String value1){

        if(i.length() == Integer.parseInt(value1)){
            System.out.println("entered into method");

            return true;
        }
        else {

            return false;
        }
    }


    public Boolean runFunction(Object obj, Method method ,String[] parameters) throws InvocationTargetException, IllegalAccessException {

        if((Boolean) method.invoke(obj, parameters)){
            return  true;
        }
        else return false;

    }




}
