package com.target.generalize.pojo;

import java.util.List;

public class SelectPojo {

    private List<String> selectfielda;

    public SelectPojo(List<String> field) {
        this.selectfielda = field;

    }

    public List<String> getSelectfielda() {
        return selectfielda;
    }

    public void setSelectfielda(List<String> selectfielda) {
        this.selectfielda = selectfielda;
    }


}
