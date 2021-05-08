package com.zto.fire.examples.bean;

import java.io.Serializable;
import java.util.List;

public interface BeanFactory extends Serializable {
    List<BeanFactory> generateList();
}
