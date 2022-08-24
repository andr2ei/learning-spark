package ru.andronov.learning.spark.dataframe.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PostJava {

    final private int id;
    final private int userId;
    final private String text;

}
