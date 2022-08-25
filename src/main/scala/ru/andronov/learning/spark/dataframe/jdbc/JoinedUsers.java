package ru.andronov.learning.spark.dataframe.jdbc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JoinedUsers {

    private int nId;
    private String nFirstName;
    private String nLastName;
    private String nCountry;
    private int oId;
    private String oFirstName;
    private String oLastName;
    private String oCountry;
    private String operation;

}
