package ru.andronov.learning.spark.dataframe.jdbc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserTruncated {

    private int age;
    private String firstName;
    private String lastName;
    private String country;

}
