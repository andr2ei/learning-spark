package ru.andronov.learning.spark.dataframe.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserJava {
    private final int id;
    private final String firstName;
    private final String lastName;
    private final int age;
    private final AddressJava address;
}
