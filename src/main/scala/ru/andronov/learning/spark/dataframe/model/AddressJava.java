package ru.andronov.learning.spark.dataframe.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AddressJava {
    private final String country;
    private final String street;
    private final int numHouse;
}
