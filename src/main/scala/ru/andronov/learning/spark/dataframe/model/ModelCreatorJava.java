package ru.andronov.learning.spark.dataframe.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModelCreatorJava {

    public static Dataset<Row> createUserDF(SparkSession spark) {
        List<Row> users = new ArrayList<Row>();
        List<String> technologies = Stream.of("Java", "Scala", "Spark", "Spring").collect(Collectors.toList());
        Map<String, String> skills = new HashMap<>();
        skills.put("primaryLanguage", "English");
        skills.put("university", "Oxford");
        users.add(RowFactory.create(1, "John", "Doe", 18, RowFactory.create("USA", "Beverly", 10), technologies, skills));
        users.add(RowFactory.create(2, "Marine", "James", 25, RowFactory.create("GB", "Walson", 11), technologies, skills));
        users.add(RowFactory.create(3, "Marine", "James", 25, RowFactory.create("GB", "Walson", 11), Collections.emptyList(), Collections.emptyMap()));
        users.add(RowFactory.create(4, "Anthony", "Hopkins", 30, RowFactory.create("RU", "Lesnay", 7), Collections.emptyList(), Collections.emptyMap()));
        users.add(RowFactory.create(5, "Jade", "Galaskin", 31, RowFactory.create("RU", "Lesnay", 7), Collections.emptyList(), Collections.emptyMap()));
        users.add(RowFactory.create(6, "John", "Travolta", 32, RowFactory.create("RU", "Lesnay", 7), Collections.emptyList(), Collections.emptyMap()));
        StructType schema = new StructType().
                add("id", "integer")
                .add("firstName", "string")
                .add("lastName", "string")
                .add("age", "integer")
                .add("address", new StructType()
                        .add("country", "string")
                        .add("street", "string")
                        .add("houseNumber", "integer"))
                .add("technologies", "array<string>")
                .add("skills", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        return spark.createDataFrame(users, schema);
    }

    public static Dataset<Row> createPostDf(SparkSession spark) {
        List<PostJava> posts = new ArrayList<PostJava>();
        posts.add(new PostJava(1, 1, "Text 1 of user 1"));
        posts.add(new PostJava(2, 1, "Text 2 of user 1"));
        posts.add(new PostJava(3, 2, "Text 3 of user 2"));
        posts.add(new PostJava(4, 2, "Text 4 of user 2"));
        posts.add(new PostJava(5, 0, "Text 5 of anonymous user"));

        return spark.createDataFrame(posts, PostJava.class);
    }
}
