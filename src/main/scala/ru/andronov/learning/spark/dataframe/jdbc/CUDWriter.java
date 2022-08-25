package ru.andronov.learning.spark.dataframe.jdbc;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CUDWriter implements Serializable {

    private final String url = "jdbc:postgresql://localhost:5432/learning-spark";
    private final String user = "postgres";
    private final String password = "postgres";

    void writeDeletes(Dataset<Row> deletes) {
        deletes.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
            try(Connection con = DriverManager.getConnection(url, user, password)) {
                while (iterator.hasNext()) {
                    try (PreparedStatement preparedStatement = con.prepareStatement("DELETE FROM public.test_2_users WHERE \"id\" = ?")) {
                        Row r = iterator.next();
                        preparedStatement.setInt(1, r.getInt(0));
                        preparedStatement.execute();
                    }
                }
            }
        });
    }

    void writeUpdates(Dataset<Row> updates) {
        updates.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
            try(Connection con = DriverManager.getConnection(url, user, password)) {
                while (iterator.hasNext()) {
                    try (PreparedStatement preparedStatement = con.prepareStatement("UPDATE public.test_2_users SET \"firstName\" = ?, \"lastName\" = ?, \"country\" = ? WHERE \"id\" = ?")) {
                        Row r = iterator.next();
                        preparedStatement.setString(1, r.getString(1));
                        preparedStatement.setString(2, r.getString(2));
                        preparedStatement.setString(3, r.getString(3));
                        preparedStatement.setInt(4, r.getInt(0));
                        preparedStatement.execute();
                    }
                }
            }
        });
    }

    void writeInserts(Dataset<Row> inserts) {
        inserts.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
            try(Connection con = DriverManager.getConnection(url, user, password)) {
                while (iterator.hasNext()) {
                    try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO public.test_2_users VALUES(?, ?, ?, ?)")) {
                        Row r = iterator.next();
                        preparedStatement.setInt(1, r.getInt(0));
                        preparedStatement.setString(2, r.getString(1));
                        preparedStatement.setString(3, r.getString(2));
                        preparedStatement.setString(4, r.getString(3));
                        preparedStatement.execute();
                    }
                }
            }
        });
    }

}
