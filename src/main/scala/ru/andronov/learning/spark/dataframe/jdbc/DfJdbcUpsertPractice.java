package ru.andronov.learning.spark.dataframe.jdbc;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.andronov.learning.spark.dataframe.model.ModelCreatorJava;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class DfJdbcUpsertPractice {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DF JDBC practice")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> newUserDF = ModelCreatorJava.createUserDF(spark)
                .select("id", "firstName", "lastName", "address.country");
        Dataset<Row> oldUserDF = getOldUserDF(spark);

        Dataset<Row> joined = joinUsers(newUserDF, oldUserDF);
        Dataset<JoinedUsers> joinedCUD = getJoinedCUD(joined);

        joinedCUD.show();

        Dataset<Row> inserts = joinedCUD.filter(col("operation").equalTo(lit("INSERT")))
                .select("nId", "nFirstName", "nLastName", "nCountry");
        Dataset<Row> deletes = joinedCUD.filter(col("operation").equalTo(lit("DELETE")))
                .select("oId", "oFirstName", "oLastName", "oCountry");
        Dataset<Row> updates = joinedCUD.filter(col("operation").equalTo(lit("UPDATE")))
                .select("nId", "nFirstName", "nLastName", "nCountry");

        CUDWriter writer = new CUDWriter();
        writer.writeInserts(inserts);
        writer.writeUpdates(updates);
        writer.writeDeletes(deletes);
    }

    private static Dataset<JoinedUsers> getJoinedCUD(Dataset<Row> joined) {
        Dataset<JoinedUsers> joinedWithOperation = joined.map((MapFunction<Row, JoinedUsers>) row -> {
            JoinedUsers joinedUsers;
            if (row.isNullAt(0)) {
                joinedUsers = new JoinedUsers(-1, null, null, null,
                        row.getInt(4), row.getString(5), row.getString(6), row.getString(7), "DELETE");
            } else if (row.isNullAt(4)) {
                joinedUsers = new JoinedUsers(row.getInt(0), row.getString(1), row.getString(2), row.getString(3),
                        -1, null, null, null, "INSERT");
            } else {
                UserTruncated newUser = new UserTruncated(row.getInt(0), row.getString(1), row.getString(2), row.getString(3));
                UserTruncated oldUser = new UserTruncated(row.getInt(4), row.getString(5), row.getString(6), row.getString(7));
                if (newUser.equals(oldUser)) {
                    joinedUsers = new JoinedUsers(row.getInt(0), row.getString(1), row.getString(2), row.getString(3),
                            row.getInt(4), row.getString(5), row.getString(6), row.getString(7), "SKIP");
                } else {
                    joinedUsers = new JoinedUsers(row.getInt(0), row.getString(1), row.getString(2), row.getString(3),
                            row.getInt(4), row.getString(5), row.getString(6), row.getString(7), "UPDATE");
                }
            }
            return joinedUsers;
        }, Encoders.bean(JoinedUsers.class)).persist();
        return joinedWithOperation;
    }

    private static Dataset<Row> joinUsers(Dataset<Row> newUserDF, Dataset<Row> oldUserDF) {
        return newUserDF.as("n")
                .join(oldUserDF.as("o"), col("n.id").equalTo(col("o.id")), "full_outer")
                .select(col("n.id").as("n_id"),
                        col("n.firstName").as("n_firstName"),
                        col("n.lastName").as("n_lastName"),
                        col("n.country").as("n_country"),
                        col("o.id").as("o_id"),
                        col("o.firstName").as("o_firstName"),
                        col("o.lastName").as("o_lastName"),
                        col("o.country").as("o_country"));
    }

    private static Dataset<Row> getOldUserDF(SparkSession spark) {
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        return spark.read().jdbc("jdbc:postgresql://localhost:5432/learning-spark", "public.test_2_users", props);
    }
}
