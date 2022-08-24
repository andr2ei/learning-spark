package ru.andronov.learning.spark.dataframe.reading;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

public class ReadingFileFromHDFSPractice {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://localhost:9000/dataframe/writing/second_attempt";

        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());

        FileStatus[] fileStatuses = fs.listStatus(new Path(uri));

        Arrays.stream(fileStatuses).forEach(fileStatus -> {
            System.out.println(fileStatus.getPath());

            try(FSDataInputStream inputStream = fs.open(fileStatus.getPath())) {
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    reader.lines().forEach(System.out::println);
                }
                System.out.println();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

}
