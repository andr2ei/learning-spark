package ru.andronov.learning.spark.dataframe.model

import java.sql.Date

case class User(id: Int,
                firstName: String,
                lastName: String,
                age: Int,
                dateOfBirth: Date)
