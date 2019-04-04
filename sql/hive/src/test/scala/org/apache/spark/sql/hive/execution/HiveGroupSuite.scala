/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.scalatest.BeforeAndAfter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.TestHive.sparkSession
import org.apache.spark.sql.test.SQLTestUtils

class HiveGroupSuite extends HiveComparisonTest with SQLTestUtils with BeforeAndAfter with Logging {


  def spark: SparkSession = sparkSession

  test("wangshuo") {
//    sql("CREATE TABLE `t1`(`login_id` bigint)")
//    sql("CREATE TABLE `t2`(`employee_id` bigint)")
    import testImplicits._
    Seq(1).toDF("login_id").createOrReplaceTempView("t1")
    Seq(1).toDF("employee_id").createOrReplaceTempView("t2")

    sql(
      """
        |SELECT
        |    a.loginid
        |FROM
        |(
        |    SELECT
        |        emp.loginid
        |    FROM
        |    (
        |        SELECT
        |            login_id AS loginid,
        |            CASE WHEN login_id < 0 THEN (1000000 - login_id) ELSE login_id END AS mt_emp_id
        |        FROM
        |            t1
        |    ) emp
        |    JOIN
        |    (
        |        SELECT
        |        employee_id
        |        FROM t2
        |    ) pos ON pos.employee_id = emp.mt_emp_id
        |) a
        |LEFT JOIN
        |(
        |  SELECT
        |    login_id AS loginid
        |  FROM
        |    t1
        |)  b ON a.loginid = b.loginid
        |
      """.stripMargin).show()
  }

}
