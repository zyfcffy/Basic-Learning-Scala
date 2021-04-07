package Basic.sparkSql

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

/*
* 需求：将sql格式用代码格式化（字符串拼接）
* 输入：fact表名，ods表名，ods filter条件，ods join条件，ods join类型（join，union，union all），partition(昨天)
* 输出：format后，可以直接放在spark-sql中执行的sql
* example:
* with
* fact_count as (
* select count(*) as counts from fact_table_name where pt_d = 20210401
* ),
* ods_table1 as (
* select * from ods_table_name where pt_d = 20210401
* ),
* ods_table2 as (
* select * from ods_table_name where pt_d = 20210401
* ),
* ods_join as (
* select * from ods_table1 join ods_table2 on ods_table1.id = ods_table2.user_id
* ),
* ods_count as (
* select count(*) as counts from ods_join
* )
* select * from fact_count left join ods_count on fact_count.counts != ods_count.counts
* */

object FormatSql {

  def main(args: Array[String]): Unit = {
    executeSql()
  }

 def executeSql(): Unit ={
   val yesterday: LocalDateTime = LocalDateTime.now(ZoneId.of("Asia/Shanghai")).minusDays(1)
   val partition: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(yesterday)
   println(partition)

   val sql = new StringBuilder()
 }


  case class checkTable(factTable: String,
                        odsTables: List[String],
                        odsFilterConditions: Map[String, String],
                        odsJoinType: String,
                        odsJoinConditions: Map[String,String]
                       )
}
