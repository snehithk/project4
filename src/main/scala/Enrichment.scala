import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object Enrichment extends App {
  val conf = new Configuration()
  val uri ="/user/fall2019/snehith/"

  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem= FileSystem.get(conf)
  println(fs.getUri)

  var create = new Staging
  create.Directories()

  // Step 1: load the Hive JDBC driver
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager
    .getConnection("jdbc:hive2://172.16.129.58:10000/snehith;user=snehith;password=snehith")
  val stmt = connection.createStatement()
  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.ext_trips")

  stmt.execute("CREATE EXTERNAL TABLE fall2019_snehith.ext_trips ( " +
    "route_id INT,"+
    "service_id STRING,"+
    "trip_id STRING,"+
    "trip_headsign STRING,"+
    "direction_id INT,"+
    "shape_id INT,"+
    "wheelchair_accessible INT,"+
    "note_fr STRING,"+
    "note_en STRING ) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE "+
    " LOCATION '/user/fall2019/snehith/project4/trips'"+
    " tblproperties(" +
    "'skip.header.line.count' = '1',"+
    "'serialization.null.format' = '')"

  )
  println("created external table for trip")

  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.ext_frequencies")

  stmt.execute("CREATE EXTERNAL TABLE fall2019_snehith.ext_frequencies ( " +
    " trip_id STRING,"+
    " start_time STRING,"+
    " end_time STRING,"+
    " headway_secs INT ) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE "+
    " LOCATION '/user/fall2019/snehith/project4/frequencies'"+
    " tblproperties(" +
    "'skip.header.line.count' = '1',"+
    "'serialization.null.format' = '')"

  )
  println("created external table for frequencies")
  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.ext_calendar_dates")
  stmt.execute("CREATE EXTERNAL TABLE fall2019_snehith.ext_calendar_dates ( " +
    " service_id STRING, "+
    " date INT, "+
    "exception_type INT ) "+
    " ROW FORMAT DELIMITED "+
    " FIELDS TERMINATED BY ',' "+
    " STORED AS TEXTFILE "+
    " LOCATION '/user/fall2019/snehith/project4/calendar_dates'"+
    " tblproperties(" +
    "'skip.header.line.count' = '1',"+
    "'serialization.null.format' = '')"

  )
  println("created external table for calendar_dates")
  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.enriched_trip")

  stmt.execute("CREATE TABLE fall2019_snehith.enriched_trip ( " +
    "route_id INT,"+
    "service_id STRING,"+
    "trip_id STRING,"+
    "trip_headsign STRING,"+
    "direction_id INT,"+
    "shape_id INT,"+
    "note_fr STRING,"+
    "note_en STRING,"+
    " start_time STRING,"+
    " end_time STRING,"+
    " headway_secs INT ," +
    " date INT, "+
    " exception_type INT ) "+
    " PARTITIONED BY (wheelchair_accessible int)" +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS PARQUET "
  )
  println("created managed  table for enriche_trip")
  var add = new Staging
  add.FilesCopy()
  println("Add source data to staging folders")
  // Step 3: run the query and process the results
  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

  stmt.execute("INSERT OVERWRITE TABLE fall2019_snehith.enriched_trip PARTITION(wheelchair_accessible) "+
    " SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign," +
    "t.direction_id,t.shape_id,t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible "+
    " FROM fall2019_snehith.ext_trips AS t "+
    "FULL OUTER JOIN fall2019_snehith.ext_frequencies As f "+
    " ON t.trip_id = f.trip_id "+
    " FULL OUTER JOIN fall2019_snehith.ext_calendar_dates AS c "+
    " ON t.service_id =  c.service_id " +
     " WHERE t.wheelchair_accessible < 5 ")

  println("inserted data to the enriched table")

//    val res: ResultSet = stmt.executeQuery("SHOW PARTITIONS fall2019_snehith.enriched_trip")
//    println("partitions"+res.getString(1))


 // while (res.next()) {
 //   println("MID: " + res.getInt(2))
 // }

  // Step 4: close resources
  stmt.close()
  connection.close()

}
