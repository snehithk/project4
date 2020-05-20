import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Staging {
  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)

  def Directories() {
    println(fs.getUri)
    if (fs.exists(new Path("/user/fall2019/snehith/project4"))) {
      fs.delete(new Path("/user/fall2019/snehith/project4"), true)
      println("deleted folder already found")
      fs.mkdirs(new Path("/user/fall2019/snehith/project4"))
      println("created directory project4")
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/trips"))
      println("created directory for trips")
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/calendar_dates"))
      println("created directory for calendar_dates")
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/frequencies"))
      println("created directory for frequencies")
    }
  }
  def FilesCopy(): Unit ={
    if (fs.exists(new Path("/user/fall2019/snehith/project4"))) {
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/trips.txt"),
        new Path("/user/fall2019/snehith/project4/trips/"))
      println("added trips file to hdfs")
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/frequencies.txt"),
        new Path("/user/fall2019/snehith/project4/frequencies/"))
      println("added frequencies file to hdfs")
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/calendar_dates.txt"),
        new Path("/user/fall2019/snehith/project4/calendar_dates/"))
      println("added calendar_dates file to the hdfs")
    }
  }

}
