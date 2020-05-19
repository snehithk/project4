import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Staging {

  def Directories() {
    val conf = new Configuration()
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)
    println(fs.getUri)

    if (fs.exists(new Path("/user/fall2019/snehith/project4"))) {
      fs.delete(new Path("/user/fall2019/snehith/project4"), true)
      println("deleted folder already found")
      fs.mkdirs(new Path("/user/fall2019/snehith/project4"))
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/trips"))
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/calendar_dates"))
      fs.mkdirs(new Path("/user/fall2019/snehith/project4/frequencies"))
      println("created folders")
    }
  }
  def FilesCopy(): Unit ={
    val conf = new Configuration()
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)
    if (fs.exists(new Path("/user/fall2019/snehith/project4"))) {
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/trips.txt"),
        new Path("/user/fall2019/snehith/project4/trips/"))
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/frequencies.txt"),
        new Path("/user/fall2019/snehith/project4/frequencies/"))
      fs.copyFromLocalFile(new Path("/home/snehith/Documents/stm/calendar_dates.txt"),
        new Path("/user/fall2019/snehith/project4/calendar_dates/"))
    }
  }

}
