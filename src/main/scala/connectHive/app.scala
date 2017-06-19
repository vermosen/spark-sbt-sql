package connectHive

import java.sql.{Connection, DriverManager, ResultSet};
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object app 
{
  // connect to a sql server database
    def main(arg: Array[String]) 
    {
        // disable logger spam
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        
        val partSize: Int = 10
        
        // new spark 2.0.0 syntax
        val spark: SparkSession = SparkSession
            .builder           ()
            .master            ("local[*]")
            .appName           ("spark.connect")
            .enableHiveSupport ()
            .config            ("spark.logLevel", "WARN")
            .config            ("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse")
            .getOrCreate       ()
            
        spark.conf.set("spark.sql.shuffle.partitions", 6)
        spark.conf.set("spark.executor.memory", "2g")
        spark.conf.set("spark.driver.memory", "2g")    
        
        println("starting test")
        
        try
        {
          
            // create the sql context 
            val sqlContext = new org.apache.spark.sql.SQLContext()
            
            // load the drivers 
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            
            // database
            val cnxStr: String = "jdbc:microsoft:sqlserver://localhost:1433;DatabaseName=dataRepository;integratedSecurity=true;"
            
            // Setup the connection
            // val cnx = DriverManager.getConnection(cnxStr)
            
            val query: String = 
              "(" + 
                  "	SELECT distinct [FILE_DATE]" +
                  " FROM [dataRepository].[dbo].[dtccTransactions]" +
                  " ORDER BY [FILE_DATE] DESC" +
              ") AS test"
                        
            try
            {
                val df: DataFrame = spark.read
                    .format("jdbc")
                    .option("url", cnxStr)
                    .option("dbtable", query)
                    .load()
                    
                println("retrieved " + df.count + " records")
                println("sample:")
                println(df.head())
                
            }
            catch
            {
                case e: Exception => println("an exception occurred: " + e.getMessage)
            }
            finally
            {
                //cnx.close
            }
        }
        catch
        {
            case e: Exception => println("an exception occurred: " + e.getMessage)
        }
        finally
        {
            println("exiting...")
            spark.stop                      // terminate spark session
        }
    }
    
    // a simple project to test spark initialization
    def mainOld(arg: Array[String]) 
    {
        val conf = new SparkConf()
            conf.setAppName("helloWorld")
                .setMaster("local")
      
        val sc = new SparkContext(conf)
            sc.setLogLevel("WARN")
        
        // do stuff
        println("Hello, world!")
        
        // terminate spark context
        sc.stop()
    }
}