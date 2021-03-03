import org.apache.commons.dbcp2.BasicDataSource

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}

object Main {

  val connectionPool = new BasicDataSource()
  connectionPool.setDriverClassName("org.postgresql.Driver")
  connectionPool.setUrl("jdbc:postgresql://localhost:5433/yugabyte")
  connectionPool.setUsername("yugabyte")
  connectionPool.setUsername("yugabyte")
  connectionPool.setInitialSize(2)

  def main(args: Array[String]): Unit = {
    var connection = connectionPool.getConnection()
    var stmt = connection.createStatement()
    stmt.execute("CREATE TABLE table0 (id int PRIMARY KEY,x int,y int,r0 int,r1 int)")
    stmt.execute("CREATE TABLE table1 (id int PRIMARY KEY,x int,y int,r0 int,r1 int)")
    stmt.execute("INSERT INTO table0 (x,y,r0,r1,id) VALUES (0,0,0,0,0)")
    stmt.execute("INSERT INTO table1 (x,y,r0,r1,id) VALUES (0,0,0,0,0)")
    stmt.close()
    connection.close()
    var break = false
    while (!break) {
      val t1 = Future(blocking(thread1()))
      val t2 = Future(blocking(thread2()))
      Await.result(t1, Duration.Inf)
      Await.result(t2, Duration.Inf)
      connection = connectionPool.getConnection()
      connection.setAutoCommit(true)
      stmt = connection.createStatement()
      val table0Result = stmt.executeQuery("SELECT * from table0 WHERE id = 0;")
      table0Result.next()
      val x = table0Result.getInt("x")
      val r0 = table0Result.getInt("r0")
      val table1Result = stmt.executeQuery("SELECT * from table1 WHERE id = 0;")
      table1Result.next()
      val y = table1Result.getInt("y")
      val r1 = table1Result.getInt("r1")
      println(s"x: $x, y: $y, r0: $r0, r1: $r1")
      if (x != 1 || y != 1) {
        println("test failed")
        break = true
      } else {
        stmt.execute("UPDATE table0 SET x = 0,y = 0,r0 = 0,r1 = 0 WHERE id = 0;")
        stmt.execute("UPDATE table1 SET x = 0,y = 0,r0 = 0,r1 = 0 WHERE id = 0;")
      }
      stmt.close()
      connection.close()
    }
    connectionPool.close()
  }

  def thread1(): Unit = {
    val connection = connectionPool.getConnection()
    connection.setAutoCommit(false)
    connection.setTransactionIsolation(8)
    val stmt = connection.createStatement()
    stmt.executeUpdate("UPDATE table1 SET y = 1 WHERE id = 0;UPDATE table0 SET r0 = x WHERE id = 0;")
    connection.commit()
    stmt.close()
    connection.close()
  }

  def thread2(): Unit = {
    val connection = connectionPool.getConnection()
    connection.setAutoCommit(false)
    connection.setTransactionIsolation(8)
    val stmt = connection.createStatement()
    stmt.executeUpdate("UPDATE table0 SET x = 1 WHERE id = 0;UPDATE table1 SET r1 = y WHERE id = 0;")
    connection.commit()
    stmt.close()
    connection.close()
  }
}
