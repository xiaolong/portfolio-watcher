/**
  * Created by xcheng on 3/30/16.
  */

import scala.io.Source

case class Stock(id: String,
                 t: String, //ticker
                 e: String, //exchange id
                 l: Double,
                 l_fix: Double,
                 l_cur: Double,
                 s: String,
                 ltt: String,
                 lt: String,
                 lt_dts: String,
                 c: String,
                 c_fix: Double,
                 cp: Double,
                 cp_fix: Double,
                 ccol: String,
                 pcls_fix: Double,
                 el: Double,
                 el_fix: Double,
                 el_cur: Double,
                 elt: String,
                 ec: String,
                 ec_fix: Double,
                 ecp: Double,
                 ecp_fix: Double,
                 eccol: String,
                 div: Option[Double],
                 yld: Option[Double])

object Stock{

  def apply(m: Map[String, String]) : Stock ={
    Stock(id = m.getOrElse("id", ""),
      t = m.getOrElse("t", ""),
      e = m.getOrElse("e", ""),
      l=m.getOrElse("l", "0").toDouble,
      l_fix=m.getOrElse("l_fix", "0").toDouble,
      l_cur=m.getOrElse("l_cur", "0").toDouble,
      s=m.getOrElse("s",""),
      ltt=m.getOrElse("ltt", ""),
      lt=m.getOrElse("lt",""),
      lt_dts = m.getOrElse("lt_dts",""),
      c=m.getOrElse("c",""),
      c_fix=m.getOrElse("c","0").toDouble,
      cp=m.getOrElse("cp","0").toDouble,
      cp_fix=m.getOrElse("cp_fix","0").toDouble,
      ccol=m.getOrElse("ccol",""),
      pcls_fix = m.getOrElse("pcls_fix","0").toDouble,
      el=m.getOrElse("el","0").toDouble,
      el_fix = m.getOrElse("el_fix", "0").toDouble,
      el_cur = m.getOrElse("el_cur","0").toDouble,
      elt=m.getOrElse("elt",""),
      ec=m.getOrElse("ec",""),
      ec_fix=m.getOrElse("ec_fix","0").toDouble,
      ecp=m.getOrElse("ecp","0").toDouble,
      ecp_fix=m.getOrElse("ecp_fix","0").toDouble,
      eccol = m.getOrElse("eccol",""),
      div=m.getOrElse("div","") match {case ""=>None; case x: String => Some(x.toDouble)},
      yld=m.getOrElse("yld", "") match {case "" => None; case x: String=> Some(x.toDouble)}
    )

  }
}

object StockTest {


  def main(args: Array[String]): Unit = {

    val googleFinStream =  Source.fromURL("http://finance.google.com/finance/info?client=ig&q=AAPL,YHOO")
    val raw = googleFinStream.getLines().mkString("").replace("//", "")

    val parsed = scala.util.parsing.json.JSON.parseFull(raw)

    val results = parsed match{
      case Some(lst : List[Map[String, String]]) => {
        var objs = List[Stock]()
        for (elem <- lst) {
          objs = Stock(elem) :: objs
        }
        objs
      }

      case _ => Nil
    }


    println(results.length)
    for (x<- results) {
      println(x)
    }

    /*
    def x(bigger:Int): Option[Float] = {
      if (bigger>10) Some(100) else None
    }
    val y = x(11)
    val z = y.getOrElse(10)
    println(z)



    def x1(bigger:Int): Option[(Double, Int)] = {
      if (bigger>10) Some((100.0, 200)) else None
    }
    val y1 = x1(9)
    val z1 = y1.getOrElse(10)
    println(z1)

    */

  }

}
