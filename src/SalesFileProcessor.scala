import scala.io.Source
import scala.util.Sorting.stableSort
import scala.collection.mutable.ArrayOps._
/**
 * BlackPepper code camp 13/3/13
 * 
 * Given a file of sales data like...
 *  
 *  R4.C1.D2 P5.L M 50 10
 *  R4.C1.D3 P5.M M 23 4
 *  R4.C1.D3 P5.L F 38 6
 *  
 *  i.e. 
 *  Region[.City][.District] Product[.Size] Gender Age Count
 *    
 *  ... find the top-selling city, the top-selling product at that city, and the top-selling size of that product
 * Then, find the median age of shoppers in that city, and find the total number of items bought by women aged 
 * from 20 to 30. 
 * 
 * City, Distict, and size are all optional.
 *  
 */
object SalesFileProcessor extends App {

	val records:List[SalesRecord] = Source.fromFile("./sales.txt").getLines.map( l=> SalesRecord(l) ).toList
	
	val topLocation = findHighestSumOfItems (records.groupBy(groupByLocation))

	val topProduct = findHighestSumOfItems (records
												.groupBy(groupByLocation)(topLocation._1)
												.groupBy(_.productCode))

	val topSize = findHighestSumOfItems (records
						                   .groupBy(groupByLocation)(topLocation._1)
						                   .groupBy(_.productCode)(topProduct._1)
						                   .groupBy(_.size))
	
	
	val medianAge = median(records.filter(_.location == topLocation._1).map(_.age))
	
	val itemsBoughtBy20SomethingWomen =sumCounts(records.filter(r=>r.gender == "F" && r.age >19 && r.age < 31)) 
	
	println("%s records read".format(records.size))
	println("Top location: %s (%s items)".format(topLocation._1, topLocation._2))
	println("Top product at location %s: %s  (%s items)".format(topLocation._1,topProduct._1,topProduct._2))
	println("Top size of product %s at location %s: %s  (%s items)".format(topLocation._1,topProduct._1,topSize._1,topSize._2))
	println("Median age of purchasers at location %s :%s".format(topLocation._1,medianAge))	
	println("Total number of items bought by women in their 20s over all locations: " +itemsBoughtBy20SomethingWomen)


    def median(s: Seq[Integer]) ={
      val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    }
    

	def groupByLocation(s:SalesRecord):Location = {
	  s.location.cityCode match {
	    case None=>UnknownLocation()
	    case Some(code) => s.location
	  }
	}
	
	def sortByCountDescending(a:Tuple2[Any,Int], b:Tuple2[Any,Int]):Boolean = {
	  a._2 > b._2
	}
	
	def sumCounts(sales:Seq[SalesRecord]):Int = {
	  sales.foldLeft(0)(_+_.count)
	}
	
	def findHighestSumOfItems[T](records:Map[T,Seq[SalesRecord]]):Tuple2[T,Int]= {
	  records.mapValues(sumCounts).toList.sortWith(sortByCountDescending).head
	}

}

case class Location(regionCode:String, cityCode:Option[String]){
  override def toString()={
    "%s%s".format(regionCode,cityCode.map("." + _).getOrElse("") )
  }
}

object UnknownLocation{
  def apply():Location = {
    Location("Unknown",None)
  }
}
case class SalesRecord(location:Location, productCode: String, size: Option[String], gender: String, age: Integer, count: Integer) {}

object SalesRecord {

  implicit def a2oa[T](a:Array[T]) = new OptionalGetableArray(a)

  def apply(line: String):SalesRecord =  {

    val elements = line.split(" ")
    val x = elements(0)
    val location = elements(0).split('.')
    val product = elements(1).split('.')

    location.optionallyGet(1)
    
    new SalesRecord(location = Location(location(0),location.optionallyGet(1)),
        productCode =  product(0),
        size =         product.optionallyGet(1), 
        gender =       elements(2),
        age =          elements(3).toInt,
        count =        elements(4).toInt)
  }

}
class OptionalGetableArray[T](source:Array[T]){
  
  def optionallyGet(index:Integer): Option[T] = {
	    source.isDefinedAt(index) match {
	      case true=>Some(source(index))
	      case _ =>None
	    }
  }
}

