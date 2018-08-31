Compile the code into Uber jar
-----------------------------------------------

    sbt assembly

The Jar will be created as spark-prediction-assembly-1.0.jar at [project-root-dir]/target/scala-2.11/

Run the jar
-----------------------------------------------

    /Developer/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --driver-memory 4G --executor-memory 4G \
    --master local \
    --class com.datalogs.main.Main [project-root-dir]/target/scala-2.11/spark-prediction-assembly-1.0.jar \
    --csv-dir "[input-data-dir]" \
    --feature-dir "[dir-to-store-constructed-features]" \
    --pipeline-stage 3

The command line parameters to run these program are -

    1.   "**--driver-memory**" and "**--executor-memory**" are optional and find details at http://spark.apache.org/docs/latest/configuration.html.

    2.   "**--master**" is required and can be set at "**local**" for local installation of spark or "**yarn**" (e.g. --master yarn)

    3.   "**--class**" is required to specify the fully-qualified-name of the "main" method in JAR and the location of the mortality_prediction-assembly-1.0.jar file.

    4.   "**--csv-dir**" is required to specify the location of MIMIC and other input files as required by this program.
            e.g. --csv-dir "[sourcecode]/MIMIC/input" (please don't put "/" at the end)

    5.   "**--feature-dir**" is required to specify the location of the program generated features file(s).
            e.g. --feature-dir "[sourcecode]/features" (please don't put "/" at the end)

    6.   "**--pipeline-stage**" is required to specify the workflow steps that the program will execute. There are 4 different options -

             --pipeline-stage 1 => For constructing features only.
             --pipeline-stage 2 => To run the all the models using the feature previous created
             --pipeline-stage 3 => To construct features and run models in one step (together)


  println("Welcome to the Scala worksheet")
  
  88 + 30
  
  var x1 = 4
  x1 = 6
  
  def size = 2
  
  5 * size
  
  def square(x: Double) = x * x
  
  square(2)
	  
  square(square(4))
  
  def sumOfSquare(x: Double, y: Double): Double =
  	square(x) + square(y)
  
  sumOfSquare(3, 4+5)
  
  /////////////////////Function by Name vs Value/////////////////////////////////////
  
  def first(x: Int, y: Int) = x
  
  first(1, 2)
  
  def constOne(x: Int, y: => Int) = 1
  
  constOne(1+2, 1)
  
  ////////////////////////Function calling Function///////////////////////////////
  
  def abs(x: Double) = if (x < 0) -x else x
  
  def sqrtIter(guess: Double, x: Double): Double =
		if (isGoodEnough(guess, x)) guess
		else sqrtIter(improve(guess, x), x)
		
	def improve(guess: Double, x: Double) =
		(guess + x / guess) / 2
	
	def isGoodEnough(guess: Double, x: Double) =
		abs(guess * guess - x)/x < 0.001
		
	def sqrt(x: Double) = sqrtIter(1.0, x)
  
  sqrt(4)
	sqrt(2)
	sqrt(1e-6)
  sqrt(1e6)
  
  //////////////////////Function inside function////////////////////////////////
  
  def sqrt1(x: Double) = {
    
	  def sqrtIter(guess: Double): Double =
			if (isGoodEnough(guess)) guess
			else sqrtIter(improve(guess))
			
		def improve(guess: Double) =
			(guess + x / guess) / 2
		
		def isGoodEnough(guess: Double) =
			abs(guess * guess - x)/x < 0.001
			
		sqrtIter(1.0)
  }
  sqrt1(9)
  
  //////////////////Tail recursion ///////////////////////////////////////
  
 def gcd(a: Int, b: Int): Int =
	if (b == 0) a else gcd(b, a % b)
	
 def factorial(n: Int): Int =
	if (n == 0) 1 else n * factorial(n - 1)
	
	factorial(4)
	
	def factorial1(n: Int): Int = {
		def loop(acc: Int, n: Int): Int =
			if (n==0) acc
			else loop(acc * n, n-1)
		loop(1, n)
	}
	
	factorial1(4)
 
 ////////////////////Function passing Func//////////////////////////////////
 
 def id(x: Int): Int = x
 def cube(x: Int): Int = x * x * x
 
 def sum(f: Int => Int, a: Int, b: Int): Int =
 	if (a > b) 0
 	else f(a) + sum(f, a+1, b)
 
 def sumInts(a: Int, b: Int) = sum(id, a, b)
 def sumCubes(a: Int, b: Int) = sum(cube, a, b)
 
 //////////////////Anonymous function///////////////////////////////////////
 
 def sumInts1(a: Int, b: Int) = sum(x=>x, a, b)
 def sumCubes1(a: Int, b: Int) = sum(x => x * x * x, a, b)
 
 /////////////////////Func Return Func////////////////////////////////////
 
 def sum1(f: Int => Int): (Int, Int) => Int = {
 	def sumF(a: Int, b: Int): Int =
 		if (a > b) 0
 		else f(a) + sumF(a+1, b)
 		
 	sumF
 }
 
 def sumInts2 = sum1(x => x)
 def sumCubes2 = sum1(x => x * x * x)
 
 sumCubes2(1, 10)
 sum1(cube)(1, 10)
 sum1(x => x * x * x)(1, 10)
 
 /////////////////////Object orientation////////////////////////////////////
 
 class Rational(x: Int, y: Int) {
 
 	private def gcd(a: Int, b: Int): Int
 		= if (b == 0) a else gcd(b, a % b)
 		
 	private val g = gcd(x, y)
 	
 	def numer = x/g
 	def denom =y/g
 	
 	def add(that: Rational) = new Rational(numer * that.denom + that.numer * denom, denom * that.denom)
 	
 	def +(r: Rational) =
		new Rational(numer * r.denom + r.numer * denom, denom * r.denom)
 }
 
 val x = new Rational(1, 2)
 val y = new Rational(1, 3)
 
 val w = x.add(y)
 w.numer
 
 val z = x add y
 z.numer
 
 val u = x + y
 u.numer
 
 /////////////////////Polymorphism////////////////////////////////////
 
 abstract class IntSet {
	def incl(x: Int): IntSet
	def contains(x: Int): Boolean
	def foo = 1
 }

 class Empty extends IntSet {
	def contains(x: Int): Boolean = false
	def incl(x: Int): IntSet = new NonEmpty(x, new Empty, new Empty)
 }

 class NonEmpty(elem: Int, left: IntSet, right: IntSet) extends IntSet {

	def contains(x: Int): Boolean =
		if (x < elem) left contains x
		else if (x > elem) right contains x
		else true
	
	def incl(x: Int): IntSet =
		if (x < elem) new NonEmpty(elem, left incl x, right)
		else if (x > elem) new NonEmpty(elem, left, right incl x)
		else this
		
	override def foo = 2
 }

 object Empty extends IntSet {
	def contains(x: Int): Boolean = false
	def incl(x: Int): IntSet = new NonEmpty(x, Empty, Empty)
 }

 object Hello {
	def main(args: Array[String]) =
		println("hello world!")
 }
 
 val ne = new Empty()
 trait List1[T] {
	def isEmpty: Boolean
	def head: T
	def tail: List1[T]
 }
 
 class Cons[T](val head: T, val tail: List1[T]) extends List1[T] {
	def isEmpty = false
 }
 
 class Nil[T] extends List1[T] {
	def isEmpty = true
	def head = throw new NoSuchElementException("Nil.head")
	def tail = throw new NoSuchElementException("Nil.tail")
 }
 
 val nil = new Nil[Int]()
 nil.isEmpty
 
 /////////////////////Patern Matching//////////////////////////////
 
 trait Expr {
	def isNumber: Boolean
	def isSum: Boolean
	def numValue: Int
	def leftOp: Expr
	def rightOp: Expr
	def eval: Int
 }
 case class Number(n: Int) extends Expr {
	def isNumber: Boolean = true
	def isSum: Boolean = false
	def numValue: Int = n
	def leftOp: Expr = throw new Error("Number.leftOp")
	def rightOp: Expr = throw new Error("Number.rightOp")
	def eval: Int = n
 }
 case class Sum(e1: Expr, e2: Expr) extends Expr {
	def isNumber: Boolean = false
	def isSum: Boolean = true
	def numValue: Int = throw new Error("Sum.numValue")
	def leftOp: Expr = e1
	def rightOp: Expr = e2
	def eval: Int = e1.eval + e2.eval
 }
 
 val num1 = new Number(5)
 val num2 = new Number(6)
 
 val sum11 = new Sum(num1, num2)
 sum11.eval
  
 def eval(e: Expr): Int = {
	if (e.isNumber) e.numValue
	else if (e.isSum) eval(e.leftOp) + eval(e.rightOp)
	else throw new Error("Unknown expression " + e)
 }
  
 eval(new Sum(num1, num2))
 
 def eval1(e: Expr): Int = e match {
	case Number(n) => n
	case Sum(e1, e2) => eval(e1) + eval(e2)
 }
 
 eval1(new Sum(num1, num2))
 
 ////////////////////////// MERGE SORT /////////////////////////////////////
 def msort(xs: List[Int]): List[Int] = {
	val n = xs.length/2
	if (n == 0) xs
	else {
  	//def merge(xs: List[Int], ys: List[Int]) = ???
  	val (fst, snd) = xs splitAt n
  	merge(msort(fst), msort(snd))
	}
 }
 def merge(xs: List[Int], ys: List[Int]): List[Int] = {
	xs match {
		case Nil =>
			ys
		case x :: xs1 =>
			ys match {
				case Nil =>
					xs
				case y :: ys1 =>
					if (x < y) x :: merge(xs1, ys)
					else y :: merge(xs, ys1)
			}
	}
 }
 
 msort(List(1,4,6,2))

