/**
  * locate 
  * Created by 79875 on 2017/6/27.
  * scala 隐式转换 理解scala
  * //Scala 做自动转换会去找 带有implicit关键字的方法 去做隐式转换
  */
class Car (var name:String){

}

class SuperMan(var name:String){
    def emitLaser()=println("emit a pingpangball !!!!")
}

class Imlicit02 {

}

object Imlicit02{

    implicit def carToSpuperman(car :Car):SuperMan={
        new SuperMan(car.name)
    }

    def main(args: Array[String]): Unit = {
        var car =new Car("qingtainzhu")
        car.emitLaser()
    }
}
