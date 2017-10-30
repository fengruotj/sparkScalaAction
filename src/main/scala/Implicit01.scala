

/**
  * locate com.basic.spark
  * Created by 79875 on 2017/6/27.
  * scala 隐式转换 理解scala
  */
class Implicit01 {

}

class SpecialPerson(var name:String) {

}

class Older(var name:String){

}

class Student(var name:String){

}

object Implicit01{
    //Scala 做自动转换会去找 带有implicit关键字的方法 去做隐式转换
    implicit def objectToSpecialPerson(obj: Object): SpecialPerson={
        if(obj.getClass == classOf[Older]){
            val older=obj.asInstanceOf[Older]
            new SpecialPerson(older.name)
        }else if (obj.getClass == classOf[Student]){
            val stu=obj.asInstanceOf[Student]
            new SpecialPerson(stu.name)
        }else {
           return Nil
        }
    }
    var sumTickets=0

    def buySpecialTicket(specialPerson : SpecialPerson ): Unit ={
        sumTickets+=1
        println(sumTickets)
    }

    def main(args: Array[String]): Unit = {
        val older=new Older("tanzhenghua")
        buySpecialTicket(older)
    }
}
