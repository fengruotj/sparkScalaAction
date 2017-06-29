/**
  * locate 
  * Created by 79875 on 2017/6/27.
  * implicit 隐式转换参数
  */
class Implicit03 {

}

class SignPen(){
    def write(name:String)= println(name)
}

object ImlicitContext{
    implicit val signPen=new SignPen;
}
object Implicit03{
    def signForExm(name:String)(implicit signPen: SignPen): Unit ={
        signPen.write("Exm："+name)
    }

    def signForRegister(name:String)(implicit signPen: SignPen): Unit ={
        signPen.write("Register："+name)
    }

    def main(args: Array[String]): Unit = {
        import ImlicitContext._
        signForExm("tanjie")
        signForRegister("tanjie")
    }
}
