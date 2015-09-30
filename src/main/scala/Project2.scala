import akka.actor._
import com.sun.javafx.tk.quantum.MasterTimer
import com.sun.org.apache.xml.internal.security.algorithms.JCEMapper.Algorithm

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration.Duration

/**
 * Created by sarathfrancis90 on 9/17/15.
 */
object Project2 {

  sealed trait Rumor
  case class  MasterInit(noOfNodes:Int,topology:String,algorithm:String) extends Rumor
  case class  Gossip_NodeInit (neighbourlist:List[Int]) extends Rumor
  case class  PushSum_NodeInit  (neighbourList:List[Int]) extends Rumor
  case object  Gossip extends Rumor
  case object  Start_PushSum extends  Rumor
  case class  Push_Sum(s:Double,w:Double)
  case object  ReceivedGossip extends Rumor
  case class  EnoughGossips() extends  Rumor
  case object Sum_Estimate_Converged extends  Rumor

  //variable to store no of nodes
 // var NoOfNodes:Int = _
  //list buffer to store nodes
  var Nodes :ListBuffer[ActorRef] = new ListBuffer[ActorRef]()
  //variable to store Actor System
  var MyActorSystem : ActorSystem = _
  //NeighbourList
  //val NeighbourList = new ListBuffer[ActorRef]()


  def main (args: Array[String])
  {

    val NoOfNodes: Int = args(0).toInt
    val Topology:String  = args(1)
    val Algorithm:String = args(2)


    //Creating Actor System
    MyActorSystem = ActorSystem ("GossipSimulator")

    //Creating MasterneighbourList.toList
    val master = MyActorSystem.actorOf(Props(new Master), name ="Master")

    //Initiating Master
    master ! MasterInit(NoOfNodes,Topology,Algorithm)

    MyActorSystem.awaitTermination()



  }

  def Node_Number(i: Int, j: Int, k:Int,Cube_Side: Int):Int = {
    val Nodenumber : Int = ((k * math.pow(Cube_Side,2)) +(i * Cube_Side) + j).toInt
    Nodenumber
  }
  class Node extends Actor with ActorLogging {

    var myNeighbours : ListBuffer[Int] = new ListBuffer[Int]
   // var No_of_Nodes: Int =_
    var Gossip_Count: Int = 0
    var MasterRef :ActorRef = _
    var neighbourRef:ActorRef = _
    var si: Double = _
    var wi: Double = _
    var Current_Sum_Estimate:Double = _
    var Sum_Estimate_Buffer: ListBuffer[Double] = new ListBuffer[Double]
    var Active_Node: Int  = 0


    def receive = {

      case Gossip_NodeInit(neighbourList) =>
       //log.info("Node Initiated")
        println("Node no. " + self.path.name + " initiated")
        MasterRef = sender()
        myNeighbours = neighbourList.to[ListBuffer]
       // No_of_Nodes = noOfNodes

//        printf("my neighbors are ")
//        myNeighbours.foreach(printf("%d  ",_))
//        println(" ")

      case PushSum_NodeInit(neighbourList)  =>

        println("Node no. " + self.path.name + " initiated")
        myNeighbours = neighbourList.to[ListBuffer]
        MasterRef =sender()
        si = self.path.name.toDouble
        wi = 1
        printf("my neighbors are ")
        myNeighbours.foreach(printf("%d  ",_))
        println(" ")
        println("S and W of Node no. " + self.path.name + " is " + si +" and "+ wi)


      case Start_PushSum =>

        println("Start Push_Sum received from the master at the Node " + self.path.name)
        si = si/2
        wi = wi/2
        Current_Sum_Estimate = si/wi
        Sum_Estimate_Buffer += Current_Sum_Estimate
        println("Current values: Si = " + si + " Wi = " + wi + "  Current Sum Estimate = " + Current_Sum_Estimate)
        printf("Sum Estimate buffer:  ")
        Sum_Estimate_Buffer.foreach(printf("%f ",_))
        println()
       // Thread.sleep(2000)
        val aRandomNumber = Random.nextInt(myNeighbours.size)
        val randomNeighbour: Int = myNeighbours(aRandomNumber)
        Nodes(randomNeighbour) !  Push_Sum(si,wi)

      case Push_Sum(s,w)  =>
        println("Push_ Sum received at Node " + self.path.name + " from Node " + sender().path.name)

        si += s
        wi += w

        Current_Sum_Estimate = si/wi
        si = si/2
        wi = wi/2

        if(Sum_Estimate_Buffer.size < 3) {
            Sum_Estimate_Buffer += Current_Sum_Estimate
          }
        else if(Active_Node == 0) {

          Sum_Estimate_Buffer.remove(0)

          Sum_Estimate_Buffer += Current_Sum_Estimate

          if(((Sum_Estimate_Buffer(0) - Sum_Estimate_Buffer(1)) < scala.math.pow(10,-10)) && ((Sum_Estimate_Buffer(1) - Sum_Estimate_Buffer(2)) < scala.math.pow(10,-10)))  {
            Active_Node = 1
            for(myNeighbour <-myNeighbours) {
              Nodes(myNeighbour) ! Sum_Estimate_Converged
            }
            MasterRef ! EnoughGossips
            println("I am done - Node "+ self.path.name)
          }
        }
            println("Current values: Si = " + si + " Wi = " + wi + "  Current Sum Estimate = " + Current_Sum_Estimate)
            printf("Sum Estimate buffer:  ")
            Sum_Estimate_Buffer.foreach(printf("%f ",_))
            println()
            //Thread.sleep(2000)
            if(myNeighbours.size > 0) {
              val aRandomNumber = Random.nextInt(myNeighbours.size)
              val randomNeighbour: Int = myNeighbours(aRandomNumber)
              Nodes(randomNeighbour) ! Push_Sum(si, wi)
            }


      case Gossip  =>
//        printf("my neighbors are ")
//        myNeighbours.foreach(printf("%d  ",_))
//        println(" ")
        Gossip_Count+=1
        if(Gossip_Count <=10)
        println("Gossip number "+ Gossip_Count + " received at " + self.path.name + " from "+ sender().path.name)

        if(Gossip_Count == 10) {
//          println("Gossip number "+ Gossip_Count + " received at " + self.path.name + " from "+ sender().path.name)
          for(myNeighbour <-myNeighbours) {
            Nodes(myNeighbour) ! EnoughGossips
          }
          MasterRef ! EnoughGossips
          println("I am done - Node "+ self.path.name)
        }
//         Thread.sleep(1000)
          self ! ReceivedGossip

      case ReceivedGossip =>
//        println("ReceivedGossip received at " + self.path.name)
//        printf("my neighbors are ")
//        myNeighbours.foreach(printf("%d  ",_))
//        println()
//        Thread.sleep(1000)

        if(myNeighbours.length > 0) {
          val aRandomNumber = Random.nextInt(myNeighbours.size)
          val randomNeighbour: Int = myNeighbours(aRandomNumber)
          Nodes(randomNeighbour) !  Gossip
        }
//          if(self.path.name == Nodes(randomNeighbour).path.name) {
//            printf ("i am %s\n", self.path.name)
//            printf("my neighbors are ")
//            myNeighbours.foreach(printf("%d  ",_))
//            println("\nsending to self")
//            printf("i generated random number %d\n", aRandomNumber)
//            printf("i am going to send to %d\n", randomNeighbour)
//            printf("that neighbors name is %s\n", Nodes(randomNeighbour).path.name)
//            Thread.sleep(3000)
//          }
        self !ReceivedGossip

      case EnoughGossips  =>
         if(myNeighbours.contains(sender().path.name.toInt))  {
           println("Node "+ self.path.name +" Received EnoughGossips from " + sender().path.name)
         myNeighbours -= sender().path.name.toInt
          // if(myNeighbours.length != 0) {
//               printf("my neighbors are ")
//               myNeighbours.foreach(printf("%d  ",_))
//               println()
//               Thread.sleep(3000)
          // }

         }
        //Thread.sleep(1000)
         self ! ReceivedGossip

      case Sum_Estimate_Converged =>

        if(myNeighbours.contains(sender().path.name.toInt)) {
          println("Node " + self.path.name + " Received EnoughGossips from " + sender().path.name)
          myNeighbours -= sender().path.name.toInt

          if(myNeighbours.size > 0) {
            val aRandomNumber = Random.nextInt(myNeighbours.size)
            val randomNeighbour: Int = myNeighbours(aRandomNumber)
            Nodes(randomNeighbour) ! Push_Sum(si, wi)
          }

        }
    }
  }

  class Master extends Actor with  ActorLogging {

    val neighbours:ListBuffer[Int] = new ListBuffer[Int]
    var noOfcompletedNodes : Int = 0
    var timeBeforeStartGossip: Long = _
    var timeAfterGossip:Long =_
    var no_Of_Nodes:Int = _
    var networktopology: String = _
    var currentalgorithm: String =_

    def receive = {

      //Initiating Master by the Main Process
      case MasterInit(noOfNodes, topology, algorithm) =>

        no_Of_Nodes = noOfNodes
        networktopology = topology
        currentalgorithm = algorithm


        //println(no_Of_Nodes +" " +networktopology +" "+ currentalgorithm)

        if (networktopology == "full") {

          for (i <- 0 until no_Of_Nodes) {
            Nodes += MyActorSystem.actorOf(Props(new Node), name = i.toString)

          }

          for (i <- 0 until no_Of_Nodes) {
            neighbours.clear()

            for (j <- 0 until no_Of_Nodes) {
              if (j != i)
                neighbours += j
            }

//            printf ("sending int to %s with neighbors ", Nodes(i).path.name)
//            neighbours.foreach(printf("--%d ", _))
//            printf("\n")
            if(currentalgorithm == "gossip")  {

              Nodes(i) ! Gossip_NodeInit(neighbours.toList)

            }
            else if( currentalgorithm == "pushsum") {


              Nodes(i)  ! PushSum_NodeInit(neighbours.toList)
             // Thread.sleep(2000)

            }


            //Thread.sleep(2000)
          }

          //println(Nodes.size)
        }
        else if (networktopology =="line") {

          for (i <- 0 until no_Of_Nodes) {
            Nodes += MyActorSystem.actorOf(Props(new Node), name = i.toString)

          }

          for(i <- 0 until no_Of_Nodes) {
            neighbours.clear()
            if(i==0) neighbours += i+1
            else if(i== (no_Of_Nodes-1)) neighbours += i-1
            else {
              neighbours += i-1
              neighbours += i+1
            }

            if(currentalgorithm == "gossip")  {
              Nodes(i) ! Gossip_NodeInit(neighbours.toList)
            }
            else if( currentalgorithm == "pushsum") {
              Nodes(i)  ! PushSum_NodeInit(neighbours.toList)
            }
            //Thread.sleep(2000)
          }


        }
        else if(networktopology == "3D")  {
          val cube_Side_rounded : Int = math.ceil(Math.cbrt(no_Of_Nodes)).toInt
          val NewNoofNodes: Int = Math.pow(cube_Side_rounded,3).toInt
//          println("Initial No of Nodes: " + no_Of_Nodes)
//          //println("Cube root: " + cube_side)
//          println("After Rounding : " + cube_Side_rounded)
//          println("New No of nodes : "  + NewNoofNodes)
          for (i <- 0 until NewNoofNodes) {
            Nodes += MyActorSystem.actorOf(Props(new Node), name = i.toString)

          }
          for( k <- 0 until cube_Side_rounded)  {
            for (i <- 0 until cube_Side_rounded)  {
              for (j <- 0 until cube_Side_rounded)  {

                val current_Node :  Int = Node_Number(i,j,k,cube_Side_rounded)
                neighbours.clear()
                if(!(i-1 < 0))
                  neighbours += Node_Number(i-1,j,k,cube_Side_rounded)
                if(!((i+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i+1,j,k,cube_Side_rounded)
                if(!(j-1 < 0))
                  neighbours += Node_Number(i,j-1,k,cube_Side_rounded)
                if(!((j+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i,j+1,k,cube_Side_rounded)
                if(!(k-1 < 0))
                  neighbours += Node_Number(i,j,k-1,cube_Side_rounded)
                if(!((k+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i,j,k+1,cube_Side_rounded)

//                println("Neighbours Of Node : " + current_Node + " are")
//                neighbours.foreach(printf("%d ",_))
//                println()


                if(currentalgorithm == "gossip")  {
                  Nodes(current_Node) ! Gossip_NodeInit(neighbours.toList)
                }
                else if( currentalgorithm == "pushsum") {
                  Nodes(current_Node)  ! PushSum_NodeInit(neighbours.toList)
                }
              }
            }
          }
        }
         else if(networktopology == "imp3D") {

          val cube_Side_rounded : Int = math.ceil(Math.cbrt(no_Of_Nodes)).toInt
          val NewNoofNodes: Int = Math.pow(cube_Side_rounded,3).toInt
          //          println("Initial No of Nodes: " + no_Of_Nodes)
          //          //println("Cube root: " + cube_side)
          //          println("After Rounding : " + cube_Side_rounded)
          //          println("New No of nodes : "  + NewNoofNodes)
          for (i <- 0 until NewNoofNodes) {
            Nodes += MyActorSystem.actorOf(Props(new Node), name = i.toString)

          }
          for( k <- 0 until cube_Side_rounded)  {
            for (i <- 0 until cube_Side_rounded)  {
              for (j <- 0 until cube_Side_rounded)  {

                val current_Node :  Int = Node_Number(i,j,k,cube_Side_rounded)
                neighbours.clear()
                if(!(i-1 < 0))
                  neighbours += Node_Number(i-1,j,k,cube_Side_rounded)
                if(!((i+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i+1,j,k,cube_Side_rounded)
                if(!(j-1 < 0))
                  neighbours += Node_Number(i,j-1,k,cube_Side_rounded)
                if(!((j+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i,j+1,k,cube_Side_rounded)
                if(!(k-1 < 0))
                  neighbours += Node_Number(i,j,k-1,cube_Side_rounded)
                if(!((k+1) > (cube_Side_rounded - 1)))
                  neighbours += Node_Number(i,j,k+1,cube_Side_rounded)

                //                println("Neighbours Of Node : " + current_Node + " are")
                //                neighbours.foreach(printf("%d ",_))
                //                println()


                if(currentalgorithm == "gossip")  {
                  Nodes(current_Node) ! Gossip_NodeInit(neighbours.toList)
                }
                else if( currentalgorithm == "pushsum") {
                  Nodes(current_Node)  ! PushSum_NodeInit(neighbours.toList)
                }
              }
            }
          }

        }


        if(currentalgorithm == "gossip")  {

          Nodes(Random.nextInt(Nodes.size)) ! Gossip

        }
        else if( currentalgorithm == "pushsum") {

          Nodes(Random.nextInt(Nodes.size)) ! Start_PushSum

        }



        timeBeforeStartGossip =  System.currentTimeMillis()


      case EnoughGossips  =>
          println("Node "+sender().path.name+" has completed")
        noOfcompletedNodes += 1
        if(noOfcompletedNodes == Nodes.length)  {

        timeAfterGossip = System.currentTimeMillis()
          println("Time taken to converge :" + (timeAfterGossip-timeBeforeStartGossip) )

          context.system.shutdown()
        }


    }

  }
}