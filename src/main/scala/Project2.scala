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
  case object Gossip extends Rumor
  case object Start_PushSum extends  Rumor
  case class  Push_Sum(s:Double,w:Double)
  case object ReceivedGossip extends Rumor
  case class  EnoughGossips() extends  Rumor
  case object Sum_Estimate_Converged extends  Rumor

  var Nodes :ListBuffer[ActorRef] = new ListBuffer[ActorRef]()

  var MyActorSystem : ActorSystem = _

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

//        println("Node no. " + self.path.name + " initiated")
        MasterRef = sender()
        myNeighbours = neighbourList.to[ListBuffer]

      case PushSum_NodeInit(neighbourList)  =>

//        println("Node no. " + self.path.name + " initiated")
        myNeighbours = neighbourList.to[ListBuffer]
        MasterRef =sender()
        si = self.path.name.toDouble
        wi = 1
//        printf("my neighbors are ")
//        myNeighbours.foreach(printf("%d  ",_))
//        println(" ")
//        println("S and W of Node no. " + self.path.name + " is " + si +" and "+ wi)


      case Start_PushSum =>

//        println("Start Push_Sum received from the master at the Node " + self.path.name)
        si = si/2
        wi = wi/2
        Current_Sum_Estimate = si/wi
        Sum_Estimate_Buffer += Current_Sum_Estimate
//        println("Current values: Si = " + si + " Wi = " + wi + "  Current Sum Estimate = " + Current_Sum_Estimate)
//        printf("Sum Estimate buffer:  ")
//        Sum_Estimate_Buffer.foreach(printf("%f ",_))
//        println()
        val aRandomNumber = Random.nextInt(myNeighbours.size)
        val randomNeighbour: Int = myNeighbours(aRandomNumber)
        Nodes(randomNeighbour) !  Push_Sum(si,wi)

      case Push_Sum(s,w)  =>

//        println("Push_ Sum received at Node " + self.path.name + " from Node " + sender().path.name)

        si += s
        wi += w

        Current_Sum_Estimate = si/wi
        si = si/2
        wi = wi/2

        if(Active_Node == 0) {



          if(Sum_Estimate_Buffer.size < 3) {
            Sum_Estimate_Buffer += Current_Sum_Estimate
          }
          else  {

            Sum_Estimate_Buffer.remove(0)

            Sum_Estimate_Buffer += Current_Sum_Estimate

            if(((Sum_Estimate_Buffer(0) - Sum_Estimate_Buffer(1)) < scala.math.pow(10,-10)) && ((Sum_Estimate_Buffer(1) - Sum_Estimate_Buffer(2)) < scala.math.pow(10,-10)))  {
              Active_Node = 1
              MasterRef ! EnoughGossips
//              println("I am done - Node "+ self.path.name)
            }
          }

        }
//        println("Current values at Node : " +self.path.name + " is  : Si = " + si + " Wi = " + wi + "  Current Sum Estimate = " + Current_Sum_Estimate)
//        printf("Sum Estimate buffer:  ")
//        Sum_Estimate_Buffer.foreach(printf("%f ",_))
//        println()
        if(myNeighbours.size > 0) {
          val aRandomNumber = Random.nextInt(myNeighbours.size)
          val randomNeighbour: Int = myNeighbours(aRandomNumber)
          Nodes(randomNeighbour) ! Push_Sum(si, wi)
        }


      case Gossip  =>
        Gossip_Count+=1
//        if(Gossip_Count <=10)
//        println("Gossip number "+ Gossip_Count + " received at " + self.path.name + " from "+ sender().path.name)

        if(Gossip_Count == 10) {
//          println("Gossip number "+ Gossip_Count + " received at " + self.path.name + " from "+ sender().path.name)
          for(myNeighbour <-myNeighbours) {
            Nodes(myNeighbour) ! EnoughGossips
          }
          MasterRef ! EnoughGossips
//          println("I am done - Node "+ self.path.name)
        }
        self ! ReceivedGossip

      case ReceivedGossip =>

        if(myNeighbours.length > 0) {
          val aRandomNumber = Random.nextInt(myNeighbours.size)
          val randomNeighbour: Int = myNeighbours(aRandomNumber)
          Nodes(randomNeighbour) !  Gossip
        }
        self !ReceivedGossip

      case EnoughGossips  =>
         if(myNeighbours.contains(sender().path.name.toInt))  {
//           println("Node "+ self.path.name +" Received EnoughGossips from " + sender().path.name)
         myNeighbours -= sender().path.name.toInt
          }
         self ! ReceivedGossip

      case Sum_Estimate_Converged =>

        if(myNeighbours.contains(sender().path.name.toInt)) {
//          println("Node " + self.path.name + " Received EnoughGossips from " + sender().path.name)
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
    var Nodes_List: ListBuffer[Int] = new ListBuffer[Int]
    def receive = {

      //Initiating Master by the Main Process
      case MasterInit(noOfNodes, topology, algorithm) =>
        println("Master Initiated")
        no_Of_Nodes = noOfNodes
        networktopology = topology.toLowerCase()
        currentalgorithm = algorithm.toLowerCase()


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
            else if( currentalgorithm == "push-sum") {

              Nodes(i)  ! PushSum_NodeInit(neighbours.toList)

            }
          }
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
            else if( currentalgorithm == "push-sum") {
              Nodes(i)  ! PushSum_NodeInit(neighbours.toList)
            }
          }


        }
        else if(networktopology == "3d")  {
          val cube_Side_rounded : Int = math.ceil(Math.cbrt(no_Of_Nodes)).toInt
          val NewNoofNodes: Int = Math.pow(cube_Side_rounded,3).toInt
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

                if(currentalgorithm == "gossip")  {
                  Nodes(current_Node) ! Gossip_NodeInit(neighbours.toList)
                }
                else if( currentalgorithm == "push-sum") {
                  Nodes(current_Node)  ! PushSum_NodeInit(neighbours.toList)
                }
              }
            }
          }
        }
         else if(networktopology == "imp3d") {

          val cube_Side_rounded : Int = math.ceil(Math.cbrt(no_Of_Nodes)).toInt
          val NewNoofNodes: Int = Math.pow(cube_Side_rounded,3).toInt
          for (i <- 0 until NewNoofNodes) {
            Nodes += MyActorSystem.actorOf(Props(new Node), name = i.toString)
          }

          for( k <- 0 until cube_Side_rounded)  {
            for (i <- 0 until cube_Side_rounded)  {
              for (j <- 0 until cube_Side_rounded)  {
                Nodes_List.clear()
                for (i <- 0 until NewNoofNodes) {
                  Nodes_List += i
                }

                val current_Node :  Int = Node_Number(i,j,k,cube_Side_rounded)
                neighbours.clear()
                if(!(i-1 < 0))  {
                  neighbours += Node_Number(i-1,j,k,cube_Side_rounded)
                  Nodes_List -= Node_Number(i-1,j,k,cube_Side_rounded)

                }
                Nodes_List -= Node_Number(i,j,k,cube_Side_rounded)

                if(!((i+1) > (cube_Side_rounded - 1)))  {
                  neighbours += Node_Number(i+1,j,k,cube_Side_rounded)
                  Nodes_List -= Node_Number(i+1,j,k,cube_Side_rounded)
                }
                if(!(j-1 < 0)) {
                  neighbours += Node_Number(i, j - 1, k, cube_Side_rounded)
                  Nodes_List -= Node_Number(i, j - 1, k, cube_Side_rounded)

                }
                if(!((j+1) > (cube_Side_rounded - 1))) {
                  neighbours += Node_Number(i, j + 1, k, cube_Side_rounded)
                  Nodes_List -= Node_Number(i, j + 1, k, cube_Side_rounded)
                }
                if(!(k-1 < 0))  {
                  neighbours += Node_Number(i,j,k-1,cube_Side_rounded)
                  Nodes_List -= Node_Number(i,j,k-1,cube_Side_rounded)
                }
                if(!((k+1) > (cube_Side_rounded - 1))) {
                  neighbours += Node_Number(i, j, k + 1, cube_Side_rounded)
                  Nodes_List -= Node_Number(i, j, k + 1, cube_Side_rounded)
                }

                neighbours += Random.nextInt(Nodes_List.size)
                if(currentalgorithm == "gossip")  {
                  Nodes(current_Node) ! Gossip_NodeInit(neighbours.toList)
//                  Thread.sleep(2000)
                }
                else if( currentalgorithm == "push-sum") {
                  Nodes(current_Node)  ! PushSum_NodeInit(neighbours.toList)
                }
              }
            }
          }

        }


        if(currentalgorithm == "gossip")  {

          Nodes(Random.nextInt(Nodes.size)) ! Gossip

        }
        else if( currentalgorithm == "push-sum") {

          Nodes(Random.nextInt(Nodes.size)) ! Start_PushSum

        }
        timeBeforeStartGossip =  System.currentTimeMillis()

      case EnoughGossips  =>
//        println("Node "+sender().path.name+" has completed")
        noOfcompletedNodes += 1
        if(noOfcompletedNodes == Nodes.length)  {
        timeAfterGossip = System.currentTimeMillis()
          println("Time taken to converge :" + (timeAfterGossip-timeBeforeStartGossip) + " milli seconds")
          context.system.shutdown()
        }
    }

  }
}