import akka.actor._
import com.sun.javafx.tk.quantum.MasterTimer
import com.sun.org.apache.xml.internal.security.algorithms.JCEMapper.Algorithm

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by sarathfrancis90 on 9/17/15.
 */
object Project2 {

  sealed trait Rumor
  case class  MasterInit(noOfNodes:Int,topology:String,algorithm:String) extends Rumor
  case class  Gossip_NodeInit (neighbourlist:List[Int],noOfNodes:Int) extends Rumor
  case class  Gossip() extends Rumor
  case class  ReceivedGossip() extends Rumor

  //variable to store no of nodes
 // var NoOfNodes:Int = _
  //list buffer to store nodes
  val Nodes = new ListBuffer[ActorRef]()
  //variable to store Actor System
  var System : ActorSystem = _
  //NeighbourList
  //val NeighbourList = new ListBuffer[ActorRef]()


  def main (args: Array[String])
  {

    var NoOfNodes:Int = args(0).toInt
    val Topology:String  = args(1)
    val Algorithm:String = args(2)

    //Creating Actor System
    System = ActorSystem ("GossipSimulator")

    //Creating MasterneighbourList.toList
    val master = System.actorOf(Props(new Master), name ="Master")

    //Initiating Master
    master ! MasterInit(NoOfNodes,Topology,Algorithm)

  }

  class Node extends Actor with ActorLogging {

    var myNeighbours :List[Int] = _
    var No_of_Nodes: Int =_
    var Gossip_Count: Int =_

    def receive = {
      case Gossip_NodeInit(neighbourList,noOfNodes) =>
        log.info("Node Initiated")
        myNeighbours = neighbourList
        No_of_Nodes = noOfNodes

      case Gossip  =>
        Gossip_Count+=1
        self ! ReceivedGossip

      case ReceivedGossip =>
        if(Gossip_Count == 10) {
          
        }
        if(!myNeighbours.isEmpty){
          Nodes(myNeighbours(Random.nextInt(myNeighbours.length))) !Gossip
        }

    }
  }

  class Master extends Actor with  ActorLogging {

    var neighbours = new ListBuffer[Int]
    var neighbourList :List[Int] =_

    def receive = {

      //Initiating Master by the Main Process
      case MasterInit(noOfNodes, topology, algorithm) =>

        for (i <- 0 until noOfNodes - 1) {
          Nodes += System.actorOf(Props(new Node), name = "Node"+(i + 1).toString)

        }
        if (topology == "full") {
          for (i <- 0 until noOfNodes - 1) {
            for (j <- 0 until noOfNodes - 1) {
              if (j != i) {
                neighbours += j
              }
            }
            neighbourList = neighbours.toList
            neighbours.clear()
            Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)
          }
        }
        if (topology =="line") {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)


        }
        if(topology == "3D")  {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)

        }
        if(topology == "imp3D") {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)

        }
        Nodes(Random.nextInt(noOfNodes)) ! Gossip
    }
  }
}