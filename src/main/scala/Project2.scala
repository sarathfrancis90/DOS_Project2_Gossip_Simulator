import akka.actor._
import com.sun.javafx.tk.quantum.MasterTimer
import com.sun.org.apache.xml.internal.security.algorithms.JCEMapper.Algorithm

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.collection.mutable._

/**
 * Created by sarathfrancis90 on 9/17/15.
 */
object Project2 {

  sealed trait Rumor
  case class  MasterInit(noOfNodes:Int,topology:String,algorithm:String) extends Rumor
  case class  Gossip_NodeInit (neighbourlist:List[Int],noOfNodes:Int) extends Rumor
  case object  Gossip extends Rumor
  case object  ReceivedGossip extends Rumor
  case class  EnoughGossips() extends  Rumor
  // add the function of pushsum(start from 09/25/2015)
  case class Pushsum
  
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

  class Node extends Actor with ActorLogging {

    var myNeighboursListssss : ListBuffer[Int] = _
    var No_of_Nodes: Int =_
    var Gossip_Count: Int = 0
    var MasterRef :ActorRef = _
    var neighbourRef:ActorRef = _


    def receive = {

      case Gossip_NodeInit(neighbourList,noOfNodes) =>
       //log.info("Node Initiated")
        println("Node no. " + self.path.name + " initiated")
        MasterRef = sender()
        myNeighboursListssss = neighbourList.to[ListBuffer]
        No_of_Nodes = noOfNodes

        printf("my neighbors are ")
        myNeighboursListssss.foreach(printf("%d  ",_))
        println(" ")


      case Gossip  =>
        Gossip_Count+=1
        println("Gossip received at " + self.path.name)
        printf("my neighbors are ")
        myNeighboursListssss.foreach(printf("%d  ",_))
        println(" ")
        if(Gossip_Count == 10) {
//          for(myNeighbour <-myNeighbours) {
//            Nodes(myNeighbour) ! EnoughGossips
//          }
//          MasterRef ! EnoughGossips
          log.info("I am done")
        }
        else {
          Thread.sleep(1000)
          self ! ReceivedGossip
        }


      case ReceivedGossip =>
        println("ReceivedGossip received at " + self.path.name)
        printf("my neighbors are ")
        myNeighboursListssss.foreach(printf("%d  ",_))
        Thread.sleep(1000)

        if(myNeighboursListssss.length != 0){
          var aRandomNumber = Random.nextInt(myNeighboursListssss.size)
          var randomNeighbour: Int = myNeighboursListssss(aRandomNumber)

          if(self.path.name == Nodes(randomNeighbour).path.name) {
            printf ("i am %s\n", self.path.name)
            printf("my neighbors are ")
            myNeighboursListssss.foreach(printf("%d  ",_))
            println("\nsending to self")
            printf("i generated random number %d\n", aRandomNumber)
            printf("i am going to send to %d\n", randomNeighbour)
            printf("that neighbors name is %s\n", Nodes(randomNeighbour).path.name)
            Thread.sleep(3000)
          }
          Nodes(randomNeighbour) !  Gossip
        }
        else
          println("hi")

      case EnoughGossips  =>
           log.info("Received EnoughGossips")
         if(myNeighboursListssss.contains(sender().path.name.toInt))  {
         // myNeighbours -= sender().path.name.toInt
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

        for (i <- 0 until no_Of_Nodes) {
          Nodes += MyActorSystem.actorOf(Props(new Node), name = s"worker$i")

        }
        //println(no_Of_Nodes +" " +networktopology +" "+ currentalgorithm)

        if (networktopology == "full") {

          for (i <- 0 until no_Of_Nodes) {
            neighbours.clear()

            for (j <- 0 until no_Of_Nodes) {
              if (j != i)
                neighbours += j
            }

//            printf ("sending int to %s with neighbors ", Nodes(i).path.name)
//            neighbours.foreach(printf("--%d ", _))
//            printf("\n")
            Thread.sleep(2000)
            Nodes(i) ! Gossip_NodeInit(neighbours.toList, no_Of_Nodes)
          }
          //println(Nodes.size)
        }
        else if (networktopology =="line") {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)


        }
        else if(networktopology == "3D")  {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)

        }
         else if(networktopology == "imp3D") {

          //Nodes(i) ! Gossip_NodeInit(neighbourList, noOfNodes)

        }

        Nodes(Random.nextInt(Nodes.size)) ! Gossip
        timeBeforeStartGossip =  System.currentTimeMillis()


      case EnoughGossips  =>
          log.info(sender().path.name.toString +" has completed")
        noOfcompletedNodes += 1
        if(noOfcompletedNodes == Nodes.length)  {

        timeAfterGossip = System.currentTimeMillis()
          println("Time taken to converge :" + (timeAfterGossip-timeBeforeStartGossip) )

          context.system.shutdown()
        }


    }

  }
  
  // the implementation of the second part function Pushsum function(start from 09/25/2015)
  class pushsumNode(neighbor:neighbourList.to[ListBuffer], id:Int, Stap:Array[pushsumNode], boss: Actor ) extends Actor {
  def act{
    var neighbor=neighbourList.to[ListBuffer]
    var neighbor2=neighbor
    var s:Double = id.toDouble
    var w:Double = 1.0
    var r0:Double = id.toDouble
    var converge_count:Int = 0
    loop{
      react{
        case "start" =>
         var t = neighbor2(Random.nextInt(neighbor2.length))
          s=s/2 
          w=w/2
          Stap(t) ! (s, w)    
         // println("from"+id+"to"+t+"with s="+s+";  w="+w)
        case "close"=> exit()
        case (sm:Double, wm:Double) =>
          s = (sm+s)/2
          w = (wm+w)/2
          var tt = neighbor2(Random.nextInt(neighbor2.length))         
         // println("from  "+id+" to "+tt+" with s= "+s+"  ;w= "+w)
          if (math.abs(s/w-r0)<1e-10) converge_count+=1
          else converge_count=0
         // println("r;diff="+(s/w)+" ; "+(s/w-r0)+" count=  "+converge_count)
          Stap(tt) ! (s, w)
          r0 = s/w
          if (converge_count==3){
           
            //for (i <- 0 until neighbor2.length) Stap(neighbor2(i)) ! im_over(id)
            boss ! (im_over(id),r0)
            println("Actor No."+id.toString+" is over by count")
            exit()
          }
          }       
       }
    }
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}