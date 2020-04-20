/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op:Operation => root ! op
    case GC => {
      val newRoot = createRoot;
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
   }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op:Operation => pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
      pendingQueue.foreach(newRoot ! _) //foreach preserves order of a queue (same as dequeueing)
      root ! PoisonPill //Will also stop all of its children
      pendingQueue = Queue.empty
      root = newRoot;
      context.become(normal)
    //Ignore GC messages here
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def goDownTo(elem : Int) : Position = if(elem < this.elem) Left else Right
  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert  (requester, id, elem) =>
      if(elem == this.elem && !removed){
        requester ! OperationFinished(id)
      }else{
        val nextPos = goDownTo(elem)
        
        subtrees get nextPos match{
          case Some(node) => node ! Insert(requester, id, elem)
          case None => {
              val newActorSubtree = (nextPos, context.actorOf(BinaryTreeNode.props(elem, false)))
              subtrees = subtrees + newActorSubtree
              requester ! OperationFinished(id);
          }
        }
      }
    case Contains(requester, id, elem) =>
      if(elem == this.elem && !removed)
        requester ! ContainsResult(id, true)
      else{
        //Need to search subtrees
        subtrees get goDownTo(elem) match{
          case Some(node) => node ! Contains(requester, id, elem)
          case None => requester ! ContainsResult(id, false)
        }
      }
        
    case Remove  (requester, id, elem) => 
      if(elem == this.elem && !removed){
        removed = true
        requester ! OperationFinished(id)
      }else{
        subtrees get goDownTo(elem) match{
          case Some(node) => node ! Remove(requester, id, elem)
          case None => requester ! OperationFinished(id) // (elem isn't in the tree)
        }
      }

    case CopyTo(newRoot) =>
      //We are already done, nothing to do
      if(removed && subtrees.isEmpty) context.parent ! CopyFinished
      else{
        if(!removed) newRoot ! Insert(self, elem, elem)
        subtrees.values foreach(_ ! CopyTo(newRoot)) //Copy subtrees elems
        //val insertConfirmed = if(removed) true else false, hence we can simply pass removed
        context.become(copying(subtrees.values.toSet, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    //To catch the insert of this node into the new tree beeing finished
    case OperationFinished(_) => {
      if(expected.isEmpty) context.parent ! CopyFinished
      else context.become(copying(expected, true))
    }
    
    case CopyFinished => {
      val newExp = expected-sender
      if(insertConfirmed && newExp.isEmpty){
        context.parent ! CopyFinished
      }else{
        context.become(copying(newExp, insertConfirmed))
      }
    }
  }
}