package lockfree

import scala.annotation.tailrec

class SortedList extends AbstractSortedList {

  // The sentinel node at the head.
  private val _head: Node = createNode(0, None, isHead = true)

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.next

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Node, Option[Node]) = {
    def findRec(prev : Node, curr : Option[Node]) : (Node, Option[Node]) = {
      (prev, curr) match {
        case (last, None) => (last, None);
        case (_, Some(node)) =>
          if(node.deleted) {
            prev.atomicState.compareAndSet((curr, false), (node.next, false))
            findNodeWithPrev(pred)
          }else if(pred(node.value)) (prev, curr)
          else findRec(node, node.next)
      }
    }
    findRec(_head, _head.next)
  }

  // Insert an element in the list.
  def insert(e: Int): Unit = {
    val (pred, next) = findNodeWithPrev(_ >= e);
    val n = createNode(e, next)
    if(!pred.atomicState.compareAndSet((next, false), (Some(n), false))) insert(e)
  }

  // Checks if the list contains an element.
  def contains(e: Int): Boolean = {
    findNodeWithPrev(_ == e)._2 match {
      case None => false
      case _ => true;
    }
  }

  // Delete an element from the list.
  // Should only delete one element when multiple occurences are present.
  def delete(e: Int): Boolean = {
    val (pred, next) = findNodeWithPrev(_ == e)
    next match{
      case None => false
      case Some(x) => if(x.mark) true else delete(e)
    }
  }
}
