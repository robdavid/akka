/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.actor

import language.implicitConversions
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Failure
import java.util.regex.Pattern
import akka.pattern.ask
import akka.routing.MurmurHash
import akka.util.Helpers
import akka.util.Timeout
import akka.dispatch.ExecutionContexts

/**
 * An ActorSelection is a logical view of a section of an ActorSystem's tree of Actors,
 * allowing for broadcasting of messages to that section.
 */
@SerialVersionUID(1L)
abstract class ActorSelection extends Serializable {
  this: ScalaActorSelection ⇒

  protected[akka] val anchor: ActorRef

  protected val path: immutable.IndexedSeq[SelectionPathElement]

  @deprecated("use the two-arg variant (typically getSelf() as second arg)", "2.2")
  def tell(msg: Any): Unit = tell(msg, Actor.noSender)

  def tell(msg: Any, sender: ActorRef): Unit =
    ActorSelection.deliverSelection(anchor.asInstanceOf[InternalActorRef], sender,
      ActorSelectionMessage(msg, path))

  /**
   * Resolve the [[ActorRef]] matching this selection.
   * The result is returned as a Future that is completed with the [[ActorRef]]
   * if such an actor exists. It is completed with failure [[ActorNotFound]] if
   * no such actor exists or the identification didn't complete within the
   * supplied `timeout`.
   *
   * Under the hood it talks to the actor to verify its existence and acquire its
   * [[ActorRef]].
   */
  def resolveOne()(implicit timeout: Timeout): Future[ActorRef] = {
    implicit val ec = ExecutionContexts.sameThreadExecutionContext
    val p = Promise[ActorRef]()
    this.ask(Identify(None)) onComplete {
      case Success(ActorIdentity(_, Some(ref))) ⇒ p.success(ref)
      case _                                    ⇒ p.failure(ActorNotFound(this))
    }
    p.future
  }

  /**
   * Resolve the [[ActorRef]] matching this selection.
   * The result is returned as a Future that is completed with the [[ActorRef]]
   * if such an actor exists. It is completed with failure [[ActorNotFound]] if
   * no such actor exists or the identification didn't complete within the
   * supplied `timeout`.
   *
   * Under the hood it talks to the actor to verify its existence and acquire its
   * [[ActorRef]].
   */
  def resolveOne(timeout: FiniteDuration): Future[ActorRef] = resolveOne()(timeout)

  override def toString: String = {
    (new java.lang.StringBuilder).append("ActorSelection[").
      append(anchor.toString).
      append(path.mkString("/", "/", "")).
      append("]").toString
  }

  override def equals(obj: Any): Boolean = obj match {
    case s: ActorSelection ⇒ this.anchor == s.anchor && this.path == s.path
    case _                 ⇒ false
  }

  override lazy val hashCode: Int = {
    import MurmurHash._
    var h = startHash(anchor.##)
    h = extendHash(h, path.##, startMagicA, startMagicB)
    finalizeHash(h)
  }
}

/**
 * An ActorSelection is a logical view of a section of an ActorSystem's tree of Actors,
 * allowing for broadcasting of messages to that section.
 */
object ActorSelection {
  //This cast is safe because the self-type of ActorSelection requires that it mixes in ScalaActorSelection
  implicit def toScala(sel: ActorSelection): ScalaActorSelection = sel.asInstanceOf[ScalaActorSelection]

  /**
   * Construct an ActorSelection from the given string representing a path
   * relative to the given target. This operation has to create all the
   * matching magic, so it is preferable to cache its result if the
   * intention is to send messages frequently.
   */
  def apply(anchorRef: ActorRef, path: String): ActorSelection = apply(anchorRef, path.split("/+"))

  /**
   * Construct an ActorSelection from the given string representing a path
   * relative to the given target. This operation has to create all the
   * matching magic, so it is preferable to cache its result if the
   * intention is to send messages frequently.
   */
  def apply(anchorRef: ActorRef, elements: Iterable[String]): ActorSelection = {
    val compiled: immutable.IndexedSeq[SelectionPathElement] = elements.collect({
      case x if !x.isEmpty ⇒
        if ((x.indexOf('?') != -1) || (x.indexOf('*') != -1)) SelectChildPattern(x)
        else if (x == "..") SelectParent
        else SelectChildName(x)
    })(scala.collection.breakOut)
    new ActorSelection with ScalaActorSelection {
      override val anchor = anchorRef
      override val path = compiled
    }
  }

  /**
   * INTERNAL API
   * The receive logic for ActorSelectionMessage. The idea is to recursively descend as far as possible
   * with local refs and hand over to that “foreign” child when we encounter it.
   */
  private[akka] def deliverSelection(anchor: InternalActorRef, sender: ActorRef, sel: ActorSelectionMessage): Unit =
    if (sel.elements.isEmpty)
      anchor.tell(sel.msg, sender)
    else {
      val iter = sel.elements.iterator

      @tailrec def rec(ref: InternalActorRef): Unit = {
        ref match {
          case refWithCell: ActorRefWithCell ⇒
            iter.next() match {
              case SelectParent ⇒
                val parent = ref.getParent
                if (iter.isEmpty)
                  parent.tell(sel.msg, sender)
                else
                  rec(parent)
              case SelectChildName(name) ⇒
                val child = refWithCell.getSingleChild(name)
                if (child == Nobody)
                  sel.identifyRequest foreach { x ⇒ sender ! ActorIdentity(x.messageId, None) }
                else if (iter.isEmpty)
                  child.tell(sel.msg, sender)
                else
                  rec(child)
              case p: SelectChildPattern ⇒
                // fan-out when there is a wildcard
                val chldr = refWithCell.children
                if (iter.isEmpty)
                  for (c ← chldr if p.pattern.matcher(c.path.name).matches)
                    c.tell(sel.msg, sender)
                else {
                  val m = sel.copy(elements = iter.toVector)
                  for (c ← chldr if p.pattern.matcher(c.path.name).matches)
                    deliverSelection(c.asInstanceOf[InternalActorRef], sender, m)
                }
            }

          case _ ⇒
            // foreign ref, continue by sending ActorSelectionMessage to it with remaining elements
            ref.tell(sel.copy(elements = iter.toVector), sender)
        }
      }

      rec(anchor)
    }
}

/**
 * Contains the Scala API (!-method) for ActorSelections) which provides automatic tracking of the sender,
 * as per the usual implicit ActorRef pattern.
 */
trait ScalaActorSelection {
  this: ActorSelection ⇒

  def !(msg: Any)(implicit sender: ActorRef = Actor.noSender) = tell(msg, sender)
}

/**
 * INTERNAL API
 * ActorRefFactory.actorSelection returns a ActorSelection which sends these
 * nested path descriptions whenever using ! on them, the idea being that the
 * message is delivered by traversing the various actor paths involved.
 */
@SerialVersionUID(1L)
private[akka] case class ActorSelectionMessage(msg: Any, elements: immutable.Iterable[SelectionPathElement])
  extends AutoReceivedMessage with PossiblyHarmful {

  def identifyRequest: Option[Identify] = msg match {
    case x: Identify ⇒ Some(x)
    case _           ⇒ None
  }
}

/**
 * INTERNAL API
 */
@SerialVersionUID(1L)
private[akka] sealed trait SelectionPathElement

/**
 * INTERNAL API
 */
@SerialVersionUID(2L)
private[akka] case class SelectChildName(name: String) extends SelectionPathElement {
  override def toString: String = name
}

/**
 * INTERNAL API
 */
@SerialVersionUID(2L)
private[akka] case class SelectChildPattern(patternStr: String) extends SelectionPathElement {
  val pattern: Pattern = Helpers.makePattern(patternStr)
  override def toString: String = patternStr
}

/**
 * INTERNAL API
 */
@SerialVersionUID(2L)
private[akka] case object SelectParent extends SelectionPathElement {
  override def toString: String = ".."
}

/**
 * When [[ActorSelection#resolveOne]] can't identify the actor the
 * `Future` is completed with this failure.
 */
@SerialVersionUID(1L)
case class ActorNotFound(selection: ActorSelection) extends RuntimeException("Actor not found for: " + selection)

