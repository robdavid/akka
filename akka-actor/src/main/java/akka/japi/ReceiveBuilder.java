/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.japi;

import scala.runtime.BoxedUnit;

/**
 * Used for building a partial function receive in Java 8 with lambdas.
 *
 * There are both a match on type only, and a match on type and predicate.
 *
 * Inside an actor you can use it like this to define your receive method:
 * <pre>
 * {@code
 * @Override
 * public PartialFunction<Object, BoxedUnit> receive() {
 *   return ReceiveBuilder.
 *     match(Double.class, d -> {
 *       sender().tell(d, self());
 *     }).
 *     orMatch(Integer.class, i -> {
 *       sender().tell(i, self());
 *     }).
 *     orMatch(String.class, s -> s.startsWith("foo"), s -> {
 *       sender().tell(s, self());
 *     });
 * }
 * </pre>
 *
 */
public final class ReceiveBuilder {

  private ReceiveBuilder() {
  }

  private static final Receive receive = new Receive();

  public static <P> Receive match(final Class<P> type, Apply<P> apply) {
    return receive.orMatch(type, apply);
  }

  public static <P> Receive match(final Class<P> type, TypedPredicate<P> predicate, Apply<P> apply) {
    return receive.orMatch(type, predicate, apply);
  }

  public static class Receive extends AbstractPartialFunction<Object, BoxedUnit> {
    private static final Option<BoxedUnit> some = new Option.Some<BoxedUnit>(null);
    private static final Option<BoxedUnit> none = Option.none();

    private Receive() {
    }

    protected boolean parentApplied(Object o, boolean onlyCheck) {
      return false;
    }

    protected boolean applied(Object o, boolean onlyCheck) {
      return false;
    }

    @Override
    public Option<BoxedUnit> apply(Object o, boolean onlyCheck) {
      if (parentApplied(o, onlyCheck) || applied(o, onlyCheck))
        return some;
      else {
        return none;
      }
    }

    public <P> Receive orMatch(final Class<P> type, Apply<P> apply) {
      return new MatchStatement<P>(this, new Predicate<P>() {
        @Override
        public boolean defined(Object o) {
          return type.isInstance(o);
        }
      }, apply);
    }

    public <P> Receive orMatch(final Class<P> type, final TypedPredicate<P> predicate, Apply<P> apply) {
      return new MatchStatement<P>(this,new Predicate<P>() {
        @Override
        public boolean defined(Object o) {
          if (!type.isInstance(o))
            return false;
          else {
            @SuppressWarnings("unchecked")
            P p = (P) o;
            return predicate.defined(p);
          }
        }
      }, apply);
    }
  }

  public static interface Predicate<P> {
    public abstract boolean defined(Object o);
  }

  public static interface TypedPredicate<P> {
    public abstract boolean defined(P p);
  }

  public static interface Apply<P> {
    public abstract void apply(P p);
  }

  private static final class MatchStatement<P> extends Receive {
    private final Receive parent;
    private final Predicate<P> predicate;
    private final Apply<P> apply;

    private MatchStatement(Receive parent, Predicate<P> predicate, Apply<P> apply) {
      this.parent = parent;
      this.predicate = predicate;
      this.apply = apply;
    }

    @Override
    public boolean applied(Object o, boolean onlyCheck) {
      boolean defined = predicate.defined(o);
      if (!onlyCheck && defined) {
        @SuppressWarnings("unchecked")
        P p = (P) o;
        apply.apply(p);
      }
      return defined;
    }

    @Override
    public boolean parentApplied(Object o, boolean onlyCheck) {
      return (parent.parentApplied(o, onlyCheck)|| parent.applied(o, onlyCheck));
    }
  }
}
