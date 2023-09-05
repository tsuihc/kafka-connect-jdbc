package io.confluent.connect.jdbc.segment.queue;

import io.confluent.connect.jdbc.segment.model.Segment;
import lombok.NonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MemorySegmentQueue extends SegmentQueue {

  private final BlockingQueue<Segment> innerQueue;

  public MemorySegmentQueue(int size) {
    super(size);
    this.innerQueue = new ArrayBlockingQueue<>(size);
  }

  @Override
  public boolean add(@NonNull Segment segment) {
    return innerQueue.add(segment);
  }

  @Override
  public boolean offer(@NonNull Segment segment) {
    return innerQueue.offer(segment);
  }

  @Override
  public Segment remove() {
    return innerQueue.remove();
  }

  @Override
  public Segment poll() {
    return innerQueue.poll();
  }

  @Override
  public Segment element() {
    return innerQueue.element();
  }

  @Override
  public Segment peek() {
    return innerQueue.peek();
  }

  @Override
  public void put(@NonNull Segment segment) throws InterruptedException {
    innerQueue.put(segment);
  }

  @Override
  public boolean offer(
    Segment segment,
    long timeout,
    @NonNull TimeUnit unit
  ) throws InterruptedException {
    return innerQueue.offer(segment, timeout, unit);
  }

  @Override
  public Segment take() throws InterruptedException {
    return innerQueue.take();
  }

  @Override
  public Segment poll(
    long timeout,
    @NonNull TimeUnit unit
  ) throws InterruptedException {
    return innerQueue.poll(timeout, unit);
  }

  @Override
  public int remainingCapacity() {
    return innerQueue.remainingCapacity();
  }

  @Override
  public boolean remove(Object o) {
    return innerQueue.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return innerQueue.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Segment> c) {
    return innerQueue.addAll(c);
  }

  @Override
  public boolean removeAll(@NonNull Collection<?> c) {
    return innerQueue.removeAll(c);
  }

  @Override
  public boolean retainAll(@NonNull Collection<?> c) {
    return innerQueue.retainAll(c);
  }

  @Override
  public void clear() {
    innerQueue.clear();
  }

  @Override
  public int size() {
    return innerQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return innerQueue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return innerQueue.contains(o);
  }

  @Override
  public Iterator<Segment> iterator() {
    return innerQueue.iterator();
  }

  @Override
  public Object[] toArray() {
    return innerQueue.toArray();
  }

  @Override
  public <T> T[] toArray(@NonNull T[] a) {
    return innerQueue.toArray(a);
  }

  @Override
  public int drainTo(@NonNull Collection<? super Segment> c) {
    return innerQueue.drainTo(c);
  }

  @Override
  public int drainTo(
    @NonNull Collection<? super Segment> c,
    int maxElements
  ) {
    return innerQueue.drainTo(c, maxElements);
  }
}
