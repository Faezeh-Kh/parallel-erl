package org.eclipse.epsilon.erl.execute.concurrent;

import java.util.*;
import java.util.function.Supplier;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;

/*
 * Thread-local storage which retains values for all threads.
 * 
 * @see https://dzone.com/articles/how-threadlocal-implemented
 * @see https://stackoverflow.com/questions/2795447/is-there-no-way-to-iterate-over-or-copy-all-the-values-of-a-java-threadlocal
 * 
 * @author Sina Madani
 */
public class PersistentThreadLocal<T> extends ThreadLocal<T> {

	protected final Map<Thread, T> allValues;
	protected final Supplier<? extends T> valueGetter;
	
	public PersistentThreadLocal(Supplier<? extends T> initialValue) {
		this(0, initialValue);
	}
	
	public PersistentThreadLocal(int numThreads, Supplier<? extends T> initialValue) {
		numThreads = numThreads > 0 ? numThreads : ConcurrencyUtils.DEFAULT_PARALLELISM;
		//Don't use a WeakHashMap because we want to persist the values!
		allValues = ConcurrencyUtils.concurrentMap(numThreads, numThreads);
		valueGetter = initialValue;
	}
	
	@Override
	protected T initialValue() {
		T value = valueGetter != null ? valueGetter.get() : super.initialValue();
		allValues.put(Thread.currentThread(), value);
		return value;
	}
	
	@Override
	public void set(T value) {
		super.set(value);
		allValues.put(Thread.currentThread(), value);
	}

	@Override
	public void remove() {
		super.remove();
		allValues.remove(Thread.currentThread());
	}
	
	public Collection<T> getAll() {
		return allValues.values();
	}
	
	public void removeAll() {
		allValues.clear();
	}
}
