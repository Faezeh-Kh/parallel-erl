package org.eclipse.epsilon.erl.execute.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.function.CheckedEolRunnable;

public interface ErlExecutorService extends ExecutorService {
	
	ErlExecutionStatus getExecutionStatus();
	
	default void awaitCompletion() throws EolRuntimeException {
		final ErlExecutionStatus status = getExecutionStatus();
		
		Thread termWait = new Thread(() -> {
			shutdown();
			try {
				awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				status.completeSuccessfully();
			}
			catch (InterruptedException ie) {
				status.setException(ie);
			}
		});
		termWait.setName(getClass().getSimpleName()+"-AwaitTermination");
		termWait.start();

		Exception exception = status.waitForCompletion();
		
		if (exception != null) {
			termWait.interrupt();
			shutdownNow();
			EolRuntimeException.propagateDetailed(exception);
		}
	}
	
	/*
	 * Hack for allowing execution of methods which throw exceptions! Lambdas will call this instead of the regular execute().
	 */
	default void execute(CheckedEolRunnable task) throws EolRuntimeException {
		//No performance penalty in upcasting!
		execute((Runnable)task);
	}
}
