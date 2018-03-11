package org.eclipse.epsilon.erl.execute.context;

import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.erl.IErlModule;

public interface IErlContext extends IEolContext {

	/*
	 * Casts the IModule to IErlModule
	 * @see org.eclipse.epsilon.eol.execute.context.IEolContext#getModule()
	 */
	@Override
	default IErlModule getModule() {
		return (IErlModule) ((IEolContext)this).getModule();
	}
}
