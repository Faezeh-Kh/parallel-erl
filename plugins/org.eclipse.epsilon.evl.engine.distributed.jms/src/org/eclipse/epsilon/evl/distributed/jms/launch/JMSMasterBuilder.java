/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.launch;

import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
@SuppressWarnings("unchecked")
public class JMSMasterBuilder<J extends JMSMasterRunner, B extends JMSMasterBuilder<J, B>> extends DistributedEvlRunConfiguration.Builder<J, B> {
	
	public String brokerHost = "tcp://localhost:61616";
	public int expectedWorkers;
	
	public JMSMasterBuilder() {
		this((Class<J>) JMSMasterRunner.class);
	}
	public JMSMasterBuilder(Class<J> runConfigClass) {
		super(runConfigClass);
	}
	
	public B withHost(String broker) {
		this.brokerHost = broker;
		return (B) this;
	}
	
	public B withWorkers(int expected) {
		this.expectedWorkers = expected;
		return (B) this;
	}
}
