/*******************************************************************************
 * Copyright (c) 2012 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 *     Sina Madani - refactoring and parallel implementation
 ******************************************************************************/
package org.eclipse.epsilon.epl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.TokenStream;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.common.parse.AST;
import org.eclipse.epsilon.common.parse.EpsilonParser;
import org.eclipse.epsilon.common.util.AstUtil;
import org.eclipse.epsilon.eol.dom.ExecutableBlock;
import org.eclipse.epsilon.eol.dom.Import;
import org.eclipse.epsilon.eol.exceptions.EolIllegalReturnException;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.Return;
import org.eclipse.epsilon.eol.execute.context.Frame;
import org.eclipse.epsilon.eol.execute.context.FrameType;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.epl.dom.Cardinality;
import org.eclipse.epsilon.epl.dom.Domain;
import org.eclipse.epsilon.epl.dom.NoMatch;
import org.eclipse.epsilon.epl.dom.Pattern;
import org.eclipse.epsilon.epl.dom.Role;
import org.eclipse.epsilon.epl.execute.PatternMatchModel;
import org.eclipse.epsilon.epl.execute.PatternMatch;
import org.eclipse.epsilon.epl.parse.EplLexer;
import org.eclipse.epsilon.epl.parse.EplParser;
import org.eclipse.epsilon.erl.ErlModule;

/**
 * 
 * @since 1.6
 * @author Sina Madani
 */
public abstract class AbstractEplModule extends ErlModule implements IEplModule {
	
	public static final int INFINITE = -1;
	
	protected List<Pattern> patterns;
	protected final ArrayList<Pattern> declaredPatterns = new ArrayList<>(0);
	protected boolean repeatWhileMatchesFound = false;
	protected int maxLoops = INFINITE;
	protected String patternMatchModelName = "P";
	
	@Override
	protected Lexer createLexer(ANTLRInputStream inputStream) {
		return new EplLexer(inputStream);
	}

	@Override
	public EpsilonParser createParser(TokenStream tokenStream) {
		return new EplParser(tokenStream);
	}

	@Override
	public String getMainRule() {
		return "eplModule";
	}
	
	@Override
	public HashMap<String, Class<?>> getImportConfiguration() {
		HashMap<String, Class<?>> importConfiguration = super.getImportConfiguration();
		importConfiguration.put("epl", getClass());
		return importConfiguration;
	}
	
	@Override
	public ModuleElement adapt(AST cst, ModuleElement parentAst) {
		switch (cst.getType()) {
			case EplParser.PATTERN: return new Pattern();
			case EplParser.CARDINALITY: return new Cardinality();
			case EplParser.DOMAIN: return new Domain();
			case EplParser.ROLE: return new Role();
			case EplParser.GUARD:
			case EplParser.ACTIVE:
			case EplParser.OPTIONAL:
			case EplParser.MATCH:
				return new ExecutableBlock<Boolean>(Boolean.class);
			case EplParser.ONMATCH:
			case EplParser.NOMATCH:
			case EplParser.DO:
				return new ExecutableBlock<Void>(Void.class);
		}
		return super.adapt(cst, parentAst);
	}
	
	@Override
	public void build(AST cst, IModule module) {
		super.build(cst, module);
		
		List<AST> patternAsts = AstUtil.getChildren(cst, EplParser.PATTERN);
		declaredPatterns.ensureCapacity(patternAsts.size());
		for (AST patternAst : patternAsts) {
			declaredPatterns.add((Pattern) module.createAst(patternAst, this));
		}
	}
	
	@Override
	public PatternMatchModel executeImpl() throws EolRuntimeException {
		prepareExecution();
		PatternMatchModel matchModel = matchPatterns();
		postExecution();
		return matchModel;
	}
	
	@Override
	public List<Pattern> getDeclaredPatterns() {
		return declaredPatterns;
	}
	
	@Override
	public List<Pattern> getPatterns() {
		if (patterns == null) {
			patterns = new ArrayList<>();
			for (Import import_ : imports) {
				if (import_.isLoaded() && (import_.getModule() instanceof IEplModule)) {
					IEplModule module = (IEplModule) import_.getModule();
					patterns.addAll(module.getPatterns());
				}
			}
			patterns.addAll(declaredPatterns);
		}
		return patterns;
	}
	
	@Override
	public int getMaxLoops() {
		return maxLoops;
	}
	
	@Override
	public void setMaxLoops(int maxLoops) {
		this.maxLoops = maxLoops;
	}
	
	@Override
	public boolean isRepeatWhileMatches() {
		return repeatWhileMatchesFound;
	}
	
	@Override
	public void setRepeatWhileMatches(boolean repeatWhileMatches) {
		this.repeatWhileMatchesFound = repeatWhileMatches;
	}

	@Override
	protected int getPreBlockTokenType() {
		return EplParser.PRE;
	}

	@Override
	protected int getPostBlockTokenType() {
		return EplParser.POST;
	}
	
	@Override
	public String getPatternMatchModelName() {
		return patternMatchModelName;
	}
	
	@Override
	public void setPatternMatchModelName(String patternMatchModelName) {
		this.patternMatchModelName = patternMatchModelName;
	}
	
	@Override
	public final PatternMatchModel matchPatterns() throws EolRuntimeException {
		PatternMatchModel matchModel = null;
		
		try {
			int loops = 0;
			do {
				matchModel = createModel();
				preMatch(matchModel);
				
				for (int level = 0; level <= getMaximumLevel(); level++) {
					Set<PatternMatch> currentMatches = matchPatterns(level, matchModel);
					postProcessMatches(level, currentMatches);
				}
				
				++loops;
			}
			while (repeatWhileMatchesFound && (		// If in iterative mode, terminate when:
				!matchModel.allContents().isEmpty()	// 	the match model is empty
				|| loops == maxLoops				// 	or when the maximum number of specified iterations is reached.
			));
		}
		catch (Exception ex) {
			EolRuntimeException.propagate(ex);
		}
		
		return matchModel;
	}

	/**
	 * This method provides the main high-level execution logic for EPL.
	 * The idea is that for each role in the pattern, appropriate bindings are made to
	 * the role and executed. Then the match block (and subsequently onMatch) is executed
	 * for the role and a PatternMatch is created where applicable. <br/>
	 * 
	 * In essence, this method can be thought of as the executor of all roles in the pattern,
	 * and therefore acts as a bridge between the high-level methods (e.g.
	 * {@link #matchPatterns(int, PatternMatchModel)}) and the low-level ones (e.g. 
	 * {@link #getRoleInstances(Role, String)}). <br/>
	 * 
	 * Implementation-wise, this method delegates the main execution logic to
	 * {@link #matchCombination(Collection, Pattern)}, and so the sole responsibility
	 * of this method is to loop through the combinations returned by {@link #getCandidates(Pattern)}
	 * and collect all the results. Subclasses may override this method to alter the type of collection
	 * returned and/or alter the looping mechanism.
	 */
	@Override
	public Collection<PatternMatch> match(Pattern pattern) throws EolRuntimeException {
		ArrayList<PatternMatch> patternMatches = new ArrayList<>();
		
		for (Iterator<? extends Collection<? extends Iterable<?>>> candidates = getCandidates(pattern); candidates.hasNext();) {
			matchCombination(candidates.next(), pattern).ifPresent(patternMatches::add);
		}

		return patternMatches;
	}
	
	/**
	 * Executes the match, onmatch and/or nomatch blocks.
	 * 
	 * @param combination The values to use for role bindings.
	 * @param pattern
	 * @return A {@linkplain PatternMatch} if the criteria was met, empty otherwise.
	 * @throws EolRuntimeException
	 */
	protected final Optional<PatternMatch> matchCombination(Collection<? extends Iterable<?>> combination, Pattern pattern) throws EolRuntimeException {
		Optional<PatternMatch> result;
		Frame frame = context.getFrameStack().enterLocal(FrameType.PROTECTED, pattern);
		
		if (pattern.getMatch() != null || pattern.getNoMatch() != null || pattern.getOnMatch() != null) {
			putRoleBindingsIntoFrame(pattern.getRoles(), combination, frame);
		}

		if (getMatchResult(pattern)) { 
			context.getExecutorFactory().execute(pattern.getOnMatch(), context);
			result = Optional.of(createPatternMatch(pattern, combination));
		}
		else {
			context.getExecutorFactory().execute(pattern.getNoMatch(), context);
			result = Optional.empty();
		}
		
		context.getFrameStack().leaveLocal(pattern);
		return result;
	}
	
	/**
	 * PatternMatchModel factory method.
	 */
	protected PatternMatchModel createModel() throws EolRuntimeException {
		return new PatternMatchModel();
	}
	
	/**
	 * Pre-processes the model.
	 */
	protected void preMatch(PatternMatchModel model) throws EolRuntimeException {
		if (getMaximumLevel() > 0) {
			model.setName(getPatternMatchModelName());
			context.getModelRepository().addModel(model);
		}
		model.setPatterns(getPatterns());
	}
	
	/**
	 * Adds all matches returned by {@link IEplModule#match()} to the match model
	 * for all patterns at the specified level.
	 * @return The set of pattern matches added to the model.
	 */
	protected Set<PatternMatch> matchPatterns(int level, PatternMatchModel model) throws EolRuntimeException {
		for (Pattern pattern : getPatterns()) {
			if (pattern.getLevel() == level) {
				for (PatternMatch match : match(pattern)) {
					model.addMatch(match);
				}
			}
		}
		return model.getMatches();
	}
	
	/**
	 * Executes the do block for all matched patterns at the specified level.
	 * To control the execution of the block itself, subclasses can override the
	 * {@link #executeDoBlock(ExecutableBlock, Map)} method.
	 */
	protected final void postProcessMatches(int level, Collection<PatternMatch> matches) throws EolRuntimeException {
		for (PatternMatch match : matches) {
			Pattern pattern = match.getPattern();
			ExecutableBlock<Void> do_;
			if (pattern.getLevel() == level && (do_ = pattern.getDo()) != null) {
				executeDoBlock(do_, match.getRoleBindings());
			}
		}
	}
	
	/**
	 * Executes the do block with the specified variables.
	 * @param doBlock The block to execute.
	 * @param roleBindings The effective collection of variables.
	 * @throws EolRuntimeException
	 */
	protected void executeDoBlock(ExecutableBlock<Void> doBlock, Map<String, Object> roleBindings) throws EolRuntimeException {
		Frame frame = context.getFrameStack().enterLocal(FrameType.UNPROTECTED, doBlock);
		
		roleBindings.entrySet()
			.stream()
			.map(Variable::createReadOnlyVariable)
			.forEach(frame::put);

		context.getExecutorFactory().execute(doBlock, context);
		context.getFrameStack().leaveLocal(doBlock);
	}
	
	/**
	 * Gets the result of the match block for the specified pattern.
	 * 
	 * @param pattern the pattern being executed
	 * @param context the context
	 * @return the result of executing the match block or <code>true</code>
	 * if the pattern does not define a match block.
	 * @throws EolRuntimeException if the result is not a boolean.
	 */
	protected final boolean getMatchResult(Pattern pattern) throws EolRuntimeException {
		ExecutableBlock<Boolean> matchBlock = pattern.getMatch();
		if (matchBlock != null) {
            Object result = context.getExecutorFactory().execute(matchBlock, context);
            if (result instanceof Return) {
            	result = ((Return) result).getValue();
            }
            if (result instanceof Boolean) {
                return (boolean) result;
            }
            else {
            	throw new EolIllegalReturnException("Pattern Match result should be a Boolean.", result, matchBlock, context);
            }
        }
		return true;
	}
	
	/**
	 * Converts all roles of the pattern into a PatternMatch with the specified bindings.
	 * Note that the size of combinations must be equal to the number of roles in the pattern.
	 * The inner (nested) iterable of combinations are the source bindings for each role name.
	 * 
	 * @see #flatMapRoleBindings(Pattern, Iterable)
	 */
	protected PatternMatch createPatternMatch(Pattern pattern, Collection<? extends Iterable<?>> combination) {
		PatternMatch patternMatch = new PatternMatch(pattern);
		flatMapRoleBindings(pattern.getRoles(), combination).forEach(patternMatch::putRoleBinding);
		return patternMatch;
	}
	
	/**
	 * This method simply flatmaps the results of calling {@linkplain #getVariables(Iterable, Role)}
	 * for each role in the pattern. It will iterate over the larger of <code>pattern.getRoles()</code>
	 * and <code>combination</code> collections first.
	 * 
	 * @param roles The roles of a pattern.
	 * @param combination The instances for each binding. Note that the inner iterable's size must
	 * be equal to the number of names in the role. However the outer size (that is, 
	 * <code>combination.size()</code>) need not necessarily be equal to the number of roles in the pattern.
	 * 
	 * @return A flattened view of the Collection<Collection<Variable>>.
	 */
	public static final Collection<Variable> flatMapRoleBindings(Collection<Role> roles, Collection<? extends Iterable<?>> combination) {
		Stream<Collection<Variable>> variables;
		
		if (combination.size() >= roles.size()) {
			Iterator<? extends Iterable<?>> combinationsIter = combination.iterator();
			variables = roles.stream()
				.map(role -> getVariables(combinationsIter.next(), role));
		}
		else {
			Iterator<Role> rolesIter = roles.iterator();
			variables = combination.stream()
				.map(bindings -> getVariables(bindings, rolesIter.next()));
		}
		
		return variables
			.flatMap(Collection::stream)
			.collect(Collectors.toList());
	}
	
	/**
	 * Binds role names to the objects returned by the bindings iterator.
	 * 
	 * @param bindings The elements to map to each role name. Note that the number
	 * of objects returned by the iterator must be equal to the number of role names.
	 * 
	 * @param role
	 * 
	 * @return The variables, effectively a Collection<Map.Entry<String, ?>>
	 * where the entry is the role name and value is the model element bound to it.
	 */
	public static Collection<Variable> getVariables(Iterable<?> bindings, Role role) {
		Iterator<?> bindingsIter = bindings.iterator();
		return role.getNames()
			.stream()
			.map(roleName -> Variable.createReadOnlyVariable(roleName, bindingsIter.next()))
			.collect(Collectors.toList());
	}

	/**
	 * Puts the result of {@link #flatMapRoleBindings(Pattern, Collection)} into the frame.
	 * @param roles
	 * @param combinations
	 * @param frame
	 * @return The number of variables inserted into the frame.
	 */
	public static final int putRoleBindingsIntoFrame(Collection<Role> roles, Collection<? extends Iterable<?>> combination, Frame frame) {
		Collection<Variable> variables = flatMapRoleBindings(roles, combination);
		variables.forEach(frame::put);
		return variables.size();
	}
	
	/**
	 * Validates whether the given combination matches the constraints imposed by the pattern.
	 * @param pattern
	 * @param combination
	 * @return
	 * @throws EolRuntimeException
	 */
	protected boolean isValidCombination(Pattern pattern, List<? extends Iterable<?>> combination) throws EolRuntimeException {
		final int lastIndex = combination.size()-1;
		List<Role> roles = pattern.getRoles();
		assert roles.size() > lastIndex;
		
		// Variables needed for evaluating successive roles, regardless of validity.
		Frame frame = context.getFrameStack().enterLocal(FrameType.PROTECTED, pattern);
		putRoleBindingsIntoFrame(roles, combination, frame);
		
		boolean result = false;
		// If all the roles are inactive, then the combination is not valid.
		for (Role role : roles) {
			result |= role.isActive(context);
		}
		if (!result) return false;
		
		// If any elements in the last combination are NoMatch, then it's a valid combination.
		for (Object o : combination.get(lastIndex)) {
			if (o instanceof NoMatch) return true;
		}
		
		if (lastIndex >= 0) {
			Role role = roles.get(lastIndex);
			if (role != null && !role.isNegative() && role.getGuard() != null && role.isActive(context) && role.getCardinality().isOne()) {
				result = role.getGuard().execute(context);
			}
		}

		context.getFrameStack().leaveLocal(pattern);
		return result;
	}
	
	/**
	 * The dimensions of the returned nested Iterables are as follows:
	 * <br/><br/>
	 * 	Outer: The number of roles in the pattern (i.e. <code>pattern.getRoles().size()</code>)
	 * <br/><br/>
	 * 	Mid: The number of instances as returned by {@link RoleExecutor#getRoleInstances(Role, String)} multiplied by
	 * 	the number of names for that role (i.e. <code>getRoleInstances(role).size()*role.getNames().size()</code>).
	 * <br/><br/>
	 * Inner: The number of elements which satisfy the guard and domain of the role, holding the
	 * value of previous role bindings and variables constant. As such, the dimensions of this inner-most
	 * Iterable can vary throughout execution if the role has dependencies on values computed in prior roles.
	 * 
	 * @param pattern
	 * @return All combinations of model element instances conforming to the constraints imposed by
	 * the pattern's roles. Note that in most cases this will not be a Collection, but a generator
	 * (lazy Iterator) wrapped into an Iterable for convenience.
	 * @throws EolRuntimeException
	 */
	protected abstract Iterator<? extends Collection<? extends Iterable<?>>> getCandidates(Pattern pattern) throws EolRuntimeException;
	
}
