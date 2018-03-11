/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.List;
import java.util.Map;

import javaMM.Block;
import javaMM.CatchClause;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;
import javaMM.SingleVariableAccess;
import javaMM.SingleVariableDeclaration;
import javaMM.Type;
import javaMM.TypeAccess;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.ocl.pivot.evaluation.Executor;

import org.eclipse.ocl.pivot.ids.IdResolver;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.collection.CollectionNotEmptyOperation;

import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;
import org.eclipse.ocl.pivot.values.InvalidValueException;
import org.eclipse.ocl.pivot.values.SetValue;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Catch Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.CatchClauseImpl#getException <em>Exception</em>}</li>
 *   <li>{@link javaMM.impl.CatchClauseImpl#getBody <em>Body</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CatchClauseImpl extends StatementImpl implements CatchClause {
	/**
	 * The cached value of the '{@link #getException() <em>Exception</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getException()
	 * @generated
	 * @ordered
	 */
	protected SingleVariableDeclaration exception;

	/**
	 * The cached value of the '{@link #getBody() <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBody()
	 * @generated
	 * @ordered
	 */
	protected Block body;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CatchClauseImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.CATCH_CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleVariableDeclaration getException() {
		return exception;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetException(SingleVariableDeclaration newException, NotificationChain msgs) {
		SingleVariableDeclaration oldException = exception;
		exception = newException;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.CATCH_CLAUSE__EXCEPTION, oldException, newException);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setException(SingleVariableDeclaration newException) {
		if (newException != exception) {
			NotificationChain msgs = null;
			if (exception != null)
				msgs = ((InternalEObject)exception).eInverseRemove(this, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE, SingleVariableDeclaration.class, msgs);
			if (newException != null)
				msgs = ((InternalEObject)newException).eInverseAdd(this, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE, SingleVariableDeclaration.class, msgs);
			msgs = basicSetException(newException, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.CATCH_CLAUSE__EXCEPTION, newException, newException));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Block getBody() {
		return body;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBody(Block newBody, NotificationChain msgs) {
		Block oldBody = body;
		body = newBody;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.CATCH_CLAUSE__BODY, oldBody, newBody);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBody(Block newBody) {
		if (newBody != body) {
			NotificationChain msgs = null;
			if (body != null)
				msgs = ((InternalEObject)body).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.CATCH_CLAUSE__BODY, null, msgs);
			if (newBody != null)
				msgs = ((InternalEObject)newBody).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.CATCH_CLAUSE__BODY, null, msgs);
			msgs = basicSetBody(newBody, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.CATCH_CLAUSE__BODY, newBody, newBody));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean doesNotCatchDubiousExceptions(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv doesNotCatchDubiousExceptions:
		 *   let
		 *     severity : Integer[1] = 'CatchClause::doesNotCatchDubiousExceptions'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[1] = self.exception.type.type.name <> 'IllegalMonitorStateException'
		 *       in
		 *         'CatchClause::doesNotCatchDubiousExceptions'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_CatchClause_c_c_doesNotCatchDubiousExceptions);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@NonNull*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ SingleVariableDeclaration exception = this.getException();
				if (exception == null) {
					throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::SingleVariableDeclaration::type\'");
				}
				final /*@Thrown*/ TypeAccess type_0 = exception.getType();
				if (type_0 == null) {
					throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
				}
				final /*@Thrown*/ Type type_1 = type_0.getType();
				if (type_1 == null) {
					throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
				}
				final /*@Thrown*/ String name = type_1.getName();
				final /*@Thrown*/ boolean result = !JavaMMTables.STR_IllegalMonitorStateException.equals(name);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_CatchClause_c_c_doesNotCatchDubiousExceptions, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean exceptionIsUsed(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv exceptionIsUsed:
		 *   let severity : Integer[1] = 'CatchClause::exceptionIsUsed'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[1] = self.exception.usageInVariableAccess->notEmpty()
		 *       in
		 *         'CatchClause::exceptionIsUsed'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_CatchClause_c_c_exceptionIsUsed);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@NonNull*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ SingleVariableDeclaration exception = this.getException();
				if (exception == null) {
					throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::VariableDeclaration::usageInVariableAccess\'");
				}
				final /*@Thrown*/ List<SingleVariableAccess> usageInVariableAccess = exception.getUsageInVariableAccess();
				final /*@Thrown*/ SetValue BOXED_usageInVariableAccess = idResolver.createSetOfAll(JavaMMTables.SET_CLSSid_SingleVariableAccess, usageInVariableAccess);
				final /*@Thrown*/ boolean result = CollectionNotEmptyOperation.INSTANCE.evaluate(BOXED_usageInVariableAccess).booleanValue();
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_CatchClause_c_c_exceptionIsUsed, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				if (exception != null)
					msgs = ((InternalEObject)exception).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.CATCH_CLAUSE__EXCEPTION, null, msgs);
				return basicSetException((SingleVariableDeclaration)otherEnd, msgs);
		}
		return super.eInverseAdd(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				return basicSetException(null, msgs);
			case JavaMMPackage.CATCH_CLAUSE__BODY:
				return basicSetBody(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				return getException();
			case JavaMMPackage.CATCH_CLAUSE__BODY:
				return getBody();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				setException((SingleVariableDeclaration)newValue);
				return;
			case JavaMMPackage.CATCH_CLAUSE__BODY:
				setBody((Block)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				setException((SingleVariableDeclaration)null);
				return;
			case JavaMMPackage.CATCH_CLAUSE__BODY:
				setBody((Block)null);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case JavaMMPackage.CATCH_CLAUSE__EXCEPTION:
				return exception != null;
			case JavaMMPackage.CATCH_CLAUSE__BODY:
				return body != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object eInvoke(int operationID, EList<?> arguments) throws InvocationTargetException {
		switch (operationID) {
			case JavaMMPackage.CATCH_CLAUSE___DOES_NOT_CATCH_DUBIOUS_EXCEPTIONS__DIAGNOSTICCHAIN_MAP:
				return doesNotCatchDubiousExceptions((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CATCH_CLAUSE___EXCEPTION_IS_USED__DIAGNOSTICCHAIN_MAP:
				return exceptionIsUsed((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

} //CatchClauseImpl
