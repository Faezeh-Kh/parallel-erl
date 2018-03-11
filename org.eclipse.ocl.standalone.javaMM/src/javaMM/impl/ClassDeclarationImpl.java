/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javaMM.AbstractMethodDeclaration;
import javaMM.AbstractTypeDeclaration;
import javaMM.Block;
import javaMM.BodyDeclaration;
import javaMM.ClassDeclaration;
import javaMM.FieldDeclaration;
import javaMM.InheritanceKind;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;
import javaMM.MethodDeclaration;
import javaMM.Modifier;
import javaMM.SingleVariableDeclaration;
import javaMM.Statement;
import javaMM.Type;
import javaMM.TypeAccess;
import javaMM.VariableDeclaration;
import javaMM.VisibilityKind;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.ocl.pivot.evaluation.Executor;

import org.eclipse.ocl.pivot.ids.EnumerationLiteralId;
import org.eclipse.ocl.pivot.ids.IdResolver;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.collection.CollectionIncludesOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionIsEmptyOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionSelectByKindOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionSizeOperation;
import org.eclipse.ocl.pivot.library.collection.OrderedCollectionFirstOperation;

import org.eclipse.ocl.pivot.library.logical.BooleanAndOperation;
import org.eclipse.ocl.pivot.library.logical.BooleanImpliesOperation;

import org.eclipse.ocl.pivot.library.oclany.OclAnyOclAsTypeOperation;
import org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsKindOfOperation;
import org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation;
import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.messages.PivotMessages;

import org.eclipse.ocl.pivot.utilities.ClassUtil;
import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;
import org.eclipse.ocl.pivot.values.InvalidValueException;
import org.eclipse.ocl.pivot.values.OrderedSetValue;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Class Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.ClassDeclarationImpl#getSuperClass <em>Super Class</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ClassDeclarationImpl extends TypeDeclarationImpl implements ClassDeclaration {
	/**
	 * The cached value of the '{@link #getSuperClass() <em>Super Class</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSuperClass()
	 * @generated
	 * @ordered
	 */
	protected TypeAccess superClass;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ClassDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.CLASS_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeAccess getSuperClass() {
		return superClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSuperClass(TypeAccess newSuperClass, NotificationChain msgs) {
		TypeAccess oldSuperClass = superClass;
		superClass = newSuperClass;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS, oldSuperClass, newSuperClass);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSuperClass(TypeAccess newSuperClass) {
		if (newSuperClass != superClass) {
			NotificationChain msgs = null;
			if (superClass != null)
				msgs = ((InternalEObject)superClass).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS, null, msgs);
			if (newSuperClass != null)
				msgs = ((InternalEObject)newSuperClass).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS, null, msgs);
			msgs = basicSetSuperClass(newSuperClass, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS, newSuperClass, newSuperClass));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<MethodDeclaration> getMethods() {
		/**
		 * self.bodyDeclarations->selectByKind(MethodDeclaration)
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_MethodDeclaration_0 = idResolver.getClass(JavaMMTables.CLSSid_MethodDeclaration, null);
		final /*@NonInvalid*/ List<BodyDeclaration> bodyDeclarations = this.getBodyDeclarations();
		final /*@NonInvalid*/ OrderedSetValue BOXED_bodyDeclarations = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_BodyDeclaration, bodyDeclarations);
		final /*@NonInvalid*/ OrderedSetValue selectByKind = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_bodyDeclarations, TYP_javaMM_c_c_MethodDeclaration_0);
		final /*@NonInvalid*/ List<MethodDeclaration> ECORE_selectByKind = ((IdResolver.IdResolverExtension)idResolver).ecoreValueOfAll(MethodDeclaration.class, selectByKind);
		return (EList<MethodDeclaration>)ECORE_selectByKind;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean hasEquals() {
		/**
		 *
		 * getMethods()
		 * ->exists(method | method.name = 'equals' and
		 *   method.parameters->first().type.type.name = 'Object' and
		 *   method.parameters->size() = 1 and
		 *   method.thrownExceptions->isEmpty() and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
		 *   method.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean))
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
		final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
		/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
		/*@NonNull*/ Iterator<Object> ITERATOR_method = BOXED_getMethods.iterator();
		/*@Thrown*/ Boolean exists;
		while (true) {
			if (!ITERATOR_method.hasNext()) {
				if (accumulator == null) {
					exists = null;
				}
				else if (accumulator == ValueUtil.FALSE_VALUE) {
					exists = ValueUtil.FALSE_VALUE;
				}
				else {
					throw (InvalidValueException)accumulator;
				}
				break;
			}
			/*@NonInvalid*/ MethodDeclaration method = (MethodDeclaration)ITERATOR_method.next();
			/**
			 * method.name = 'equals' and
			 * method.parameters->first().type.type.name = 'Object' and
			 * method.parameters->size() = 1 and
			 * method.thrownExceptions->isEmpty() and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
			 * method.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean)
			 */
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_4;
			try {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_3;
				try {
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_2;
					try {
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
							try {
								final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters_0 = method.getParameters();
								final /*@NonInvalid*/ OrderedSetValue BOXED_parameters_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters_0);
								/*@Caught*/ /*@NonNull*/ Object CAUGHT_and;
								try {
									final /*@NonInvalid*/ String name = method.getName();
									final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_equals.equals(name);
									/*@Thrown*/ boolean and;
									if (eq) {
										final /*@Thrown*/ SingleVariableDeclaration first = (SingleVariableDeclaration)OrderedCollectionFirstOperation.INSTANCE.evaluate(BOXED_parameters_0);
										if (first == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::SingleVariableDeclaration::type\'");
										}
										final /*@Thrown*/ TypeAccess type_0 = first.getType();
										if (type_0 == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
										}
										final /*@Thrown*/ Type type_1 = type_0.getType();
										if (type_1 == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
										}
										final /*@Thrown*/ String name_0 = type_1.getName();
										final /*@Thrown*/ boolean eq_0 = JavaMMTables.STR_Object.equals(name_0);
										and = eq_0;
									}
									else {
										and = ValueUtil.FALSE_VALUE;
									}
									CAUGHT_and = and;
								}
								catch (Exception e) {
									CAUGHT_and = ValueUtil.createInvalidValue(e);
								}
								final /*@NonInvalid*/ IntegerValue size = CollectionSizeOperation.INSTANCE.evaluate(BOXED_parameters_0);
								final /*@NonInvalid*/ boolean eq_1 = size.equals(JavaMMTables.INT_1);
								final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and, eq_1);
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							final /*@NonInvalid*/ List<TypeAccess> thrownExceptions = method.getThrownExceptions();
							final /*@NonInvalid*/ OrderedSetValue BOXED_thrownExceptions = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, thrownExceptions);
							final /*@NonInvalid*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_thrownExceptions).booleanValue();
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, isEmpty);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						final /*@NonInvalid*/ Modifier modifier = method.getModifier();
						final /*@NonInvalid*/ boolean ne = modifier != null;
						final /*@Thrown*/ Boolean and_2 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_1, ne);
						CAUGHT_and_2 = and_2;
					}
					catch (Exception e) {
						CAUGHT_and_2 = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_2;
					try {
						final /*@NonInvalid*/ Modifier modifier_0 = method.getModifier();
						if (modifier_0 == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
						}
						final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
						final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
						final /*@Thrown*/ boolean eq_2 = BOXED_visibility == JavaMMTables.ELITid_public;
						CAUGHT_eq_2 = eq_2;
					}
					catch (Exception e) {
						CAUGHT_eq_2 = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean and_3 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_2, CAUGHT_eq_2);
					CAUGHT_and_3 = and_3;
				}
				catch (Exception e) {
					CAUGHT_and_3 = ValueUtil.createInvalidValue(e);
				}
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
				try {
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeBoolean_0 = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeBoolean, null);
					final /*@NonInvalid*/ TypeAccess returnType = method.getReturnType();
					if (returnType == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
					}
					final /*@Thrown*/ Type type_2 = returnType.getType();
					final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_2, TYP_javaMM_c_c_PrimitiveTypeBoolean_0).booleanValue();
					CAUGHT_oclIsTypeOf = oclIsTypeOf;
				}
				catch (Exception e) {
					CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
				}
				final /*@Thrown*/ Boolean and_4 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_3, CAUGHT_oclIsTypeOf);
				CAUGHT_and_4 = and_4;
			}
			catch (Exception e) {
				CAUGHT_and_4 = ValueUtil.createInvalidValue(e);
			}
			//
			if (CAUGHT_and_4 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
				exists = ValueUtil.TRUE_VALUE;
				break;														// Stop immediately
			}
			else if (CAUGHT_and_4 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
				;															// Carry on
			}
			else if (CAUGHT_and_4 == null) {								// Abnormal null body evaluation result
				if (accumulator == ValueUtil.FALSE_VALUE) {
					accumulator = null;										// Cache a null failure
				}
			}
			else if (CAUGHT_and_4 instanceof InvalidValueException) {		// Abnormal exception evaluation result
				accumulator = CAUGHT_and_4;									// Cache an exception failure
			}
			else {															// Impossible badly typed result
				accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
			}
		}
		return exists;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean hasHashcode() {
		/**
		 *
		 * getMethods()
		 * ->exists(method | method.name = 'hashCode' and
		 *   method.parameters->isEmpty() and
		 *   method.thrownExceptions->isEmpty() and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
		 *   method.returnType.type.oclIsTypeOf(PrimitiveTypeInt))
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
		final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
		/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
		/*@NonNull*/ Iterator<Object> ITERATOR_method = BOXED_getMethods.iterator();
		/*@Thrown*/ Boolean exists;
		while (true) {
			if (!ITERATOR_method.hasNext()) {
				if (accumulator == null) {
					exists = null;
				}
				else if (accumulator == ValueUtil.FALSE_VALUE) {
					exists = ValueUtil.FALSE_VALUE;
				}
				else {
					throw (InvalidValueException)accumulator;
				}
				break;
			}
			/*@NonInvalid*/ MethodDeclaration method = (MethodDeclaration)ITERATOR_method.next();
			/**
			 * method.name = 'hashCode' and
			 * method.parameters->isEmpty() and
			 * method.thrownExceptions->isEmpty() and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
			 * method.returnType.type.oclIsTypeOf(PrimitiveTypeInt)
			 */
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_3;
			try {
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_and_2;
				try {
					final /*@NonInvalid*/ String name = method.getName();
					final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_hashCode.equals(name);
					/*@NonInvalid*/ boolean and;
					if (eq) {
						final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters = method.getParameters();
						final /*@NonInvalid*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
						final /*@NonInvalid*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_parameters).booleanValue();
						and = isEmpty;
					}
					else {
						and = ValueUtil.FALSE_VALUE;
					}
					/*@NonInvalid*/ boolean and_0;
					if (and) {
						final /*@NonInvalid*/ List<TypeAccess> thrownExceptions = method.getThrownExceptions();
						final /*@NonInvalid*/ OrderedSetValue BOXED_thrownExceptions = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, thrownExceptions);
						final /*@NonInvalid*/ boolean isEmpty_0 = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_thrownExceptions).booleanValue();
						and_0 = isEmpty_0;
					}
					else {
						and_0 = ValueUtil.FALSE_VALUE;
					}
					/*@NonInvalid*/ boolean and_1;
					if (and_0) {
						final /*@NonInvalid*/ Modifier modifier = method.getModifier();
						final /*@NonInvalid*/ boolean ne = modifier != null;
						and_1 = ne;
					}
					else {
						and_1 = ValueUtil.FALSE_VALUE;
					}
					/*@Thrown*/ boolean and_2;
					if (and_1) {
						final /*@NonInvalid*/ Modifier modifier_0 = method.getModifier();
						if (modifier_0 == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
						}
						final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
						final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
						final /*@Thrown*/ boolean eq_0 = BOXED_visibility == JavaMMTables.ELITid_public;
						and_2 = eq_0;
					}
					else {
						and_2 = ValueUtil.FALSE_VALUE;
					}
					CAUGHT_and_2 = and_2;
				}
				catch (Exception e) {
					CAUGHT_and_2 = ValueUtil.createInvalidValue(e);
				}
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
				try {
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeInt_0 = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeInt, null);
					final /*@NonInvalid*/ TypeAccess returnType = method.getReturnType();
					if (returnType == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
					}
					final /*@Thrown*/ Type type_0 = returnType.getType();
					final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_0, TYP_javaMM_c_c_PrimitiveTypeInt_0).booleanValue();
					CAUGHT_oclIsTypeOf = oclIsTypeOf;
				}
				catch (Exception e) {
					CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
				}
				final /*@Thrown*/ Boolean and_3 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_2, CAUGHT_oclIsTypeOf);
				CAUGHT_and_3 = and_3;
			}
			catch (Exception e) {
				CAUGHT_and_3 = ValueUtil.createInvalidValue(e);
			}
			//
			if (CAUGHT_and_3 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
				exists = ValueUtil.TRUE_VALUE;
				break;														// Stop immediately
			}
			else if (CAUGHT_and_3 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
				;															// Carry on
			}
			else if (CAUGHT_and_3 == null) {								// Abnormal null body evaluation result
				if (accumulator == ValueUtil.FALSE_VALUE) {
					accumulator = null;										// Cache a null failure
				}
			}
			else if (CAUGHT_and_3 instanceof InvalidValueException) {		// Abnormal exception evaluation result
				accumulator = CAUGHT_and_3;									// Cache an exception failure
			}
			else {															// Impossible badly typed result
				accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
			}
		}
		return exists;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean hasCompareTo() {
		/**
		 *
		 * getMethods()
		 * ->exists(method | method.name = 'compareTo' and
		 *   method.parameters->size() = 1 and
		 *   method.parameters->first().type.type.name = 'Object' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
		 *   method.thrownExceptions->isEmpty() and
		 *   method.returnType.type.oclIsTypeOf(PrimitiveTypeInt))
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
		final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
		/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
		/*@NonNull*/ Iterator<Object> ITERATOR_method = BOXED_getMethods.iterator();
		/*@Thrown*/ Boolean exists;
		while (true) {
			if (!ITERATOR_method.hasNext()) {
				if (accumulator == null) {
					exists = null;
				}
				else if (accumulator == ValueUtil.FALSE_VALUE) {
					exists = ValueUtil.FALSE_VALUE;
				}
				else {
					throw (InvalidValueException)accumulator;
				}
				break;
			}
			/*@NonInvalid*/ MethodDeclaration method = (MethodDeclaration)ITERATOR_method.next();
			/**
			 * method.name = 'compareTo' and
			 * method.parameters->size() = 1 and
			 * method.parameters->first().type.type.name = 'Object' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
			 * method.thrownExceptions->isEmpty() and
			 * method.returnType.type.oclIsTypeOf(PrimitiveTypeInt)
			 */
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_4;
			try {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_3;
				try {
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_2;
					try {
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_and_0;
							try {
								final /*@NonInvalid*/ String name = method.getName();
								final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_compareTo.equals(name);
								/*@NonInvalid*/ boolean and;
								if (eq) {
									final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters = method.getParameters();
									final /*@NonInvalid*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
									final /*@NonInvalid*/ IntegerValue size = CollectionSizeOperation.INSTANCE.evaluate(BOXED_parameters);
									final /*@NonInvalid*/ boolean eq_0 = size.equals(JavaMMTables.INT_1);
									and = eq_0;
								}
								else {
									and = ValueUtil.FALSE_VALUE;
								}
								/*@Thrown*/ boolean and_0;
								if (and) {
									final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters_0 = method.getParameters();
									final /*@NonInvalid*/ OrderedSetValue BOXED_parameters_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters_0);
									final /*@Thrown*/ SingleVariableDeclaration first = (SingleVariableDeclaration)OrderedCollectionFirstOperation.INSTANCE.evaluate(BOXED_parameters_0);
									if (first == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::SingleVariableDeclaration::type\'");
									}
									final /*@Thrown*/ TypeAccess type_0 = first.getType();
									if (type_0 == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
									}
									final /*@Thrown*/ Type type_1 = type_0.getType();
									if (type_1 == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
									}
									final /*@Thrown*/ String name_0 = type_1.getName();
									final /*@Thrown*/ boolean eq_1 = JavaMMTables.STR_Object.equals(name_0);
									and_0 = eq_1;
								}
								else {
									and_0 = ValueUtil.FALSE_VALUE;
								}
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							final /*@NonInvalid*/ Modifier modifier = method.getModifier();
							final /*@NonInvalid*/ boolean ne = modifier != null;
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, ne);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_2;
						try {
							final /*@NonInvalid*/ Modifier modifier_0 = method.getModifier();
							if (modifier_0 == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
							}
							final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
							final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
							final /*@Thrown*/ boolean eq_2 = BOXED_visibility == JavaMMTables.ELITid_public;
							CAUGHT_eq_2 = eq_2;
						}
						catch (Exception e) {
							CAUGHT_eq_2 = ValueUtil.createInvalidValue(e);
						}
						final /*@Thrown*/ Boolean and_2 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_1, CAUGHT_eq_2);
						CAUGHT_and_2 = and_2;
					}
					catch (Exception e) {
						CAUGHT_and_2 = ValueUtil.createInvalidValue(e);
					}
					final /*@NonInvalid*/ List<TypeAccess> thrownExceptions = method.getThrownExceptions();
					final /*@NonInvalid*/ OrderedSetValue BOXED_thrownExceptions = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, thrownExceptions);
					final /*@NonInvalid*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_thrownExceptions).booleanValue();
					final /*@Thrown*/ Boolean and_3 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_2, isEmpty);
					CAUGHT_and_3 = and_3;
				}
				catch (Exception e) {
					CAUGHT_and_3 = ValueUtil.createInvalidValue(e);
				}
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
				try {
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeInt_0 = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeInt, null);
					final /*@NonInvalid*/ TypeAccess returnType = method.getReturnType();
					if (returnType == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
					}
					final /*@Thrown*/ Type type_2 = returnType.getType();
					final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_2, TYP_javaMM_c_c_PrimitiveTypeInt_0).booleanValue();
					CAUGHT_oclIsTypeOf = oclIsTypeOf;
				}
				catch (Exception e) {
					CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
				}
				final /*@Thrown*/ Boolean and_4 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_3, CAUGHT_oclIsTypeOf);
				CAUGHT_and_4 = and_4;
			}
			catch (Exception e) {
				CAUGHT_and_4 = ValueUtil.createInvalidValue(e);
			}
			//
			if (CAUGHT_and_4 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
				exists = ValueUtil.TRUE_VALUE;
				break;														// Stop immediately
			}
			else if (CAUGHT_and_4 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
				;															// Carry on
			}
			else if (CAUGHT_and_4 == null) {								// Abnormal null body evaluation result
				if (accumulator == ValueUtil.FALSE_VALUE) {
					accumulator = null;										// Cache a null failure
				}
			}
			else if (CAUGHT_and_4 instanceof InvalidValueException) {		// Abnormal exception evaluation result
				accumulator = CAUGHT_and_4;									// Cache an exception failure
			}
			else {															// Impossible badly typed result
				accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
			}
		}
		return exists;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean noCovariantCompareTo(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv noCovariantCompareTo:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::noCovariantCompareTo'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = getMethods()
		 *         ->exists(method | method.name = 'compareTo' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
		 *           method.returnType.type.oclIsTypeOf(PrimitiveTypeInt)) implies
		 *         hasCompareTo()
		 *       in
		 *         'ClassDeclaration::noCovariantCompareTo'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_noCovariantCompareTo);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_exists;
				try {
					final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
					final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
					/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
					/*@NonNull*/ Iterator<Object> ITERATOR_method = BOXED_getMethods.iterator();
					/*@Thrown*/ Boolean exists;
					while (true) {
						if (!ITERATOR_method.hasNext()) {
							if (accumulator == null) {
								exists = null;
							}
							else if (accumulator == ValueUtil.FALSE_VALUE) {
								exists = ValueUtil.FALSE_VALUE;
							}
							else {
								throw (InvalidValueException)accumulator;
							}
							break;
						}
						/*@NonInvalid*/ MethodDeclaration method = (MethodDeclaration)ITERATOR_method.next();
						/**
						 * method.name = 'compareTo' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
						 * method.returnType.type.oclIsTypeOf(PrimitiveTypeInt)
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_and_0;
							try {
								final /*@NonInvalid*/ String name = method.getName();
								final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_compareTo.equals(name);
								/*@NonInvalid*/ boolean and;
								if (eq) {
									final /*@NonInvalid*/ Modifier modifier = method.getModifier();
									final /*@NonInvalid*/ boolean ne = modifier != null;
									and = ne;
								}
								else {
									and = ValueUtil.FALSE_VALUE;
								}
								/*@Thrown*/ boolean and_0;
								if (and) {
									final /*@NonInvalid*/ Modifier modifier_0 = method.getModifier();
									if (modifier_0 == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
									}
									final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
									final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
									final /*@Thrown*/ boolean eq_0 = BOXED_visibility == JavaMMTables.ELITid_public;
									and_0 = eq_0;
								}
								else {
									and_0 = ValueUtil.FALSE_VALUE;
								}
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeInt = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeInt, null);
								final /*@NonInvalid*/ TypeAccess returnType = method.getReturnType();
								if (returnType == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
								}
								final /*@Thrown*/ Type type_0 = returnType.getType();
								final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_0, TYP_javaMM_c_c_PrimitiveTypeInt).booleanValue();
								CAUGHT_oclIsTypeOf = oclIsTypeOf;
							}
							catch (Exception e) {
								CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_oclIsTypeOf);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						//
						if (CAUGHT_and_1 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
							exists = ValueUtil.TRUE_VALUE;
							break;														// Stop immediately
						}
						else if (CAUGHT_and_1 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
							;															// Carry on
						}
						else if (CAUGHT_and_1 == null) {								// Abnormal null body evaluation result
							if (accumulator == ValueUtil.FALSE_VALUE) {
								accumulator = null;										// Cache a null failure
							}
						}
						else if (CAUGHT_and_1 instanceof InvalidValueException) {		// Abnormal exception evaluation result
							accumulator = CAUGHT_and_1;									// Cache an exception failure
						}
						else {															// Impossible badly typed result
							accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
						}
					}
					CAUGHT_exists = exists;
				}
				catch (Exception e) {
					CAUGHT_exists = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ boolean hasCompareTo = this.hasCompareTo();
				final /*@Thrown*/ Boolean result = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_exists, hasCompareTo);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_noCovariantCompareTo, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean hashCodeAndEquals(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv hashCodeAndEquals:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::hashCodeAndEquals'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = let hasEq : Boolean[?] = hasEquals()
		 *         in
		 *           let hasHc : Boolean[?] = hasHashcode()
		 *           in (hasEq implies hasHc) and (hasHc implies hasEq)
		 *       in
		 *         'ClassDeclaration::hashCodeAndEquals'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_hashCodeAndEquals);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ boolean hasEq = this.hasEquals();
			final /*@NonInvalid*/ boolean hasHc = this.hasHashcode();
			/*@NonInvalid*/ boolean implies;
			if (hasEq) {
				implies = hasHc;
			}
			else {
				implies = ValueUtil.TRUE_VALUE;
			}
			/*@NonInvalid*/ boolean and;
			if (implies) {
				/*@NonInvalid*/ boolean implies_0;
				if (hasHc) {
					implies_0 = hasEq;
				}
				else {
					implies_0 = ValueUtil.TRUE_VALUE;
				}
				and = implies_0;
			}
			else {
				and = ValueUtil.FALSE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_hashCodeAndEquals, this, (Object)null, diagnostics, context, (Object)null, severity_0, and, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean serialUIDInSerializableClass(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv serialUIDInSerializableClass:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::serialUIDInSerializableClass'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = self.implements('Serializable') implies
		 *         self.bodyDeclarations->selectByKind(FieldDeclaration)
		 *         ->exists(bd | bd.name = 'serialVersionUID' and bd.modifier.static and bd.modifier.inheritance = InheritanceKind::final and
		 *           bd.type.type.oclIsTypeOf(PrimitiveTypeLong))
		 *       in
		 *         'ClassDeclaration::serialUIDInSerializableClass'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_serialUIDInSerializableClass);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_2;
		if (le) {
			symbol_2 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ boolean symbol_0 = ((AbstractTypeDeclaration)this).implements_(JavaMMTables.STR_Serializable);
				/*@Thrown*/ Boolean result;
				if (symbol_0) {
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_FieldDeclaration = idResolver.getClass(JavaMMTables.CLSSid_FieldDeclaration, null);
					final /*@NonInvalid*/ List<BodyDeclaration> bodyDeclarations = this.getBodyDeclarations();
					final /*@NonInvalid*/ OrderedSetValue BOXED_bodyDeclarations = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_BodyDeclaration, bodyDeclarations);
					final /*@NonInvalid*/ OrderedSetValue selectByKind = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_bodyDeclarations, TYP_javaMM_c_c_FieldDeclaration);
					/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
					/*@NonNull*/ Iterator<Object> ITERATOR_bd = selectByKind.iterator();
					/*@Thrown*/ Boolean exists;
					while (true) {
						if (!ITERATOR_bd.hasNext()) {
							if (accumulator == null) {
								exists = null;
							}
							else if (accumulator == ValueUtil.FALSE_VALUE) {
								exists = ValueUtil.FALSE_VALUE;
							}
							else {
								throw (InvalidValueException)accumulator;
							}
							break;
						}
						/*@NonInvalid*/ FieldDeclaration bd = (FieldDeclaration)ITERATOR_bd.next();
						/**
						 * bd.name = 'serialVersionUID' and bd.modifier.static and bd.modifier.inheritance = InheritanceKind::final and
						 * bd.type.type.oclIsTypeOf(PrimitiveTypeLong)
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
							try {
								/*@Caught*/ /*@Nullable*/ Object CAUGHT_and;
								try {
									final /*@NonInvalid*/ String name = bd.getName();
									final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_serialVersionUID.equals(name);
									/*@Thrown*/ Boolean and;
									if (eq) {
										final /*@NonInvalid*/ Modifier modifier = bd.getModifier();
										if (modifier == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::static\'");
										}
										final /*@Thrown*/ Boolean symbol_1 = modifier.getStatic();
										and = symbol_1;
									}
									else {
										and = ValueUtil.FALSE_VALUE;
									}
									CAUGHT_and = and;
								}
								catch (Exception e) {
									CAUGHT_and = ValueUtil.createInvalidValue(e);
								}
								/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_0;
								try {
									final /*@NonInvalid*/ Modifier modifier_0 = bd.getModifier();
									if (modifier_0 == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::inheritance\'");
									}
									final /*@Thrown*/ InheritanceKind inheritance = modifier_0.getInheritance();
									final /*@Thrown*/ EnumerationLiteralId BOXED_inheritance = inheritance == null ? null : JavaMMTables.ENUMid_InheritanceKind.getEnumerationLiteralId(ClassUtil.nonNullState(inheritance.getName()));
									final /*@Thrown*/ boolean eq_0 = BOXED_inheritance == JavaMMTables.ELITid_final;
									CAUGHT_eq_0 = eq_0;
								}
								catch (Exception e) {
									CAUGHT_eq_0 = ValueUtil.createInvalidValue(e);
								}
								final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and, CAUGHT_eq_0);
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeLong = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeLong, null);
								final /*@NonInvalid*/ TypeAccess type_0 = bd.getType();
								if (type_0 == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
								}
								final /*@Thrown*/ Type type_1 = type_0.getType();
								final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_1, TYP_javaMM_c_c_PrimitiveTypeLong).booleanValue();
								CAUGHT_oclIsTypeOf = oclIsTypeOf;
							}
							catch (Exception e) {
								CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_oclIsTypeOf);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						//
						if (CAUGHT_and_1 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
							exists = ValueUtil.TRUE_VALUE;
							break;														// Stop immediately
						}
						else if (CAUGHT_and_1 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
							;															// Carry on
						}
						else if (CAUGHT_and_1 == null) {								// Abnormal null body evaluation result
							if (accumulator == ValueUtil.FALSE_VALUE) {
								accumulator = null;										// Cache a null failure
							}
						}
						else if (CAUGHT_and_1 instanceof InvalidValueException) {		// Abnormal exception evaluation result
							accumulator = CAUGHT_and_1;									// Cache an exception failure
						}
						else {															// Impossible badly typed result
							accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
						}
					}
					result = exists;
				}
				else {
					result = ValueUtil.TRUE_VALUE;
				}
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_serialUIDInSerializableClass, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_2 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_2;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean equalsAndCompareTo(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv equalsAndCompareTo:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::equalsAndCompareTo'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = hasCompareTo() implies hasEquals()
		 *       in
		 *         'ClassDeclaration::equalsAndCompareTo'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_equalsAndCompareTo);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ boolean hasCompareTo = this.hasCompareTo();
			/*@NonInvalid*/ boolean result;
			if (hasCompareTo) {
				final /*@NonInvalid*/ boolean hasEquals = this.hasEquals();
				result = hasEquals;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_equalsAndCompareTo, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean noRedundantInterfaceImpl(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv noRedundantInterfaceImpl:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::noRedundantInterfaceImpl'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.superInterfaces <> null and self.superClass <> null and
		 *         self.superClass.type.oclIsKindOf(ClassDeclaration) and
		 *         self.superClass.type.oclAsType(ClassDeclaration).superInterfaces <> null implies
		 *         self.superInterfaces->exists(tIface |
		 *           self.superClass.type.oclIsKindOf(AbstractTypeDeclaration) implies
		 *           self.superClass.type.oclAsType(AbstractTypeDeclaration)
		 *           .superInterfaces->includes(tIface))
		 *       in
		 *         'ClassDeclaration::noRedundantInterfaceImpl'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_noRedundantInterfaceImpl);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
				try {
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_and;
					try {
						final /*@NonInvalid*/ TypeAccess superClass = this.getSuperClass();
						final /*@NonInvalid*/ boolean ne = superClass != null;
						/*@Thrown*/ boolean and;
						if (ne) {
							final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_ClassDeclaration = idResolver.getClass(JavaMMTables.CLSSid_ClassDeclaration, null);
							if (superClass == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
							}
							final /*@Thrown*/ Type type_0 = superClass.getType();
							final /*@Thrown*/ boolean oclIsKindOf = OclAnyOclIsKindOfOperation.INSTANCE.evaluate(executor, type_0, TYP_javaMM_c_c_ClassDeclaration).booleanValue();
							and = oclIsKindOf;
						}
						else {
							and = ValueUtil.FALSE_VALUE;
						}
						CAUGHT_and = and;
					}
					catch (Exception e) {
						CAUGHT_and = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and, ValueUtil.TRUE_VALUE);
					CAUGHT_and_0 = and_0;
				}
				catch (Exception e) {
					CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
				}
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_exists;
				try {
					final /*@NonInvalid*/ List<TypeAccess> superInterfaces = this.getSuperInterfaces();
					final /*@NonInvalid*/ OrderedSetValue BOXED_superInterfaces = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, superInterfaces);
					/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
					/*@NonNull*/ Iterator<Object> ITERATOR_tIface = BOXED_superInterfaces.iterator();
					/*@Thrown*/ Boolean exists;
					while (true) {
						if (!ITERATOR_tIface.hasNext()) {
							if (accumulator == null) {
								exists = null;
							}
							else if (accumulator == ValueUtil.FALSE_VALUE) {
								exists = ValueUtil.FALSE_VALUE;
							}
							else {
								throw (InvalidValueException)accumulator;
							}
							break;
						}
						/*@NonInvalid*/ TypeAccess tIface = (TypeAccess)ITERATOR_tIface.next();
						/**
						 *
						 * self.superClass.type.oclIsKindOf(AbstractTypeDeclaration) implies
						 * self.superClass.type.oclAsType(AbstractTypeDeclaration)
						 * .superInterfaces->includes(tIface)
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_implies;
						try {
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsKindOf_0;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_AbstractTypeDeclaration = idResolver.getClass(JavaMMTables.CLSSid_AbstractTypeDeclaration, null);
								final /*@NonInvalid*/ TypeAccess superClass_1 = this.getSuperClass();
								if (superClass_1 == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
								}
								final /*@Thrown*/ Type type_1 = superClass_1.getType();
								final /*@Thrown*/ boolean oclIsKindOf_0 = OclAnyOclIsKindOfOperation.INSTANCE.evaluate(executor, type_1, TYP_javaMM_c_c_AbstractTypeDeclaration).booleanValue();
								CAUGHT_oclIsKindOf_0 = oclIsKindOf_0;
							}
							catch (Exception e) {
								CAUGHT_oclIsKindOf_0 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_includes;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_AbstractTypeDeclaration_0 = idResolver.getClass(JavaMMTables.CLSSid_AbstractTypeDeclaration, null);
								final /*@NonInvalid*/ TypeAccess superClass_2 = this.getSuperClass();
								if (superClass_2 == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
								}
								final /*@Thrown*/ Type type_2 = superClass_2.getType();
								final /*@Thrown*/ AbstractTypeDeclaration oclAsType = ClassUtil.nonNullState((AbstractTypeDeclaration)OclAnyOclAsTypeOperation.INSTANCE.evaluate(executor, type_2, TYP_javaMM_c_c_AbstractTypeDeclaration_0));
								final /*@Thrown*/ List<TypeAccess> superInterfaces_0 = oclAsType.getSuperInterfaces();
								final /*@Thrown*/ OrderedSetValue BOXED_superInterfaces_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, superInterfaces_0);
								final /*@Thrown*/ boolean includes = CollectionIncludesOperation.INSTANCE.evaluate(BOXED_superInterfaces_0, tIface).booleanValue();
								CAUGHT_includes = includes;
							}
							catch (Exception e) {
								CAUGHT_includes = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean implies = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_oclIsKindOf_0, CAUGHT_includes);
							CAUGHT_implies = implies;
						}
						catch (Exception e) {
							CAUGHT_implies = ValueUtil.createInvalidValue(e);
						}
						//
						if (CAUGHT_implies == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
							exists = ValueUtil.TRUE_VALUE;
							break;														// Stop immediately
						}
						else if (CAUGHT_implies == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
							;															// Carry on
						}
						else if (CAUGHT_implies == null) {								// Abnormal null body evaluation result
							if (accumulator == ValueUtil.FALSE_VALUE) {
								accumulator = null;										// Cache a null failure
							}
						}
						else if (CAUGHT_implies instanceof InvalidValueException) {		// Abnormal exception evaluation result
							accumulator = CAUGHT_implies;									// Cache an exception failure
						}
						else {															// Impossible badly typed result
							accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
						}
					}
					CAUGHT_exists = exists;
				}
				catch (Exception e) {
					CAUGHT_exists = ValueUtil.createInvalidValue(e);
				}
				final /*@Thrown*/ Boolean result = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_exists);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_noRedundantInterfaceImpl, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean cloneInCloneable(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv cloneInCloneable:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::cloneInCloneable'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = self.implements('Cloneable') implies
		 *         self.getMethods()
		 *         ->exists(md | md.name = 'clone' and
		 *           md.parameters->isEmpty() and
		 *           md.thrownExceptions->size() = 1 and
		 *           md.thrownExceptions->first().type.name = 'CloneNotSupportedException' and md.modifier <> null and md.modifier.visibility = VisibilityKind::public)
		 *       in
		 *         'ClassDeclaration::cloneInCloneable'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_cloneInCloneable);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_1;
		if (le) {
			symbol_1 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ boolean symbol_0 = ((AbstractTypeDeclaration)this).implements_(JavaMMTables.STR_Cloneable);
				/*@Thrown*/ Boolean result;
				if (symbol_0) {
					final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
					final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
					/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
					/*@NonNull*/ Iterator<Object> ITERATOR_md = BOXED_getMethods.iterator();
					/*@Thrown*/ Boolean exists;
					while (true) {
						if (!ITERATOR_md.hasNext()) {
							if (accumulator == null) {
								exists = null;
							}
							else if (accumulator == ValueUtil.FALSE_VALUE) {
								exists = ValueUtil.FALSE_VALUE;
							}
							else {
								throw (InvalidValueException)accumulator;
							}
							break;
						}
						/*@NonInvalid*/ MethodDeclaration md = (MethodDeclaration)ITERATOR_md.next();
						/**
						 * md.name = 'clone' and
						 * md.parameters->isEmpty() and
						 * md.thrownExceptions->size() = 1 and
						 * md.thrownExceptions->first().type.name = 'CloneNotSupportedException' and md.modifier <> null and md.modifier.visibility = VisibilityKind::public
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_3;
						try {
							/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_2;
							try {
								/*@Caught*/ /*@NonNull*/ Object CAUGHT_and_1;
								try {
									final /*@NonInvalid*/ String name = md.getName();
									final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_clone.equals(name);
									/*@NonInvalid*/ boolean and;
									if (eq) {
										final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters = md.getParameters();
										final /*@NonInvalid*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
										final /*@NonInvalid*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_parameters).booleanValue();
										and = isEmpty;
									}
									else {
										and = ValueUtil.FALSE_VALUE;
									}
									/*@NonInvalid*/ boolean and_0;
									if (and) {
										final /*@NonInvalid*/ List<TypeAccess> thrownExceptions = md.getThrownExceptions();
										final /*@NonInvalid*/ OrderedSetValue BOXED_thrownExceptions = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, thrownExceptions);
										final /*@NonInvalid*/ IntegerValue size = CollectionSizeOperation.INSTANCE.evaluate(BOXED_thrownExceptions);
										final /*@NonInvalid*/ boolean eq_0 = size.equals(JavaMMTables.INT_1);
										and_0 = eq_0;
									}
									else {
										and_0 = ValueUtil.FALSE_VALUE;
									}
									/*@Thrown*/ boolean and_1;
									if (and_0) {
										final /*@NonInvalid*/ List<TypeAccess> thrownExceptions_0 = md.getThrownExceptions();
										final /*@NonInvalid*/ OrderedSetValue BOXED_thrownExceptions_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_TypeAccess, thrownExceptions_0);
										final /*@Thrown*/ TypeAccess first = (TypeAccess)OrderedCollectionFirstOperation.INSTANCE.evaluate(BOXED_thrownExceptions_0);
										if (first == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
										}
										final /*@Thrown*/ Type type_0 = first.getType();
										if (type_0 == null) {
											throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
										}
										final /*@Thrown*/ String name_0 = type_0.getName();
										final /*@Thrown*/ boolean eq_1 = JavaMMTables.STR_CloneNotSupportedException.equals(name_0);
										and_1 = eq_1;
									}
									else {
										and_1 = ValueUtil.FALSE_VALUE;
									}
									CAUGHT_and_1 = and_1;
								}
								catch (Exception e) {
									CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
								}
								final /*@NonInvalid*/ Modifier modifier = md.getModifier();
								final /*@NonInvalid*/ boolean ne = modifier != null;
								final /*@Thrown*/ Boolean and_2 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_1, ne);
								CAUGHT_and_2 = and_2;
							}
							catch (Exception e) {
								CAUGHT_and_2 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_2;
							try {
								final /*@NonInvalid*/ Modifier modifier_0 = md.getModifier();
								if (modifier_0 == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
								}
								final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
								final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
								final /*@Thrown*/ boolean eq_2 = BOXED_visibility == JavaMMTables.ELITid_public;
								CAUGHT_eq_2 = eq_2;
							}
							catch (Exception e) {
								CAUGHT_eq_2 = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean and_3 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_2, CAUGHT_eq_2);
							CAUGHT_and_3 = and_3;
						}
						catch (Exception e) {
							CAUGHT_and_3 = ValueUtil.createInvalidValue(e);
						}
						//
						if (CAUGHT_and_3 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
							exists = ValueUtil.TRUE_VALUE;
							break;														// Stop immediately
						}
						else if (CAUGHT_and_3 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
							;															// Carry on
						}
						else if (CAUGHT_and_3 == null) {								// Abnormal null body evaluation result
							if (accumulator == ValueUtil.FALSE_VALUE) {
								accumulator = null;										// Cache a null failure
							}
						}
						else if (CAUGHT_and_3 instanceof InvalidValueException) {		// Abnormal exception evaluation result
							accumulator = CAUGHT_and_3;									// Cache an exception failure
						}
						else {															// Impossible badly typed result
							accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
						}
					}
					result = exists;
				}
				else {
					result = ValueUtil.TRUE_VALUE;
				}
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_cloneInCloneable, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_1 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_1;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean noCovariantEquals(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv noCovariantEquals:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::noCovariantEquals'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = getMethods()
		 *         ->exists(method | method.name = 'equals' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
		 *           method.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean)) implies
		 *         hasEquals()
		 *       in
		 *         'ClassDeclaration::noCovariantEquals'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_noCovariantEquals);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_exists;
				try {
					final /*@NonInvalid*/ List<MethodDeclaration> getMethods = this.getMethods();
					final /*@NonInvalid*/ OrderedSetValue BOXED_getMethods = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_MethodDeclaration, getMethods);
					/*@Thrown*/ Object accumulator = ValueUtil.FALSE_VALUE;
					/*@NonNull*/ Iterator<Object> ITERATOR_method = BOXED_getMethods.iterator();
					/*@Thrown*/ Boolean exists;
					while (true) {
						if (!ITERATOR_method.hasNext()) {
							if (accumulator == null) {
								exists = null;
							}
							else if (accumulator == ValueUtil.FALSE_VALUE) {
								exists = ValueUtil.FALSE_VALUE;
							}
							else {
								throw (InvalidValueException)accumulator;
							}
							break;
						}
						/*@NonInvalid*/ MethodDeclaration method = (MethodDeclaration)ITERATOR_method.next();
						/**
						 * method.name = 'equals' and method.modifier <> null and method.modifier.visibility = VisibilityKind::public and
						 * method.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean)
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_and_0;
							try {
								final /*@NonInvalid*/ String name = method.getName();
								final /*@NonInvalid*/ boolean eq = JavaMMTables.STR_equals.equals(name);
								/*@NonInvalid*/ boolean and;
								if (eq) {
									final /*@NonInvalid*/ Modifier modifier = method.getModifier();
									final /*@NonInvalid*/ boolean ne = modifier != null;
									and = ne;
								}
								else {
									and = ValueUtil.FALSE_VALUE;
								}
								/*@Thrown*/ boolean and_0;
								if (and) {
									final /*@NonInvalid*/ Modifier modifier_0 = method.getModifier();
									if (modifier_0 == null) {
										throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
									}
									final /*@Thrown*/ VisibilityKind visibility = modifier_0.getVisibility();
									final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
									final /*@Thrown*/ boolean eq_0 = BOXED_visibility == JavaMMTables.ELITid_public;
									and_0 = eq_0;
								}
								else {
									and_0 = ValueUtil.FALSE_VALUE;
								}
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeBoolean = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeBoolean, null);
								final /*@NonInvalid*/ TypeAccess returnType = method.getReturnType();
								if (returnType == null) {
									throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
								}
								final /*@Thrown*/ Type type_0 = returnType.getType();
								final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_0, TYP_javaMM_c_c_PrimitiveTypeBoolean).booleanValue();
								CAUGHT_oclIsTypeOf = oclIsTypeOf;
							}
							catch (Exception e) {
								CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_oclIsTypeOf);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						//
						if (CAUGHT_and_1 == ValueUtil.TRUE_VALUE) {					// Normal successful body evaluation result
							exists = ValueUtil.TRUE_VALUE;
							break;														// Stop immediately
						}
						else if (CAUGHT_and_1 == ValueUtil.FALSE_VALUE) {				// Normal unsuccessful body evaluation result
							;															// Carry on
						}
						else if (CAUGHT_and_1 == null) {								// Abnormal null body evaluation result
							if (accumulator == ValueUtil.FALSE_VALUE) {
								accumulator = null;										// Cache a null failure
							}
						}
						else if (CAUGHT_and_1 instanceof InvalidValueException) {		// Abnormal exception evaluation result
							accumulator = CAUGHT_and_1;									// Cache an exception failure
						}
						else {															// Impossible badly typed result
							accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "exists");
						}
					}
					CAUGHT_exists = exists;
				}
				catch (Exception e) {
					CAUGHT_exists = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ boolean hasEquals = this.hasEquals();
				final /*@Thrown*/ Boolean result = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_exists, hasEquals);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_noCovariantEquals, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean noObscuredVariables(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv noObscuredVariables:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::noObscuredVariables'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = let
		 *           fields : OrderedSet(javaMM::FieldDeclaration) = self.bodyDeclarations->selectByKind(FieldDeclaration)
		 *         in
		 *           self.bodyDeclarations->selectByKind(AbstractMethodDeclaration)
		 *           ->forAll(md | md.body <> null implies
		 *             md.body.statements->selectByKind(VariableDeclaration)
		 *             ->forAll(vd | fields->forAll(fd | fd.name <> vd.name)))
		 *       in
		 *         'ClassDeclaration::noObscuredVariables'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_noObscuredVariables);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_forAll;
			try {
				final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_FieldDeclaration_0 = idResolver.getClass(JavaMMTables.CLSSid_FieldDeclaration, null);
				final /*@NonInvalid*/ List<BodyDeclaration> bodyDeclarations_0 = this.getBodyDeclarations();
				final /*@NonInvalid*/ OrderedSetValue BOXED_bodyDeclarations_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_BodyDeclaration, bodyDeclarations_0);
				final /*@NonInvalid*/ OrderedSetValue fields = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_bodyDeclarations_0, TYP_javaMM_c_c_FieldDeclaration_0);
				final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_AbstractMethodDeclaration = idResolver.getClass(JavaMMTables.CLSSid_AbstractMethodDeclaration, null);
				final /*@NonInvalid*/ OrderedSetValue selectByKind = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_bodyDeclarations_0, TYP_javaMM_c_c_AbstractMethodDeclaration);
				/*@Thrown*/ Object accumulator = ValueUtil.TRUE_VALUE;
				/*@NonNull*/ Iterator<Object> ITERATOR_md = selectByKind.iterator();
				/*@Thrown*/ Boolean forAll;
				while (true) {
					if (!ITERATOR_md.hasNext()) {
						if (accumulator == null) {
							forAll = null;
						}
						else if (accumulator == ValueUtil.TRUE_VALUE) {
							forAll = ValueUtil.TRUE_VALUE;
						}
						else {
							throw (InvalidValueException)accumulator;
						}
						break;
					}
					/*@NonInvalid*/ AbstractMethodDeclaration md = (AbstractMethodDeclaration)ITERATOR_md.next();
					/**
					 * md.body <> null implies
					 * md.body.statements->selectByKind(VariableDeclaration)
					 * ->forAll(vd | fields->forAll(fd | fd.name <> vd.name))
					 */
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_implies;
					try {
						final /*@NonInvalid*/ Block body = md.getBody();
						final /*@NonInvalid*/ boolean ne = body != null;
						/*@Thrown*/ Boolean implies;
						if (ne) {
							final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_VariableDeclaration = idResolver.getClass(JavaMMTables.CLSSid_VariableDeclaration, null);
							if (body == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Block::statements\'");
							}
							final /*@Thrown*/ List<Statement> statements = body.getStatements();
							final /*@Thrown*/ OrderedSetValue BOXED_statements = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_Statement, statements);
							final /*@Thrown*/ OrderedSetValue selectByKind_0 = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_statements, TYP_javaMM_c_c_VariableDeclaration);
							/*@Thrown*/ Object accumulator_0 = ValueUtil.TRUE_VALUE;
							/*@NonNull*/ Iterator<Object> ITERATOR_vd = selectByKind_0.iterator();
							/*@Thrown*/ Boolean forAll_0;
							while (true) {
								if (!ITERATOR_vd.hasNext()) {
									if (accumulator_0 == null) {
										forAll_0 = null;
									}
									else if (accumulator_0 == ValueUtil.TRUE_VALUE) {
										forAll_0 = ValueUtil.TRUE_VALUE;
									}
									else {
										throw (InvalidValueException)accumulator_0;
									}
									break;
								}
								/*@NonInvalid*/ VariableDeclaration vd = (VariableDeclaration)ITERATOR_vd.next();
								/**
								 * fields->forAll(fd | fd.name <> vd.name)
								 */
								/*@Thrown*/ Object accumulator_1 = ValueUtil.TRUE_VALUE;
								/*@NonNull*/ Iterator<Object> ITERATOR_fd = fields.iterator();
								/*@NonInvalid*/ Boolean forAll_1;
								while (true) {
									if (!ITERATOR_fd.hasNext()) {
										if (accumulator_1 == ValueUtil.TRUE_VALUE) {
											forAll_1 = ValueUtil.TRUE_VALUE;
										}
										else {
											throw (InvalidValueException)accumulator_1;
										}
										break;
									}
									/*@NonInvalid*/ FieldDeclaration fd = (FieldDeclaration)ITERATOR_fd.next();
									/**
									 * fd.name <> vd.name
									 */
									final /*@NonInvalid*/ String name = fd.getName();
									final /*@NonInvalid*/ String name_0 = vd.getName();
									final /*@NonInvalid*/ boolean ne_0 = (name != null) ? !name.equals(name_0) : (name_0 != null);
									//
									if (ne_0 == ValueUtil.FALSE_VALUE) {					// Normal unsuccessful body evaluation result
										forAll_1 = ValueUtil.FALSE_VALUE;
										break;														// Stop immediately
									}
									else if (ne_0 == ValueUtil.TRUE_VALUE) {				// Normal successful body evaluation result
										;															// Carry on
									}
									else {															// Impossible badly typed result
										accumulator_1 = new InvalidValueException(PivotMessages.NonBooleanBody, "forAll");
									}
								}
								//
								if (forAll_1 == ValueUtil.FALSE_VALUE) {					// Normal unsuccessful body evaluation result
									forAll_0 = ValueUtil.FALSE_VALUE;
									break;														// Stop immediately
								}
								else if (forAll_1 == ValueUtil.TRUE_VALUE) {				// Normal successful body evaluation result
									;															// Carry on
								}
								else if (forAll_1 == null) {								// Abnormal null body evaluation result
									if (accumulator_0 == ValueUtil.TRUE_VALUE) {
										accumulator_0 = null;										// Cache a null failure
									}
								}
								else {															// Impossible badly typed result
									accumulator_0 = new InvalidValueException(PivotMessages.NonBooleanBody, "forAll");
								}
							}
							implies = forAll_0;
						}
						else {
							implies = ValueUtil.TRUE_VALUE;
						}
						CAUGHT_implies = implies;
					}
					catch (Exception e) {
						CAUGHT_implies = ValueUtil.createInvalidValue(e);
					}
					//
					if (CAUGHT_implies == ValueUtil.FALSE_VALUE) {					// Normal unsuccessful body evaluation result
						forAll = ValueUtil.FALSE_VALUE;
						break;														// Stop immediately
					}
					else if (CAUGHT_implies == ValueUtil.TRUE_VALUE) {				// Normal successful body evaluation result
						;															// Carry on
					}
					else if (CAUGHT_implies == null) {								// Abnormal null body evaluation result
						if (accumulator == ValueUtil.TRUE_VALUE) {
							accumulator = null;										// Cache a null failure
						}
					}
					else if (CAUGHT_implies instanceof InvalidValueException) {		// Abnormal exception evaluation result
						accumulator = CAUGHT_implies;									// Cache an exception failure
					}
					else {															// Impossible badly typed result
						accumulator = new InvalidValueException(PivotMessages.NonBooleanBody, "forAll");
					}
				}
				CAUGHT_forAll = forAll;
			}
			catch (Exception e) {
				CAUGHT_forAll = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_noObscuredVariables, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_forAll, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean comparatorImplementsSerializable(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv comparatorImplementsSerializable:
		 *   let
		 *     severity : Integer[1] = 'ClassDeclaration::comparatorImplementsSerializable'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = self.implements('Comparator') implies
		 *         self.implements('Serializable')
		 *       in
		 *         'ClassDeclaration::comparatorImplementsSerializable'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_ClassDeclaration_c_c_comparatorImplementsSerializable);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_2;
		if (le) {
			symbol_2 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ boolean symbol_0 = ((AbstractTypeDeclaration)this).implements_(JavaMMTables.STR_Comparator);
			/*@NonInvalid*/ boolean result;
			if (symbol_0) {
				final /*@NonInvalid*/ boolean symbol_1 = ((AbstractTypeDeclaration)this).implements_(JavaMMTables.STR_Serializable);
				result = symbol_1;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_ClassDeclaration_c_c_comparatorImplementsSerializable, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_2 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_2;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS:
				return basicSetSuperClass(null, msgs);
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
			case JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS:
				return getSuperClass();
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
			case JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS:
				setSuperClass((TypeAccess)newValue);
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
			case JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS:
				setSuperClass((TypeAccess)null);
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
			case JavaMMPackage.CLASS_DECLARATION__SUPER_CLASS:
				return superClass != null;
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
			case JavaMMPackage.CLASS_DECLARATION___GET_METHODS:
				return getMethods();
			case JavaMMPackage.CLASS_DECLARATION___HAS_EQUALS:
				return hasEquals();
			case JavaMMPackage.CLASS_DECLARATION___HAS_HASHCODE:
				return hasHashcode();
			case JavaMMPackage.CLASS_DECLARATION___HAS_COMPARE_TO:
				return hasCompareTo();
			case JavaMMPackage.CLASS_DECLARATION___NO_COVARIANT_COMPARE_TO__DIAGNOSTICCHAIN_MAP:
				return noCovariantCompareTo((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___HASH_CODE_AND_EQUALS__DIAGNOSTICCHAIN_MAP:
				return hashCodeAndEquals((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___SERIAL_UID_IN_SERIALIZABLE_CLASS__DIAGNOSTICCHAIN_MAP:
				return serialUIDInSerializableClass((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___EQUALS_AND_COMPARE_TO__DIAGNOSTICCHAIN_MAP:
				return equalsAndCompareTo((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___NO_REDUNDANT_INTERFACE_IMPL__DIAGNOSTICCHAIN_MAP:
				return noRedundantInterfaceImpl((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___CLONE_IN_CLONEABLE__DIAGNOSTICCHAIN_MAP:
				return cloneInCloneable((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___NO_COVARIANT_EQUALS__DIAGNOSTICCHAIN_MAP:
				return noCovariantEquals((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___NO_OBSCURED_VARIABLES__DIAGNOSTICCHAIN_MAP:
				return noObscuredVariables((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.CLASS_DECLARATION___COMPARATOR_IMPLEMENTS_SERIALIZABLE__DIAGNOSTICCHAIN_MAP:
				return comparatorImplementsSerializable((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

} //ClassDeclarationImpl
