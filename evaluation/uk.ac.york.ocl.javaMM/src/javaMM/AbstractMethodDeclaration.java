/**
 */
package javaMM;

import java.util.Map;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getBody <em>Body</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getParameters <em>Parameters</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getThrownExceptions <em>Thrown Exceptions</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getTypeParameters <em>Type Parameters</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getUsagesInDocComments <em>Usages In Doc Comments</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getUsages <em>Usages</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration()
 * @model abstract="true"
 *        annotation="http://www.eclipse.org/emf/2002/Ecore constraints='localMethodIsUsed'"
 * @generated
 */
public interface AbstractMethodDeclaration extends BodyDeclaration {
	/**
	 * Returns the value of the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Body</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body</em>' containment reference.
	 * @see #setBody(Block)
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Body()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!body'"
	 * @generated
	 */
	Block getBody();

	/**
	 * Sets the value of the '{@link javaMM.AbstractMethodDeclaration#getBody <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Body</em>' containment reference.
	 * @see #getBody()
	 * @generated
	 */
	void setBody(Block value);

	/**
	 * Returns the value of the '<em><b>Parameters</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.SingleVariableDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.SingleVariableDeclaration#getMethodDeclaration <em>Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Parameters</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parameters</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Parameters()
	 * @see javaMM.SingleVariableDeclaration#getMethodDeclaration
	 * @model opposite="methodDeclaration" containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!parameters'"
	 * @generated
	 */
	EList<SingleVariableDeclaration> getParameters();

	/**
	 * Returns the value of the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeAccess}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Thrown Exceptions</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Thrown Exceptions</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_ThrownExceptions()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!thrownExceptions'"
	 * @generated
	 */
	EList<TypeAccess> getThrownExceptions();

	/**
	 * Returns the value of the '<em><b>Type Parameters</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeParameter}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Type Parameters</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type Parameters</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_TypeParameters()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!typeParameters'"
	 * @generated
	 */
	EList<TypeParameter> getTypeParameters();

	/**
	 * Returns the value of the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * The list contents are of type {@link javaMM.MethodRef}.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodRef#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Usages In Doc Comments</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages In Doc Comments</em>' reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_UsagesInDocComments()
	 * @see javaMM.MethodRef#getMethod
	 * @model opposite="method" ordered="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!usagesInDocComments'"
	 * @generated
	 */
	EList<MethodRef> getUsagesInDocComments();

	/**
	 * Returns the value of the '<em><b>Usages</b></em>' reference list.
	 * The list contents are of type {@link javaMM.AbstractMethodInvocation}.
	 * It is bidirectional and its opposite is '{@link javaMM.AbstractMethodInvocation#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Usages</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages</em>' reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Usages()
	 * @see javaMM.AbstractMethodInvocation#getMethod
	 * @model opposite="method" ordered="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractMethodDeclaration!usages'"
	 * @generated
	 */
	EList<AbstractMethodInvocation> getUsages();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv parametersEffectivelyFinal:\n *   let\n *     severity : Integer[1] = \'AbstractMethodDeclaration::parametersEffectivelyFinal\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let result : Boolean[?] = self.body &lt;&gt; null and\n *         self.parameters-&gt;notEmpty() implies\n *         self.body.statements-&gt;selectByKind(ExpressionStatement)\n *         -&gt;collect(s | s.expression)\n *         -&gt;selectByKind(Assignment)\n *         -&gt;select(assignment |\n *           assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and assignment.operator = AssignmentKind::ASSIGN and\n *           assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and\n *           self.parameters-&gt;excludes(\n *             assignment.leftHandSide.oclAsType(SingleVariableAccess).variable))\n *         -&gt;isEmpty()\n *       in\n *         \'AbstractMethodDeclaration::parametersEffectivelyFinal\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.IdResolver%&gt; idResolver = executor.getIdResolver();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_AbstractMethodDeclaration_c_c_parametersEffectivelyFinal);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_result;\n\ttry {\n\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Block%&gt; body = this.getBody();\n\t\tfinal /*@NonInvalid\052/ boolean ne = body != null;\n\t\t/*@NonInvalid\052/ boolean and;\n\t\tif (ne) {\n\t\t\tfinal /*@NonInvalid\052/ &lt;%java.util.List%&gt;&lt;&lt;%javaMM.SingleVariableDeclaration%&gt;&gt; parameters = this.getParameters();\n\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt; BOXED_parameters = idResolver.createOrderedSetOfAll(&lt;%javaMM.JavaMMTables%&gt;.ORD_CLSSid_SingleVariableDeclaration, parameters);\n\t\t\tfinal /*@NonInvalid\052/ boolean notEmpty = &lt;%org.eclipse.ocl.pivot.library.collection.CollectionNotEmptyOperation%&gt;.INSTANCE.evaluate(BOXED_parameters).booleanValue();\n\t\t\tand = notEmpty;\n\t\t}\n\t\telse {\n\t\t\tand = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE;\n\t\t}\n\t\t/*@Thrown\052/ boolean result;\n\t\tif (and) {\n\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_Assignment = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_Assignment, null);\n\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_ExpressionStatement = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_ExpressionStatement, null);\n\t\t\tif (body == null) {\n\t\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null source for \\\'\\\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\\\'::Block::statements\\\'\");\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ &lt;%java.util.List%&gt;&lt;&lt;%javaMM.Statement%&gt;&gt; statements = body.getStatements();\n\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt; BOXED_statements = idResolver.createOrderedSetOfAll(&lt;%javaMM.JavaMMTables%&gt;.ORD_CLSSid_Statement, statements);\n\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt; selectByKind = (&lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt;)&lt;%org.eclipse.ocl.pivot.library.collection.CollectionSelectByKindOperation%&gt;.INSTANCE.evaluate(executor, BOXED_statements, TYP_javaMM_c_c_ExpressionStatement);\n\t\t\t/*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt;.Accumulator accumulator = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createSequenceAccumulatorValue(&lt;%javaMM.JavaMMTables%&gt;.SEQ_CLSSid_Expression);\n\t\t\t/*@NonNull\052/ &lt;%java.util.Iterator%&gt;&lt;&lt;%java.lang.Object%&gt;&gt; ITERATOR_s = selectByKind.iterator();\n\t\t\t/*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt; collect;\n\t\t\twhile (true) {\n\t\t\t\tif (!ITERATOR_s.hasNext()) {\n\t\t\t\t\tcollect = accumulator;\n\t\t\t\t\tbreak;\n\t\t\t\t}\n\t\t\t\t/*@NonInvalid\052/ &lt;%javaMM.ExpressionStatement%&gt; s = (&lt;%javaMM.ExpressionStatement%&gt;)ITERATOR_s.next();\n\t\t\t\t/**\n\t\t\t\t * s.expression\n\t\t\t\t \052/\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; expression = s.getExpression();\n\t\t\t\t//\n\t\t\t\taccumulator.add(expression);\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt; selectByKind_0 = (&lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt;)&lt;%org.eclipse.ocl.pivot.library.collection.CollectionSelectByKindOperation%&gt;.INSTANCE.evaluate(executor, collect, TYP_javaMM_c_c_Assignment);\n\t\t\t/*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt;.Accumulator accumulator_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createSequenceAccumulatorValue(&lt;%javaMM.JavaMMTables%&gt;.SEQ_CLSSid_Assignment);\n\t\t\t/*@NonNull\052/ &lt;%java.util.Iterator%&gt;&lt;&lt;%java.lang.Object%&gt;&gt; ITERATOR_assignment = selectByKind_0.iterator();\n\t\t\t/*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.values.SequenceValue%&gt; select;\n\t\t\twhile (true) {\n\t\t\t\tif (!ITERATOR_assignment.hasNext()) {\n\t\t\t\t\tselect = accumulator_0;\n\t\t\t\t\tbreak;\n\t\t\t\t}\n\t\t\t\t/*@NonInvalid\052/ &lt;%javaMM.Assignment%&gt; assignment = (&lt;%javaMM.Assignment%&gt;)ITERATOR_assignment.next();\n\t\t\t\t/**\n\t\t\t\t *\n\t\t\t\t * assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and assignment.operator = AssignmentKind::ASSIGN and\n\t\t\t\t * assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and\n\t\t\t\t * self.parameters-&gt;excludes(\n\t\t\t\t *   assignment.leftHandSide.oclAsType(SingleVariableAccess).variable)\n\t\t\t\t \052/\n\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_and_1;\n\t\t\t\ttry {\n\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsKindOf_0;\n\t\t\t\t\ttry {\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_SingleVariableAccess_0 = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_SingleVariableAccess, null);\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftHandSide_0 = assignment.getLeftHandSide();\n\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsKindOf_0 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsKindOfOperation%&gt;.INSTANCE.evaluate(executor, leftHandSide_0, TYP_javaMM_c_c_SingleVariableAccess_0).booleanValue();\n\t\t\t\t\t\tCAUGHT_oclIsKindOf_0 = oclIsKindOf_0;\n\t\t\t\t\t}\n\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\tCAUGHT_oclIsKindOf_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t}\n\t\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_and_0;\n\t\t\t\t\ttry {\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.AssignmentKind%&gt; operator = assignment.getOperator();\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.EnumerationLiteralId%&gt; BOXED_operator = operator == null ? null : &lt;%javaMM.JavaMMTables%&gt;.ENUMid_AssignmentKind.getEnumerationLiteralId(&lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState(operator.getName()));\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ boolean eq = BOXED_operator == &lt;%javaMM.JavaMMTables%&gt;.ELITid_ASSIGN;\n\t\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; and_0 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanAndOperation%&gt;.INSTANCE.evaluate(CAUGHT_oclIsKindOf_0, eq);\n\t\t\t\t\t\tCAUGHT_and_0 = and_0;\n\t\t\t\t\t}\n\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\tCAUGHT_and_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t}\n\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; and_1 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanAndOperation%&gt;.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_oclIsKindOf_0);\n\t\t\t\t\tCAUGHT_and_1 = and_1;\n\t\t\t\t}\n\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\tCAUGHT_and_1 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t}\n\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_excludes;\n\t\t\t\ttry {\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_SingleVariableAccess_1 = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_SingleVariableAccess, null);\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%java.util.List%&gt;&lt;&lt;%javaMM.SingleVariableDeclaration%&gt;&gt; parameters_0 = this.getParameters();\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt; BOXED_parameters_0 = idResolver.createOrderedSetOfAll(&lt;%javaMM.JavaMMTables%&gt;.ORD_CLSSid_SingleVariableDeclaration, parameters_0);\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftHandSide_1 = assignment.getLeftHandSide();\n\t\t\t\t\tfinal /*@Thrown\052/ &lt;%javaMM.SingleVariableAccess%&gt; oclAsType = &lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState((&lt;%javaMM.SingleVariableAccess%&gt;)&lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclAsTypeOperation%&gt;.INSTANCE.evaluate(executor, leftHandSide_1, TYP_javaMM_c_c_SingleVariableAccess_1));\n\t\t\t\t\tfinal /*@Thrown\052/ &lt;%javaMM.VariableDeclaration%&gt; variable = oclAsType.getVariable();\n\t\t\t\t\tfinal /*@Thrown\052/ boolean excludes = &lt;%org.eclipse.ocl.pivot.library.collection.CollectionExcludesOperation%&gt;.INSTANCE.evaluate(BOXED_parameters_0, variable).booleanValue();\n\t\t\t\t\tCAUGHT_excludes = excludes;\n\t\t\t\t}\n\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\tCAUGHT_excludes = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t}\n\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; and_2 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanAndOperation%&gt;.INSTANCE.evaluate(CAUGHT_and_1, CAUGHT_excludes);\n\t\t\t\tif (and_2 == null) {\n\t\t\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null body for \\\'Sequence(T).select(Sequence.T[?] | Lambda T() : Boolean[1]) : Sequence(T)\\\'\");\n\t\t\t\t}\n\t\t\t\t//\n\t\t\t\tif (and_2 == &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE) {\n\t\t\t\t\taccumulator_0.add(assignment);\n\t\t\t\t}\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ boolean isEmpty = &lt;%org.eclipse.ocl.pivot.library.collection.CollectionIsEmptyOperation%&gt;.INSTANCE.evaluate(select).booleanValue();\n\t\t\tresult = isEmpty;\n\t\t}\n\t\telse {\n\t\t\tresult = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n\t\t}\n\t\tCAUGHT_result = result;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_result = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_AbstractMethodDeclaration_c_c_parametersEffectivelyFinal, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean parametersEffectivelyFinal(DiagnosticChain diagnostics, Map<Object, Object> context);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv localMethodIsUsed:\n *   let\n *     severity : Integer[1] = \'AbstractMethodDeclaration::localMethodIsUsed\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let result : Boolean[?] = self.modifier &lt;&gt; null and\n *         self.modifier.isLocal() implies\n *         self.usages-&gt;notEmpty()\n *       in\n *         \'AbstractMethodDeclaration::localMethodIsUsed\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.IdResolver%&gt; idResolver = executor.getIdResolver();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_AbstractMethodDeclaration_c_c_localMethodIsUsed);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_result;\n\ttry {\n\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_and;\n\t\ttry {\n\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Modifier%&gt; modifier = this.getModifier();\n\t\t\tfinal /*@NonInvalid\052/ boolean ne = modifier != null;\n\t\t\t/*@Thrown\052/ boolean and;\n\t\t\tif (ne) {\n\t\t\t\tif (modifier == null) {\n\t\t\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null source for \\\'javaMM::Modifier::isLocal() : Boolean[?]\\\'\");\n\t\t\t\t}\n\t\t\t\tfinal /*@Thrown\052/ boolean isLocal = modifier.isLocal();\n\t\t\t\tand = isLocal;\n\t\t\t}\n\t\t\telse {\n\t\t\t\tand = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE;\n\t\t\t}\n\t\t\tCAUGHT_and = and;\n\t\t}\n\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\tCAUGHT_and = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t}\n\t\tfinal /*@NonInvalid\052/ &lt;%java.util.List%&gt;&lt;&lt;%javaMM.AbstractMethodInvocation%&gt;&gt; usages = this.getUsages();\n\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.SetValue%&gt; BOXED_usages = idResolver.createSetOfAll(&lt;%javaMM.JavaMMTables%&gt;.SET_CLSSid_AbstractMethodInvocation, usages);\n\t\tfinal /*@NonInvalid\052/ boolean notEmpty = &lt;%org.eclipse.ocl.pivot.library.collection.CollectionNotEmptyOperation%&gt;.INSTANCE.evaluate(BOXED_usages).booleanValue();\n\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; result = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanImpliesOperation%&gt;.INSTANCE.evaluate(CAUGHT_and, notEmpty);\n\t\tCAUGHT_result = result;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_result = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_AbstractMethodDeclaration_c_c_localMethodIsUsed, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean localMethodIsUsed(DiagnosticChain diagnostics, Map<Object, Object> context);

} // AbstractMethodDeclaration
