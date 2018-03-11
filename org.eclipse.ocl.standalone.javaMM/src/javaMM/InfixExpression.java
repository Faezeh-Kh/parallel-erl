/**
 */
package javaMM;

import java.util.Map;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Infix Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.InfixExpression#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getRightOperand <em>Right Operand</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getLeftOperand <em>Left Operand</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getExtendedOperands <em>Extended Operands</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getInfixExpression()
 * @model annotation="http://www.eclipse.org/emf/2002/Ecore constraints='equalsNotOnStrings'"
 * @generated
 */
public interface InfixExpression extends Expression {
	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.InfixExpressionKind}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Operator</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see javaMM.InfixExpressionKind
	 * @see #setOperator(InfixExpressionKind)
	 * @see javaMM.JavaMMPackage#getInfixExpression_Operator()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!InfixExpression!operator'"
	 * @generated
	 */
	InfixExpressionKind getOperator();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see javaMM.InfixExpressionKind
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(InfixExpressionKind value);

	/**
	 * Returns the value of the '<em><b>Right Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Right Operand</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Right Operand</em>' containment reference.
	 * @see #setRightOperand(Expression)
	 * @see javaMM.JavaMMPackage#getInfixExpression_RightOperand()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!InfixExpression!rightOperand'"
	 * @generated
	 */
	Expression getRightOperand();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getRightOperand <em>Right Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Right Operand</em>' containment reference.
	 * @see #getRightOperand()
	 * @generated
	 */
	void setRightOperand(Expression value);

	/**
	 * Returns the value of the '<em><b>Left Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Left Operand</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Left Operand</em>' containment reference.
	 * @see #setLeftOperand(Expression)
	 * @see javaMM.JavaMMPackage#getInfixExpression_LeftOperand()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!InfixExpression!leftOperand'"
	 * @generated
	 */
	Expression getLeftOperand();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getLeftOperand <em>Left Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Left Operand</em>' containment reference.
	 * @see #getLeftOperand()
	 * @generated
	 */
	void setLeftOperand(Expression value);

	/**
	 * Returns the value of the '<em><b>Extended Operands</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Expression}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Extended Operands</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Extended Operands</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getInfixExpression_ExtendedOperands()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!InfixExpression!extendedOperands'"
	 * @generated
	 */
	EList<Expression> getExtendedOperands();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n * self.operator = InfixExpressionKind::EQUALS or self.operator = InfixExpressionKind::NOT_EQUALS\n \052/\nfinal /*@NonInvalid\052/ &lt;%javaMM.InfixExpressionKind%&gt; operator_0 = this.getOperator();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.EnumerationLiteralId%&gt; BOXED_operator_0 = operator_0 == null ? null : &lt;%javaMM.JavaMMTables%&gt;.ENUMid_InfixExpressionKind.getEnumerationLiteralId(&lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState(operator_0.getName()));\nfinal /*@NonInvalid\052/ boolean eq = BOXED_operator_0 == &lt;%javaMM.JavaMMTables%&gt;.ELITid_EQUALS;\n/*@NonInvalid\052/ boolean or;\nif (eq) {\n\tor = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\tfinal /*@NonInvalid\052/ boolean eq_0 = BOXED_operator_0 == &lt;%javaMM.JavaMMTables%&gt;.ELITid_NOT_EQUALS;\n\tor = eq_0;\n}\nreturn or;'"
	 * @generated
	 */
	Boolean operatorIsEquality();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv equalsNotOnLiterals:\n *   let\n *     severity : Integer[1] = \'InfixExpression::equalsNotOnLiterals\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let\n *         result : Boolean[?] = operatorIsEquality() implies\n *         self.leftOperand.oclIsTypeOf(self.rightOperand.oclType()) and\n *         not (\n *           self.leftOperand.oclIsTypeOf(StringLiteral) or\n *           self.leftOperand.oclIsTypeOf(NumberLiteral) or\n *           self.leftOperand.oclIsTypeOf(CharacterLiteral) or\n *           self.leftOperand.oclIsTypeOf(BooleanLiteral) or\n *           self.leftOperand.oclIsTypeOf(NullLiteral) or\n *           self.leftOperand.oclIsTypeOf(TypeLiteral)\n *         )\n *       in\n *         \'InfixExpression::equalsNotOnLiterals\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.IdResolver%&gt; idResolver = executor.getIdResolver();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_equalsNotOnLiterals);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_result;\n\ttry {\n\t\tfinal /*@NonInvalid\052/ boolean operatorIsEquality = this.operatorIsEquality();\n\t\t/*@Thrown\052/ &lt;%java.lang.Boolean%&gt; result;\n\t\tif (operatorIsEquality) {\n\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf;\n\t\t\ttry {\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand = this.getLeftOperand();\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; rightOperand = this.getRightOperand();\n\t\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; oclType = (&lt;%org.eclipse.ocl.pivot.Class%&gt;)&lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclTypeOperation%&gt;.INSTANCE.evaluate(executor, rightOperand);\n\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand, oclType).booleanValue();\n\t\t\t\tCAUGHT_oclIsTypeOf = oclIsTypeOf;\n\t\t\t}\n\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\tCAUGHT_oclIsTypeOf = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t}\n\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or_3;\n\t\t\ttry {\n\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or_2;\n\t\t\t\ttry {\n\t\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or_1;\n\t\t\t\t\ttry {\n\t\t\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or_0;\n\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or;\n\t\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_0;\n\t\t\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_StringLiteral = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_StringLiteral, null);\n\t\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_0 = this.getLeftOperand();\n\t\t\t\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_0 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_0, TYP_javaMM_c_c_StringLiteral).booleanValue();\n\t\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_0 = oclIsTypeOf_0;\n\t\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_1;\n\t\t\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_NumberLiteral = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_NumberLiteral, null);\n\t\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_1 = this.getLeftOperand();\n\t\t\t\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_1 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_1, TYP_javaMM_c_c_NumberLiteral).booleanValue();\n\t\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_1 = oclIsTypeOf_1;\n\t\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_1 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_oclIsTypeOf_0, CAUGHT_oclIsTypeOf_1);\n\t\t\t\t\t\t\t\tCAUGHT_or = or;\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\t\tCAUGHT_or = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_2;\n\t\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_CharacterLiteral = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_CharacterLiteral, null);\n\t\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_2 = this.getLeftOperand();\n\t\t\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_2 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_2, TYP_javaMM_c_c_CharacterLiteral).booleanValue();\n\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_2 = oclIsTypeOf_2;\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_2 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or_0 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_or, CAUGHT_oclIsTypeOf_2);\n\t\t\t\t\t\t\tCAUGHT_or_0 = or_0;\n\t\t\t\t\t\t}\n\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\tCAUGHT_or_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t}\n\t\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_3;\n\t\t\t\t\t\ttry {\n\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_BooleanLiteral = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_BooleanLiteral, null);\n\t\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_3 = this.getLeftOperand();\n\t\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_3 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_3, TYP_javaMM_c_c_BooleanLiteral).booleanValue();\n\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_3 = oclIsTypeOf_3;\n\t\t\t\t\t\t}\n\t\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\t\tCAUGHT_oclIsTypeOf_3 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t\t}\n\t\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or_1 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_or_0, CAUGHT_oclIsTypeOf_3);\n\t\t\t\t\t\tCAUGHT_or_1 = or_1;\n\t\t\t\t\t}\n\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\tCAUGHT_or_1 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t}\n\t\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_4;\n\t\t\t\t\ttry {\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_NullLiteral = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_NullLiteral, null);\n\t\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_4 = this.getLeftOperand();\n\t\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_4 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_4, TYP_javaMM_c_c_NullLiteral).booleanValue();\n\t\t\t\t\t\tCAUGHT_oclIsTypeOf_4 = oclIsTypeOf_4;\n\t\t\t\t\t}\n\t\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\t\tCAUGHT_oclIsTypeOf_4 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t\t}\n\t\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or_2 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_or_1, CAUGHT_oclIsTypeOf_4);\n\t\t\t\t\tCAUGHT_or_2 = or_2;\n\t\t\t\t}\n\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\tCAUGHT_or_2 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t}\n\t\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_5;\n\t\t\t\ttry {\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_TypeLiteral_0 = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_TypeLiteral, null);\n\t\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand_5 = this.getLeftOperand();\n\t\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_5 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand_5, TYP_javaMM_c_c_TypeLiteral_0).booleanValue();\n\t\t\t\t\tCAUGHT_oclIsTypeOf_5 = oclIsTypeOf_5;\n\t\t\t\t}\n\t\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\t\tCAUGHT_oclIsTypeOf_5 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t\t}\n\t\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or_3 = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_or_2, CAUGHT_oclIsTypeOf_5);\n\t\t\t\tCAUGHT_or_3 = or_3;\n\t\t\t}\n\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\tCAUGHT_or_3 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t}\n\t\t\tfinal /*@NonInvalid\052/ &lt;%java.lang.Boolean%&gt; not = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanNotOperation%&gt;.INSTANCE.evaluate(CAUGHT_or_3);\n\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; and = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanAndOperation%&gt;.INSTANCE.evaluate(CAUGHT_oclIsTypeOf, not);\n\t\t\tresult = and;\n\t\t}\n\t\telse {\n\t\t\tresult = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n\t\t}\n\t\tCAUGHT_result = result;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_result = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_equalsNotOnLiterals, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean equalsNotOnLiterals(DiagnosticChain diagnostics, Map<Object, Object> context);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv noRedundantComparison:\n *   let\n *     severity : Integer[1] = \'InfixExpression::noRedundantComparison\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let result : Boolean[1] = self.leftOperand &lt;&gt; self.rightOperand\n *       in\n *         \'InfixExpression::noRedundantComparison\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_noRedundantComparison);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand = this.getLeftOperand();\n\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; rightOperand = this.getRightOperand();\n\tfinal /*@NonInvalid\052/ boolean result = (leftOperand != null) ? !leftOperand.equals(rightOperand) : (rightOperand != null);\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_noRedundantComparison, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean noRedundantComparison(DiagnosticChain diagnostics, Map<Object, Object> context);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv equalsNotOnStrings:\n *   let\n *     severity : Integer[1] = \'InfixExpression::equalsNotOnStrings\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let\n *         result : Boolean[?] = operatorIsEquality() implies\n *         not (\n *           self.leftOperand.oclIsTypeOf(StringLiteral) or\n *           self.rightOperand.oclIsTypeOf(StringLiteral)\n *         )\n *       in\n *         \'InfixExpression::equalsNotOnStrings\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.IdResolver%&gt; idResolver = executor.getIdResolver();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_equalsNotOnStrings);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\tfinal /*@NonInvalid\052/ boolean operatorIsEquality = this.operatorIsEquality();\n\t/*@NonInvalid\052/ &lt;%java.lang.Boolean%&gt; result;\n\tif (operatorIsEquality) {\n\t\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_or;\n\t\ttry {\n\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf;\n\t\t\ttry {\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_StringLiteral_0 = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_StringLiteral, null);\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftOperand = this.getLeftOperand();\n\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, leftOperand, TYP_javaMM_c_c_StringLiteral_0).booleanValue();\n\t\t\t\tCAUGHT_oclIsTypeOf = oclIsTypeOf;\n\t\t\t}\n\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\tCAUGHT_oclIsTypeOf = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t}\n\t\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_oclIsTypeOf_0;\n\t\t\ttry {\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.Class%&gt; TYP_javaMM_c_c_StringLiteral_1 = idResolver.getClass(&lt;%javaMM.JavaMMTables%&gt;.CLSSid_StringLiteral, null);\n\t\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; rightOperand = this.getRightOperand();\n\t\t\t\tfinal /*@Thrown\052/ boolean oclIsTypeOf_0 = &lt;%org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation%&gt;.INSTANCE.evaluate(executor, rightOperand, TYP_javaMM_c_c_StringLiteral_1).booleanValue();\n\t\t\t\tCAUGHT_oclIsTypeOf_0 = oclIsTypeOf_0;\n\t\t\t}\n\t\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\t\tCAUGHT_oclIsTypeOf_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; or = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanOrOperation%&gt;.INSTANCE.evaluate(CAUGHT_oclIsTypeOf, CAUGHT_oclIsTypeOf_0);\n\t\t\tCAUGHT_or = or;\n\t\t}\n\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\tCAUGHT_or = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t}\n\t\tfinal /*@NonInvalid\052/ &lt;%java.lang.Boolean%&gt; not = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanNotOperation%&gt;.INSTANCE.evaluate(CAUGHT_or);\n\t\tresult = not;\n\t}\n\telse {\n\t\tresult = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_InfixExpression_c_c_equalsNotOnStrings, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean equalsNotOnStrings(DiagnosticChain diagnostics, Map<Object, Object> context);

} // InfixExpression
