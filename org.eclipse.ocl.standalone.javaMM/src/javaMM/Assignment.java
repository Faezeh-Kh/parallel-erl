/**
 */
package javaMM;

import java.util.Map;

import org.eclipse.emf.common.util.DiagnosticChain;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Assignment</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Assignment#getLeftHandSide <em>Left Hand Side</em>}</li>
 *   <li>{@link javaMM.Assignment#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.Assignment#getRightHandSide <em>Right Hand Side</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAssignment()
 * @model annotation="http://www.eclipse.org/emf/2002/Ecore constraints='noRedundantAssignment'"
 * @generated
 */
public interface Assignment extends Expression {
	/**
	 * Returns the value of the '<em><b>Left Hand Side</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Left Hand Side</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Left Hand Side</em>' containment reference.
	 * @see #setLeftHandSide(Expression)
	 * @see javaMM.JavaMMPackage#getAssignment_LeftHandSide()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Assignment!leftHandSide'"
	 * @generated
	 */
	Expression getLeftHandSide();

	/**
	 * Sets the value of the '{@link javaMM.Assignment#getLeftHandSide <em>Left Hand Side</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Left Hand Side</em>' containment reference.
	 * @see #getLeftHandSide()
	 * @generated
	 */
	void setLeftHandSide(Expression value);

	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.AssignmentKind}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Operator</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see javaMM.AssignmentKind
	 * @see #setOperator(AssignmentKind)
	 * @see javaMM.JavaMMPackage#getAssignment_Operator()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Assignment!operator'"
	 * @generated
	 */
	AssignmentKind getOperator();

	/**
	 * Sets the value of the '{@link javaMM.Assignment#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see javaMM.AssignmentKind
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(AssignmentKind value);

	/**
	 * Returns the value of the '<em><b>Right Hand Side</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Right Hand Side</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Right Hand Side</em>' containment reference.
	 * @see #setRightHandSide(Expression)
	 * @see javaMM.JavaMMPackage#getAssignment_RightHandSide()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Assignment!rightHandSide'"
	 * @generated
	 */
	Expression getRightHandSide();

	/**
	 * Sets the value of the '{@link javaMM.Assignment#getRightHandSide <em>Right Hand Side</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Right Hand Side</em>' containment reference.
	 * @see #getRightHandSide()
	 * @generated
	 */
	void setRightHandSide(Expression value);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv noRedundantAssignment:\n *   let\n *     severity : Integer[1] = \'Assignment::noRedundantAssignment\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let result : Boolean[?] = self.operator = AssignmentKind::ASSIGN implies self.leftHandSide &lt;&gt; self.rightHandSide\n *       in\n *         \'Assignment::noRedundantAssignment\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_Assignment_c_c_noRedundantAssignment);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\tfinal /*@NonInvalid\052/ &lt;%javaMM.AssignmentKind%&gt; operator = this.getOperator();\n\tfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.EnumerationLiteralId%&gt; BOXED_operator = operator == null ? null : &lt;%javaMM.JavaMMTables%&gt;.ENUMid_AssignmentKind.getEnumerationLiteralId(&lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState(operator.getName()));\n\tfinal /*@NonInvalid\052/ boolean eq = BOXED_operator == &lt;%javaMM.JavaMMTables%&gt;.ELITid_ASSIGN;\n\t/*@NonInvalid\052/ boolean result;\n\tif (eq) {\n\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; leftHandSide = this.getLeftHandSide();\n\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Expression%&gt; rightHandSide = this.getRightHandSide();\n\t\tfinal /*@NonInvalid\052/ boolean ne = (leftHandSide != null) ? !leftHandSide.equals(rightHandSide) : (rightHandSide != null);\n\t\tresult = ne;\n\t}\n\telse {\n\t\tresult = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_Assignment_c_c_noRedundantAssignment, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean noRedundantAssignment(DiagnosticChain diagnostics, Map<Object, Object> context);

} // Assignment
