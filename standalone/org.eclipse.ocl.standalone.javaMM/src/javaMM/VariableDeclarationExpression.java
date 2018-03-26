/**
 */
package javaMM;

import java.util.Map;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Declaration Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.VariableDeclarationExpression#getModifier <em>Modifier</em>}</li>
 *   <li>{@link javaMM.VariableDeclarationExpression#getAnnotations <em>Annotations</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getVariableDeclarationExpression()
 * @model annotation="http://www.eclipse.org/emf/2002/Ecore constraints='publicVariableIsFinal'"
 * @generated
 */
public interface VariableDeclarationExpression extends Expression, AbstractVariablesContainer {
	/**
	 * Returns the value of the '<em><b>Modifier</b></em>' containment reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Modifier#getVariableDeclarationExpression <em>Variable Declaration Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Modifier</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Modifier</em>' containment reference.
	 * @see #setModifier(Modifier)
	 * @see javaMM.JavaMMPackage#getVariableDeclarationExpression_Modifier()
	 * @see javaMM.Modifier#getVariableDeclarationExpression
	 * @model opposite="variableDeclarationExpression" containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!VariableDeclarationExpression!modifier'"
	 * @generated
	 */
	Modifier getModifier();

	/**
	 * Sets the value of the '{@link javaMM.VariableDeclarationExpression#getModifier <em>Modifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Modifier</em>' containment reference.
	 * @see #getModifier()
	 * @generated
	 */
	void setModifier(Modifier value);

	/**
	 * Returns the value of the '<em><b>Annotations</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Annotation}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Annotations</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Annotations</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getVariableDeclarationExpression_Annotations()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!VariableDeclarationExpression!annotations'"
	 * @generated
	 */
	EList<Annotation> getAnnotations();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv publicVariableIsFinal:\n *   let\n *     severity : Integer[1] = \'VariableDeclarationExpression::publicVariableIsFinal\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let result : Boolean[?] = self.modifier.visibility = VisibilityKind::public implies self.modifier.inheritance = InheritanceKind::final\n *       in\n *         \'VariableDeclarationExpression::publicVariableIsFinal\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_VariableDeclarationExpression_c_c_publicVariableIsFinal);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\t/*@Caught\052/ /*@Nullable\052/ &lt;%java.lang.Object%&gt; CAUGHT_result;\n\ttry {\n\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_eq;\n\t\ttry {\n\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Modifier%&gt; modifier = this.getModifier();\n\t\t\tif (modifier == null) {\n\t\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null source for \\\'\\\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\\\'::Modifier::visibility\\\'\");\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ &lt;%javaMM.VisibilityKind%&gt; visibility = modifier.getVisibility();\n\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.ids.EnumerationLiteralId%&gt; BOXED_visibility = visibility == null ? null : &lt;%javaMM.JavaMMTables%&gt;.ENUMid_VisibilityKind.getEnumerationLiteralId(&lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState(visibility.getName()));\n\t\t\tfinal /*@Thrown\052/ boolean eq = BOXED_visibility == &lt;%javaMM.JavaMMTables%&gt;.ELITid_public;\n\t\t\tCAUGHT_eq = eq;\n\t\t}\n\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\tCAUGHT_eq = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t}\n\t\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_eq_0;\n\t\ttry {\n\t\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Modifier%&gt; modifier_0 = this.getModifier();\n\t\t\tif (modifier_0 == null) {\n\t\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null source for \\\'\\\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\\\'::Modifier::inheritance\\\'\");\n\t\t\t}\n\t\t\tfinal /*@Thrown\052/ &lt;%javaMM.InheritanceKind%&gt; inheritance = modifier_0.getInheritance();\n\t\t\tfinal /*@Thrown\052/ &lt;%org.eclipse.ocl.pivot.ids.EnumerationLiteralId%&gt; BOXED_inheritance = inheritance == null ? null : &lt;%javaMM.JavaMMTables%&gt;.ENUMid_InheritanceKind.getEnumerationLiteralId(&lt;%org.eclipse.ocl.pivot.utilities.ClassUtil%&gt;.nonNullState(inheritance.getName()));\n\t\t\tfinal /*@Thrown\052/ boolean eq_0 = BOXED_inheritance == &lt;%javaMM.JavaMMTables%&gt;.ELITid_final;\n\t\t\tCAUGHT_eq_0 = eq_0;\n\t\t}\n\t\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\t\tCAUGHT_eq_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t\t}\n\t\tfinal /*@Thrown\052/ &lt;%java.lang.Boolean%&gt; result = &lt;%org.eclipse.ocl.pivot.library.logical.BooleanImpliesOperation%&gt;.INSTANCE.evaluate(CAUGHT_eq, CAUGHT_eq_0);\n\t\tCAUGHT_result = result;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_result = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_VariableDeclarationExpression_c_c_publicVariableIsFinal, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean publicVariableIsFinal(DiagnosticChain diagnostics, Map<Object, Object> context);

} // VariableDeclarationExpression
