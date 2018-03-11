/**
 */
package javaMM;

import java.util.Map;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.MethodDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getReturnType <em>Return Type</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getRedefinitions <em>Redefinitions</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getMethodDeclaration()
 * @model annotation="http://www.eclipse.org/emf/2002/Ecore constraints='shouldStartWithLowerCase'"
 * @generated
 */
public interface MethodDeclaration extends AbstractMethodDeclaration {
	/**
	 * Returns the value of the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Extra Array Dimensions</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #setExtraArrayDimensions(int)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_ExtraArrayDimensions()
	 * @model required="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!MethodDeclaration!extraArrayDimensions'"
	 * @generated
	 */
	int getExtraArrayDimensions();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #getExtraArrayDimensions()
	 * @generated
	 */
	void setExtraArrayDimensions(int value);

	/**
	 * Returns the value of the '<em><b>Return Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Return Type</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Return Type</em>' containment reference.
	 * @see #setReturnType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_ReturnType()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!MethodDeclaration!returnType'"
	 * @generated
	 */
	TypeAccess getReturnType();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getReturnType <em>Return Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Return Type</em>' containment reference.
	 * @see #getReturnType()
	 * @generated
	 */
	void setReturnType(TypeAccess value);

	/**
	 * Returns the value of the '<em><b>Redefined Method Declaration</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodDeclaration#getRedefinitions <em>Redefinitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Redefined Method Declaration</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Redefined Method Declaration</em>' reference.
	 * @see #setRedefinedMethodDeclaration(MethodDeclaration)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_RedefinedMethodDeclaration()
	 * @see javaMM.MethodDeclaration#getRedefinitions
	 * @model opposite="redefinitions"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!MethodDeclaration!redefinedMethodDeclaration'"
	 * @generated
	 */
	MethodDeclaration getRedefinedMethodDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Redefined Method Declaration</em>' reference.
	 * @see #getRedefinedMethodDeclaration()
	 * @generated
	 */
	void setRedefinedMethodDeclaration(MethodDeclaration value);

	/**
	 * Returns the value of the '<em><b>Redefinitions</b></em>' reference list.
	 * The list contents are of type {@link javaMM.MethodDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Redefinitions</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Redefinitions</em>' reference list.
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_Redefinitions()
	 * @see javaMM.MethodDeclaration#getRedefinedMethodDeclaration
	 * @model opposite="redefinedMethodDeclaration" ordered="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!MethodDeclaration!redefinitions'"
	 * @generated
	 */
	EList<MethodDeclaration> getRedefinitions();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n *\n * inv shouldStartWithLowerCase:\n *   let\n *     severity : Integer[1] = \'MethodDeclaration::shouldStartWithLowerCase\'.getSeverity()\n *   in\n *     if severity &lt;= 0\n *     then true\n *     else\n *       let\n *         result : Boolean[1] = self.name.at(1)\n *         .toLowerCase() =\n *         self.name.at(1)\n *       in\n *         \'MethodDeclaration::shouldStartWithLowerCase\'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)\n *     endif\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.IntegerValue%&gt; severity_0 = &lt;%org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation%&gt;.INSTANCE.evaluate(executor, &lt;%javaMM.JavaMMTables%&gt;.STR_MethodDeclaration_c_c_shouldStartWithLowerCase);\nfinal /*@NonInvalid\052/ boolean le = &lt;%org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation%&gt;.INSTANCE.evaluate(executor, severity_0, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n/*@NonInvalid\052/ boolean symbol_0;\nif (le) {\n\tsymbol_0 = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n}\nelse {\n\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_result;\n\ttry {\n\t\tfinal /*@NonInvalid\052/ &lt;%java.lang.String%&gt; name_0 = this.getName();\n\t\tfinal /*@Thrown\052/ &lt;%java.lang.String%&gt; at_0 = &lt;%org.eclipse.ocl.pivot.library.string.StringAtOperation%&gt;.INSTANCE.evaluate(name_0, &lt;%javaMM.JavaMMTables%&gt;.INT_1);\n\t\tfinal /*@Thrown\052/ &lt;%java.lang.String%&gt; toLowerCase = &lt;%org.eclipse.ocl.pivot.library.string.StringToLowerCaseOperation%&gt;.INSTANCE.evaluate(at_0);\n\t\tfinal /*@Thrown\052/ boolean result = toLowerCase.equals(at_0);\n\t\tCAUGHT_result = result;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_result = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\tfinal /*@NonInvalid\052/ boolean logDiagnostic = &lt;%org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation%&gt;.INSTANCE.evaluate(executor, &lt;%org.eclipse.ocl.pivot.ids.TypeId%&gt;.BOOLEAN, &lt;%javaMM.JavaMMTables%&gt;.STR_MethodDeclaration_c_c_shouldStartWithLowerCase, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, &lt;%javaMM.JavaMMTables%&gt;.INT_0).booleanValue();\n\tsymbol_0 = logDiagnostic;\n}\nreturn Boolean.TRUE == symbol_0;'"
	 * @generated
	 */
	boolean shouldStartWithLowerCase(DiagnosticChain diagnostics, Map<Object, Object> context);

} // MethodDeclaration
