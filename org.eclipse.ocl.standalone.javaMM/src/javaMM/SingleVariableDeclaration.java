/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Single Variable Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.SingleVariableDeclaration#getModifier <em>Modifier</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getVarargs <em>Varargs</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getAnnotations <em>Annotations</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getMethodDeclaration <em>Method Declaration</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getCatchClause <em>Catch Clause</em>}</li>
 *   <li>{@link javaMM.SingleVariableDeclaration#getEnhancedForStatement <em>Enhanced For Statement</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration()
 * @model
 * @generated
 */
public interface SingleVariableDeclaration extends VariableDeclaration {
	/**
	 * Returns the value of the '<em><b>Modifier</b></em>' containment reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Modifier#getSingleVariableDeclaration <em>Single Variable Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Modifier</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Modifier</em>' containment reference.
	 * @see #setModifier(Modifier)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_Modifier()
	 * @see javaMM.Modifier#getSingleVariableDeclaration
	 * @model opposite="singleVariableDeclaration" containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!modifier'"
	 * @generated
	 */
	Modifier getModifier();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getModifier <em>Modifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Modifier</em>' containment reference.
	 * @see #getModifier()
	 * @generated
	 */
	void setModifier(Modifier value);

	/**
	 * Returns the value of the '<em><b>Varargs</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Varargs</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Varargs</em>' attribute.
	 * @see #setVarargs(Boolean)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_Varargs()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!varargs'"
	 * @generated
	 */
	Boolean getVarargs();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getVarargs <em>Varargs</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Varargs</em>' attribute.
	 * @see #getVarargs()
	 * @generated
	 */
	void setVarargs(Boolean value);

	/**
	 * Returns the value of the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Type</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' containment reference.
	 * @see #setType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_Type()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!type'"
	 * @generated
	 */
	TypeAccess getType();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getType <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' containment reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(TypeAccess value);

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
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_Annotations()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!annotations'"
	 * @generated
	 */
	EList<Annotation> getAnnotations();

	/**
	 * Returns the value of the '<em><b>Method Declaration</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.AbstractMethodDeclaration#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Method Declaration</em>' container reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Method Declaration</em>' container reference.
	 * @see #setMethodDeclaration(AbstractMethodDeclaration)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_MethodDeclaration()
	 * @see javaMM.AbstractMethodDeclaration#getParameters
	 * @model opposite="parameters" transient="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!methodDeclaration'"
	 * @generated
	 */
	AbstractMethodDeclaration getMethodDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getMethodDeclaration <em>Method Declaration</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Method Declaration</em>' container reference.
	 * @see #getMethodDeclaration()
	 * @generated
	 */
	void setMethodDeclaration(AbstractMethodDeclaration value);

	/**
	 * Returns the value of the '<em><b>Catch Clause</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.CatchClause#getException <em>Exception</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Catch Clause</em>' container reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Catch Clause</em>' container reference.
	 * @see #setCatchClause(CatchClause)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_CatchClause()
	 * @see javaMM.CatchClause#getException
	 * @model opposite="exception" transient="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!catchClause'"
	 * @generated
	 */
	CatchClause getCatchClause();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getCatchClause <em>Catch Clause</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Catch Clause</em>' container reference.
	 * @see #getCatchClause()
	 * @generated
	 */
	void setCatchClause(CatchClause value);

	/**
	 * Returns the value of the '<em><b>Enhanced For Statement</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.EnhancedForStatement#getParameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Enhanced For Statement</em>' container reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enhanced For Statement</em>' container reference.
	 * @see #setEnhancedForStatement(EnhancedForStatement)
	 * @see javaMM.JavaMMPackage#getSingleVariableDeclaration_EnhancedForStatement()
	 * @see javaMM.EnhancedForStatement#getParameter
	 * @model opposite="parameter" transient="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!SingleVariableDeclaration!enhancedForStatement'"
	 * @generated
	 */
	EnhancedForStatement getEnhancedForStatement();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableDeclaration#getEnhancedForStatement <em>Enhanced For Statement</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enhanced For Statement</em>' container reference.
	 * @see #getEnhancedForStatement()
	 * @generated
	 */
	void setEnhancedForStatement(EnhancedForStatement value);

} // SingleVariableDeclaration
