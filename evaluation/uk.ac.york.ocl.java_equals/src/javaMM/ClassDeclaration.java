/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Class Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.ClassDeclaration#getSuperClass <em>Super Class</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getClassDeclaration()
 * @model annotation="http://www.eclipse.org/emf/2002/Ecore constraints='hasEquals hasHashCode'"
 *        annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot hasEquals='hasHashcode() implies\n\t\t\tgetPublicMethods()-&gt;exists(method |\n\t\t\t\tmethod.name = \'equals\' and\n\t\t\t\tmethod.parameters-&gt;size() = 1 and\n\t\t\t\tmethod.parameters-&gt;first().type.type.name = \'Object\' and\n\t\t\t\tmethod.thrownExceptions-&gt;isEmpty() and\n\t\t\t\tmethod.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean)\n\t\t\t)' hasHashCode='hasHashcode()'"
 * @generated
 */
public interface ClassDeclaration extends TypeDeclaration {
	/**
	 * Returns the value of the '<em><b>Super Class</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Class</em>' containment reference.
	 * @see #setSuperClass(TypeAccess)
	 * @see javaMM.JavaMMPackage#getClassDeclaration_SuperClass()
	 * @model containment="true"
	 * @generated
	 */
	TypeAccess getSuperClass();

	/**
	 * Sets the value of the '{@link javaMM.ClassDeclaration#getSuperClass <em>Super Class</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Super Class</em>' containment reference.
	 * @see #getSuperClass()
	 * @generated
	 */
	void setSuperClass(TypeAccess value);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model kind="operation"
	 *        annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='self.bodyDeclarations\n\t\t\t\t-&gt;selectByKind(MethodDeclaration)\n\t\t\t\t-&gt;select(md | md.modifier &lt;&gt; null and md.modifier.visibility = VisibilityKind::public)'"
	 * @generated
	 */
	EList<MethodDeclaration> getPublicMethods();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='getPublicMethods()-&gt;exists(method |\n\t\t\t\tmethod.name = \'hashCode\' and\n\t\t\t\tmethod.parameters-&gt;isEmpty() and\n\t\t\t\tmethod.thrownExceptions-&gt;isEmpty() and\n\t\t\t\tmethod.returnType.type.oclIsTypeOf(PrimitiveTypeInt)\n\t\t\t)'"
	 * @generated
	 */
	Boolean hasHashcode();

} // ClassDeclaration
