/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Type Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getBodyDeclarations <em>Body Declarations</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getCommentsBeforeBody <em>Comments Before Body</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getCommentsAfterBody <em>Comments After Body</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getPackage <em>Package</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getSuperInterfaces <em>Super Interfaces</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration()
 * @model abstract="true"
 * @generated
 */
public interface AbstractTypeDeclaration extends BodyDeclaration, Type {
	/**
	 * Returns the value of the '<em><b>Body Declarations</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.BodyDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.BodyDeclaration#getAbstractTypeDeclaration <em>Abstract Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Body Declarations</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body Declarations</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_BodyDeclarations()
	 * @see javaMM.BodyDeclaration#getAbstractTypeDeclaration
	 * @model opposite="abstractTypeDeclaration" containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractTypeDeclaration!bodyDeclarations'"
	 * @generated
	 */
	EList<BodyDeclaration> getBodyDeclarations();

	/**
	 * Returns the value of the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Comment}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Comments Before Body</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comments Before Body</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_CommentsBeforeBody()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractTypeDeclaration!commentsBeforeBody'"
	 * @generated
	 */
	EList<Comment> getCommentsBeforeBody();

	/**
	 * Returns the value of the '<em><b>Comments After Body</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Comment}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Comments After Body</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comments After Body</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_CommentsAfterBody()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractTypeDeclaration!commentsAfterBody'"
	 * @generated
	 */
	EList<Comment> getCommentsAfterBody();

	/**
	 * Returns the value of the '<em><b>Package</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Package#getOwnedElements <em>Owned Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Package</em>' container reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Package</em>' container reference.
	 * @see #setPackage(javaMM.Package)
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_Package()
	 * @see javaMM.Package#getOwnedElements
	 * @model opposite="ownedElements" transient="false"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractTypeDeclaration!package'"
	 * @generated
	 */
	javaMM.Package getPackage();

	/**
	 * Sets the value of the '{@link javaMM.AbstractTypeDeclaration#getPackage <em>Package</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Package</em>' container reference.
	 * @see #getPackage()
	 * @generated
	 */
	void setPackage(javaMM.Package value);

	/**
	 * Returns the value of the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeAccess}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Super Interfaces</em>' containment reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Interfaces</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_SuperInterfaces()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!AbstractTypeDeclaration!superInterfaces'"
	 * @generated
	 */
	EList<TypeAccess> getSuperInterfaces();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel body='/**\n * self.superInterfaces-&gt;exists(si | si.type.name = type)\n \052/\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.evaluation.Executor%&gt; executor = &lt;%org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal%&gt;.getExecutor(this);\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.ids.IdResolver%&gt; idResolver = executor.getIdResolver();\nfinal /*@NonInvalid\052/ &lt;%java.util.List%&gt;&lt;&lt;%javaMM.TypeAccess%&gt;&gt; superInterfaces = this.getSuperInterfaces();\nfinal /*@NonInvalid\052/ &lt;%org.eclipse.ocl.pivot.values.OrderedSetValue%&gt; BOXED_superInterfaces = idResolver.createOrderedSetOfAll(&lt;%javaMM.JavaMMTables%&gt;.ORD_CLSSid_TypeAccess, superInterfaces);\n/*@Thrown\052/ &lt;%java.lang.Object%&gt; accumulator = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE;\n/*@NonNull\052/ &lt;%java.util.Iterator%&gt;&lt;&lt;%java.lang.Object%&gt;&gt; ITERATOR_si = BOXED_superInterfaces.iterator();\n/*@Thrown\052/ &lt;%java.lang.Boolean%&gt; exists;\nwhile (true) {\n\tif (!ITERATOR_si.hasNext()) {\n\t\tif (accumulator == &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE) {\n\t\t\texists = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE;\n\t\t}\n\t\telse {\n\t\t\tthrow (&lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;)accumulator;\n\t\t}\n\t\tbreak;\n\t}\n\t/*@NonInvalid\052/ &lt;%javaMM.TypeAccess%&gt; si = (&lt;%javaMM.TypeAccess%&gt;)ITERATOR_si.next();\n\t/**\n\t * si.type.name = type\n\t \052/\n\t/*@Caught\052/ /*@NonNull\052/ &lt;%java.lang.Object%&gt; CAUGHT_eq;\n\ttry {\n\t\tfinal /*@NonInvalid\052/ &lt;%javaMM.Type%&gt; type_0 = si.getType();\n\t\tif (type_0 == null) {\n\t\t\tthrow new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(\"Null source for \\\'\\\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\\\'::NamedElement::name\\\'\");\n\t\t}\n\t\tfinal /*@Thrown\052/ &lt;%java.lang.String%&gt; name = type_0.getName();\n\t\tfinal /*@Thrown\052/ boolean eq = (name != null) ? name.equals(type) : (type == null);\n\t\tCAUGHT_eq = eq;\n\t}\n\tcatch (&lt;%java.lang.Exception%&gt; e) {\n\t\tCAUGHT_eq = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.createInvalidValue(e);\n\t}\n\t//\n\tif (CAUGHT_eq == &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE) {\t\t\t\t\t// Normal successful body evaluation result\n\t\texists = &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.TRUE_VALUE;\n\t\tbreak;\t\t\t\t\t\t\t\t\t\t\t\t\t\t// Stop immediately\n\t}\n\telse if (CAUGHT_eq == &lt;%org.eclipse.ocl.pivot.utilities.ValueUtil%&gt;.FALSE_VALUE) {\t\t\t\t// Normal unsuccessful body evaluation result\n\t\t;\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t// Carry on\n\t}\n\telse if (CAUGHT_eq instanceof &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;) {\t\t// Abnormal exception evaluation result\n\t\taccumulator = CAUGHT_eq;\t\t\t\t\t\t\t\t\t// Cache an exception failure\n\t}\n\telse {\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t// Impossible badly typed result\n\t\taccumulator = new &lt;%org.eclipse.ocl.pivot.values.InvalidValueException%&gt;(&lt;%org.eclipse.ocl.pivot.messages.PivotMessages%&gt;.NonBooleanBody, \"exists\");\n\t}\n}\nreturn exists;'"
	 * @generated
	 */
	Boolean implements_(String type);

} // AbstractTypeDeclaration
