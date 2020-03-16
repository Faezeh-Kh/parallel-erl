/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Modifier</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Modifier#getVisibility <em>Visibility</em>}</li>
 *   <li>{@link javaMM.Modifier#getInheritance <em>Inheritance</em>}</li>
 *   <li>{@link javaMM.Modifier#isStatic <em>Static</em>}</li>
 *   <li>{@link javaMM.Modifier#isTransient <em>Transient</em>}</li>
 *   <li>{@link javaMM.Modifier#isVolatile <em>Volatile</em>}</li>
 *   <li>{@link javaMM.Modifier#isNative <em>Native</em>}</li>
 *   <li>{@link javaMM.Modifier#isStrictfp <em>Strictfp</em>}</li>
 *   <li>{@link javaMM.Modifier#isSynchronized <em>Synchronized</em>}</li>
 *   <li>{@link javaMM.Modifier#getBodyDeclaration <em>Body Declaration</em>}</li>
 *   <li>{@link javaMM.Modifier#getSingleVariableDeclaration <em>Single Variable Declaration</em>}</li>
 *   <li>{@link javaMM.Modifier#getVariableDeclarationStatement <em>Variable Declaration Statement</em>}</li>
 *   <li>{@link javaMM.Modifier#getVariableDeclarationExpression <em>Variable Declaration Expression</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getModifier()
 * @model
 * @generated
 */
public interface Modifier extends ASTNode {
	/**
	 * Returns the value of the '<em><b>Visibility</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.VisibilityKind}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Visibility</em>' attribute.
	 * @see javaMM.VisibilityKind
	 * @see #setVisibility(VisibilityKind)
	 * @see javaMM.JavaMMPackage#getModifier_Visibility()
	 * @model required="true"
	 * @generated
	 */
	VisibilityKind getVisibility();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getVisibility <em>Visibility</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Visibility</em>' attribute.
	 * @see javaMM.VisibilityKind
	 * @see #getVisibility()
	 * @generated
	 */
	void setVisibility(VisibilityKind value);

	/**
	 * Returns the value of the '<em><b>Inheritance</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.InheritanceKind}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Inheritance</em>' attribute.
	 * @see javaMM.InheritanceKind
	 * @see #setInheritance(InheritanceKind)
	 * @see javaMM.JavaMMPackage#getModifier_Inheritance()
	 * @model required="true"
	 * @generated
	 */
	InheritanceKind getInheritance();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getInheritance <em>Inheritance</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Inheritance</em>' attribute.
	 * @see javaMM.InheritanceKind
	 * @see #getInheritance()
	 * @generated
	 */
	void setInheritance(InheritanceKind value);

	/**
	 * Returns the value of the '<em><b>Static</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Static</em>' attribute.
	 * @see #setStatic(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Static()
	 * @model required="true"
	 * @generated
	 */
	boolean isStatic();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isStatic <em>Static</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Static</em>' attribute.
	 * @see #isStatic()
	 * @generated
	 */
	void setStatic(boolean value);

	/**
	 * Returns the value of the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Transient</em>' attribute.
	 * @see #setTransient(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Transient()
	 * @model required="true"
	 * @generated
	 */
	boolean isTransient();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isTransient <em>Transient</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Transient</em>' attribute.
	 * @see #isTransient()
	 * @generated
	 */
	void setTransient(boolean value);

	/**
	 * Returns the value of the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Volatile</em>' attribute.
	 * @see #setVolatile(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Volatile()
	 * @model required="true"
	 * @generated
	 */
	boolean isVolatile();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isVolatile <em>Volatile</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Volatile</em>' attribute.
	 * @see #isVolatile()
	 * @generated
	 */
	void setVolatile(boolean value);

	/**
	 * Returns the value of the '<em><b>Native</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Native</em>' attribute.
	 * @see #setNative(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Native()
	 * @model required="true"
	 * @generated
	 */
	boolean isNative();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isNative <em>Native</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Native</em>' attribute.
	 * @see #isNative()
	 * @generated
	 */
	void setNative(boolean value);

	/**
	 * Returns the value of the '<em><b>Strictfp</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Strictfp</em>' attribute.
	 * @see #setStrictfp(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Strictfp()
	 * @model required="true"
	 * @generated
	 */
	boolean isStrictfp();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isStrictfp <em>Strictfp</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Strictfp</em>' attribute.
	 * @see #isStrictfp()
	 * @generated
	 */
	void setStrictfp(boolean value);

	/**
	 * Returns the value of the '<em><b>Synchronized</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Synchronized</em>' attribute.
	 * @see #setSynchronized(boolean)
	 * @see javaMM.JavaMMPackage#getModifier_Synchronized()
	 * @model required="true"
	 * @generated
	 */
	boolean isSynchronized();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#isSynchronized <em>Synchronized</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Synchronized</em>' attribute.
	 * @see #isSynchronized()
	 * @generated
	 */
	void setSynchronized(boolean value);

	/**
	 * Returns the value of the '<em><b>Body Declaration</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.BodyDeclaration#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body Declaration</em>' container reference.
	 * @see #setBodyDeclaration(BodyDeclaration)
	 * @see javaMM.JavaMMPackage#getModifier_BodyDeclaration()
	 * @see javaMM.BodyDeclaration#getModifier
	 * @model opposite="modifier" transient="false"
	 * @generated
	 */
	BodyDeclaration getBodyDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getBodyDeclaration <em>Body Declaration</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Body Declaration</em>' container reference.
	 * @see #getBodyDeclaration()
	 * @generated
	 */
	void setBodyDeclaration(BodyDeclaration value);

	/**
	 * Returns the value of the '<em><b>Single Variable Declaration</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.SingleVariableDeclaration#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Single Variable Declaration</em>' container reference.
	 * @see #setSingleVariableDeclaration(SingleVariableDeclaration)
	 * @see javaMM.JavaMMPackage#getModifier_SingleVariableDeclaration()
	 * @see javaMM.SingleVariableDeclaration#getModifier
	 * @model opposite="modifier" transient="false"
	 * @generated
	 */
	SingleVariableDeclaration getSingleVariableDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getSingleVariableDeclaration <em>Single Variable Declaration</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Single Variable Declaration</em>' container reference.
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	void setSingleVariableDeclaration(SingleVariableDeclaration value);

	/**
	 * Returns the value of the '<em><b>Variable Declaration Statement</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.VariableDeclarationStatement#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable Declaration Statement</em>' container reference.
	 * @see #setVariableDeclarationStatement(VariableDeclarationStatement)
	 * @see javaMM.JavaMMPackage#getModifier_VariableDeclarationStatement()
	 * @see javaMM.VariableDeclarationStatement#getModifier
	 * @model opposite="modifier" transient="false"
	 * @generated
	 */
	VariableDeclarationStatement getVariableDeclarationStatement();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getVariableDeclarationStatement <em>Variable Declaration Statement</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable Declaration Statement</em>' container reference.
	 * @see #getVariableDeclarationStatement()
	 * @generated
	 */
	void setVariableDeclarationStatement(VariableDeclarationStatement value);

	/**
	 * Returns the value of the '<em><b>Variable Declaration Expression</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.VariableDeclarationExpression#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable Declaration Expression</em>' container reference.
	 * @see #setVariableDeclarationExpression(VariableDeclarationExpression)
	 * @see javaMM.JavaMMPackage#getModifier_VariableDeclarationExpression()
	 * @see javaMM.VariableDeclarationExpression#getModifier
	 * @model opposite="modifier" transient="false"
	 * @generated
	 */
	VariableDeclarationExpression getVariableDeclarationExpression();

	/**
	 * Sets the value of the '{@link javaMM.Modifier#getVariableDeclarationExpression <em>Variable Declaration Expression</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable Declaration Expression</em>' container reference.
	 * @see #getVariableDeclarationExpression()
	 * @generated
	 */
	void setVariableDeclarationExpression(VariableDeclarationExpression value);

} // Modifier
