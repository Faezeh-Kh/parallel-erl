/**
 */
package javaMM;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see javaMM.JavaMMFactory
 * @model kind="package"
 * @generated
 */
public interface JavaMMPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "javaMM";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/MoDisco/Java/0.2.incubation/java";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "javaMM";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	JavaMMPackage eINSTANCE = javaMM.impl.JavaMMPackageImpl.init();

	/**
	 * The meta object id for the '{@link javaMM.impl.ASTNodeImpl <em>AST Node</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ASTNodeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getASTNode()
	 * @generated
	 */
	int AST_NODE = 8;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AST_NODE__COMMENTS = 0;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AST_NODE__ORIGINAL_COMPILATION_UNIT = 1;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AST_NODE__ORIGINAL_CLASS_FILE = 2;

	/**
	 * The number of structural features of the '<em>AST Node</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AST_NODE_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>AST Node</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AST_NODE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.NamedElementImpl <em>Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.NamedElementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getNamedElement()
	 * @generated
	 */
	int NAMED_ELEMENT = 65;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__NAME = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__PROXY = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__USAGES_IN_IMPORTS = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.BodyDeclarationImpl <em>Body Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.BodyDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getBodyDeclaration()
	 * @generated
	 */
	int BODY_DECLARATION = 19;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__ANNOTATIONS = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION__MODIFIER = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Body Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Body Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BODY_DECLARATION_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AbstractMethodDeclarationImpl <em>Abstract Method Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AbstractMethodDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAbstractMethodDeclaration()
	 * @generated
	 */
	int ABSTRACT_METHOD_DECLARATION = 0;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__BODY = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__PARAMETERS = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS = BODY_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS = BODY_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS = BODY_DECLARATION_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION__USAGES = BODY_DECLARATION_FEATURE_COUNT + 5;

	/**
	 * The number of structural features of the '<em>Abstract Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 6;

	/**
	 * The operation id for the '<em>Parameters Effectively Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>Local Method Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP = BODY_DECLARATION_OPERATION_COUNT + 1;

	/**
	 * The number of operations of the '<em>Abstract Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_DECLARATION_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 2;

	/**
	 * The meta object id for the '{@link javaMM.impl.AbstractMethodInvocationImpl <em>Abstract Method Invocation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AbstractMethodInvocationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAbstractMethodInvocation()
	 * @generated
	 */
	int ABSTRACT_METHOD_INVOCATION = 1;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__METHOD = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__ARGUMENTS = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION__TYPE_ARGUMENTS = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Abstract Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Abstract Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_METHOD_INVOCATION_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AbstractTypeDeclarationImpl <em>Abstract Type Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AbstractTypeDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAbstractTypeDeclaration()
	 * @generated
	 */
	int ABSTRACT_TYPE_DECLARATION = 2;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY = BODY_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY = BODY_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__PACKAGE = BODY_DECLARATION_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES = BODY_DECLARATION_FEATURE_COUNT + 5;

	/**
	 * The number of structural features of the '<em>Abstract Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 6;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Abstract Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_DECLARATION_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.ExpressionImpl <em>Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getExpression()
	 * @generated
	 */
	int EXPRESSION = 41;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The number of structural features of the '<em>Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AbstractTypeQualifiedExpressionImpl <em>Abstract Type Qualified Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AbstractTypeQualifiedExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAbstractTypeQualifiedExpression()
	 * @generated
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION = 3;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION__QUALIFIER = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Abstract Type Qualified Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Abstract Type Qualified Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_TYPE_QUALIFIED_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AbstractVariablesContainerImpl <em>Abstract Variables Container</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AbstractVariablesContainerImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAbstractVariablesContainer()
	 * @generated
	 */
	int ABSTRACT_VARIABLES_CONTAINER = 4;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER__TYPE = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Fragments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER__FRAGMENTS = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Abstract Variables Container</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Abstract Variables Container</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ABSTRACT_VARIABLES_CONTAINER_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AnnotationImpl <em>Annotation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AnnotationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAnnotation()
	 * @generated
	 */
	int ANNOTATION = 5;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION__TYPE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Values</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION__VALUES = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Annotation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArchiveImpl <em>Archive</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArchiveImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArchive()
	 * @generated
	 */
	int ARCHIVE = 6;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Original File Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__ORIGINAL_FILE_PATH = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Class Files</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__CLASS_FILES = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Manifest</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE__MANIFEST = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Archive</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Archive</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARCHIVE_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.StatementImpl <em>Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.StatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getStatement()
	 * @generated
	 */
	int STATEMENT = 88;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATEMENT__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATEMENT__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATEMENT__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The number of structural features of the '<em>Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATEMENT_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATEMENT_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AssertStatementImpl <em>Assert Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AssertStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAssertStatement()
	 * @generated
	 */
	int ASSERT_STATEMENT = 7;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Message</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT__MESSAGE = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Assert Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Assert Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSERT_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AnnotationMemberValuePairImpl <em>Annotation Member Value Pair</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AnnotationMemberValuePairImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationMemberValuePair()
	 * @generated
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR = 9;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__MEMBER = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR__VALUE = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Annotation Member Value Pair</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Annotation Member Value Pair</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_MEMBER_VALUE_PAIR_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AnnotationTypeDeclarationImpl <em>Annotation Type Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AnnotationTypeDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationTypeDeclaration()
	 * @generated
	 */
	int ANNOTATION_TYPE_DECLARATION = 10;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__COMMENTS = ABSTRACT_TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__NAME = ABSTRACT_TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__PROXY = ABSTRACT_TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__ANNOTATIONS = ABSTRACT_TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__MODIFIER = ABSTRACT_TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__BODY_DECLARATIONS = ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__COMMENTS_BEFORE_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__COMMENTS_AFTER_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__PACKAGE = ABSTRACT_TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION__SUPER_INTERFACES = ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The number of structural features of the '<em>Annotation Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION_FEATURE_COUNT = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION___IMPLEMENTS____STRING = ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Annotation Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_DECLARATION_OPERATION_COUNT = ABSTRACT_TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AnnotationTypeMemberDeclarationImpl <em>Annotation Type Member Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AnnotationTypeMemberDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationTypeMemberDeclaration()
	 * @generated
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION = 11;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Default</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__DEFAULT = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__TYPE = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES = BODY_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Annotation Type Member Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Annotation Type Member Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANNOTATION_TYPE_MEMBER_DECLARATION_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AnonymousClassDeclarationImpl <em>Anonymous Class Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AnonymousClassDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAnonymousClassDeclaration()
	 * @generated
	 */
	int ANONYMOUS_CLASS_DECLARATION = 12;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Class Instance Creation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Anonymous Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Anonymous Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ANONYMOUS_CLASS_DECLARATION_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArrayAccessImpl <em>Array Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArrayAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArrayAccess()
	 * @generated
	 */
	int ARRAY_ACCESS = 13;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Array</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS__ARRAY = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Index</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS__INDEX = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Array Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Array Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArrayCreationImpl <em>Array Creation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArrayCreationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArrayCreation()
	 * @generated
	 */
	int ARRAY_CREATION = 14;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Dimensions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__DIMENSIONS = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__INITIALIZER = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION__TYPE = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Array Creation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Array Creation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_CREATION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArrayInitializerImpl <em>Array Initializer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArrayInitializerImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArrayInitializer()
	 * @generated
	 */
	int ARRAY_INITIALIZER = 15;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expressions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER__EXPRESSIONS = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Array Initializer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Array Initializer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_INITIALIZER_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArrayLengthAccessImpl <em>Array Length Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArrayLengthAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArrayLengthAccess()
	 * @generated
	 */
	int ARRAY_LENGTH_ACCESS = 16;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Array</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS__ARRAY = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Array Length Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Array Length Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_LENGTH_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeImpl <em>Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getType()
	 * @generated
	 */
	int TYPE = 101;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE__USAGES_IN_TYPE_ACCESS = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ArrayTypeImpl <em>Array Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ArrayTypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getArrayType()
	 * @generated
	 */
	int ARRAY_TYPE = 17;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__DIMENSIONS = TYPE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Element Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE__ELEMENT_TYPE = TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Array Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE_FEATURE_COUNT = TYPE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Array Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPE_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.AssignmentImpl <em>Assignment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.AssignmentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getAssignment()
	 * @generated
	 */
	int ASSIGNMENT = 18;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Left Hand Side</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__LEFT_HAND_SIDE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__OPERATOR = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Right Hand Side</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT__RIGHT_HAND_SIDE = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Assignment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The operation id for the '<em>No Redundant Assignment</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT___NO_REDUNDANT_ASSIGNMENT__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Assignment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ASSIGNMENT_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.BooleanLiteralImpl <em>Boolean Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.BooleanLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getBooleanLiteral()
	 * @generated
	 */
	int BOOLEAN_LITERAL = 20;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL__VALUE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Boolean Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Boolean Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.CommentImpl <em>Comment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.CommentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getComment()
	 * @generated
	 */
	int COMMENT = 33;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Content</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__CONTENT = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Enclosed By Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__ENCLOSED_BY_PARENT = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT__PREFIX_OF_PARENT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMMENT_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.BlockCommentImpl <em>Block Comment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.BlockCommentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getBlockComment()
	 * @generated
	 */
	int BLOCK_COMMENT = 21;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__COMMENTS = COMMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__ORIGINAL_COMPILATION_UNIT = COMMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__ORIGINAL_CLASS_FILE = COMMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Content</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__CONTENT = COMMENT__CONTENT;

	/**
	 * The feature id for the '<em><b>Enclosed By Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__ENCLOSED_BY_PARENT = COMMENT__ENCLOSED_BY_PARENT;

	/**
	 * The feature id for the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT__PREFIX_OF_PARENT = COMMENT__PREFIX_OF_PARENT;

	/**
	 * The number of structural features of the '<em>Block Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT_FEATURE_COUNT = COMMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Block Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_COMMENT_OPERATION_COUNT = COMMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.BlockImpl <em>Block</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.BlockImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getBlock()
	 * @generated
	 */
	int BLOCK = 22;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Statements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK__STATEMENTS = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Block</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Empty Block Is Documented</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK___EMPTY_BLOCK_IS_DOCUMENTED__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Block</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BLOCK_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.BreakStatementImpl <em>Break Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.BreakStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getBreakStatement()
	 * @generated
	 */
	int BREAK_STATEMENT = 23;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Label</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT__LABEL = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Break Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Break Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BREAK_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.CastExpressionImpl <em>Cast Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.CastExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getCastExpression()
	 * @generated
	 */
	int CAST_EXPRESSION = 24;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION__EXPRESSION = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION__TYPE = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Cast Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Cast Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CAST_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.CatchClauseImpl <em>Catch Clause</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.CatchClauseImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getCatchClause()
	 * @generated
	 */
	int CATCH_CLAUSE = 25;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Exception</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE__EXCEPTION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE__BODY = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Catch Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Does Not Catch Dubious Exceptions</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE___DOES_NOT_CATCH_DUBIOUS_EXCEPTIONS__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>Exception Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE___EXCEPTION_IS_USED__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The number of operations of the '<em>Catch Clause</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CATCH_CLAUSE_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 2;

	/**
	 * The meta object id for the '{@link javaMM.impl.CharacterLiteralImpl <em>Character Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.CharacterLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getCharacterLiteral()
	 * @generated
	 */
	int CHARACTER_LITERAL = 26;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Escaped Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL__ESCAPED_VALUE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Character Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Character Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CHARACTER_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ClassFileImpl <em>Class File</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ClassFileImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getClassFile()
	 * @generated
	 */
	int CLASS_FILE = 27;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Original File Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__ORIGINAL_FILE_PATH = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__TYPE = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Attached Source</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__ATTACHED_SOURCE = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Package</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE__PACKAGE = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Class File</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Class File</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_FILE_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ClassInstanceCreationImpl <em>Class Instance Creation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ClassInstanceCreationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getClassInstanceCreation()
	 * @generated
	 */
	int CLASS_INSTANCE_CREATION = 28;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__METHOD = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__ARGUMENTS = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__TYPE_ARGUMENTS = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__ANONYMOUS_CLASS_DECLARATION = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__EXPRESSION = EXPRESSION_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION__TYPE = EXPRESSION_FEATURE_COUNT + 5;

	/**
	 * The number of structural features of the '<em>Class Instance Creation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 6;

	/**
	 * The number of operations of the '<em>Class Instance Creation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_INSTANCE_CREATION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ConstructorDeclarationImpl <em>Constructor Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ConstructorDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getConstructorDeclaration()
	 * @generated
	 */
	int CONSTRUCTOR_DECLARATION = 29;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__COMMENTS = ABSTRACT_METHOD_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_METHOD_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__NAME = ABSTRACT_METHOD_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__PROXY = ABSTRACT_METHOD_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_METHOD_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__ANNOTATIONS = ABSTRACT_METHOD_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__MODIFIER = ABSTRACT_METHOD_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__BODY = ABSTRACT_METHOD_DECLARATION__BODY;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__PARAMETERS = ABSTRACT_METHOD_DECLARATION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__THROWN_EXCEPTIONS = ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__TYPE_PARAMETERS = ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__USAGES_IN_DOC_COMMENTS = ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION__USAGES = ABSTRACT_METHOD_DECLARATION__USAGES;

	/**
	 * The number of structural features of the '<em>Constructor Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION_FEATURE_COUNT = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Parameters Effectively Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP = ABSTRACT_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Local Method Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP = ABSTRACT_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Constructor Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_DECLARATION_OPERATION_COUNT = ABSTRACT_METHOD_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ConditionalExpressionImpl <em>Conditional Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ConditionalExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getConditionalExpression()
	 * @generated
	 */
	int CONDITIONAL_EXPRESSION = 30;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Else Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__ELSE_EXPRESSION = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__EXPRESSION = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Then Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION__THEN_EXPRESSION = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Conditional Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Conditional Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONDITIONAL_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ConstructorInvocationImpl <em>Constructor Invocation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ConstructorInvocationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getConstructorInvocation()
	 * @generated
	 */
	int CONSTRUCTOR_INVOCATION = 31;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__METHOD = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__ARGUMENTS = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION__TYPE_ARGUMENTS = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Constructor Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Constructor Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONSTRUCTOR_INVOCATION_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeDeclarationImpl <em>Type Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTypeDeclaration()
	 * @generated
	 */
	int TYPE_DECLARATION = 103;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__COMMENTS = ABSTRACT_TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__NAME = ABSTRACT_TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__PROXY = ABSTRACT_TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__ANNOTATIONS = ABSTRACT_TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__MODIFIER = ABSTRACT_TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__BODY_DECLARATIONS = ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__COMMENTS_BEFORE_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__COMMENTS_AFTER_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__PACKAGE = ABSTRACT_TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__SUPER_INTERFACES = ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION__TYPE_PARAMETERS = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_FEATURE_COUNT = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION___IMPLEMENTS____STRING = ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_OPERATION_COUNT = ABSTRACT_TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ClassDeclarationImpl <em>Class Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ClassDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getClassDeclaration()
	 * @generated
	 */
	int CLASS_DECLARATION = 32;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__COMMENTS = TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__ORIGINAL_COMPILATION_UNIT = TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__ORIGINAL_CLASS_FILE = TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__NAME = TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__PROXY = TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__USAGES_IN_IMPORTS = TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__ABSTRACT_TYPE_DECLARATION = TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__ANNOTATIONS = TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__MODIFIER = TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__USAGES_IN_TYPE_ACCESS = TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__BODY_DECLARATIONS = TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__COMMENTS_BEFORE_BODY = TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__COMMENTS_AFTER_BODY = TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__PACKAGE = TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__SUPER_INTERFACES = TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__TYPE_PARAMETERS = TYPE_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Super Class</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION__SUPER_CLASS = TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION_FEATURE_COUNT = TYPE_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___IMPLEMENTS____STRING = TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The operation id for the '<em>Get Methods</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___GET_METHODS = TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>Has Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___HAS_EQUALS = TYPE_DECLARATION_OPERATION_COUNT + 1;

	/**
	 * The operation id for the '<em>Has Hashcode</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___HAS_HASHCODE = TYPE_DECLARATION_OPERATION_COUNT + 2;

	/**
	 * The operation id for the '<em>Has Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___HAS_COMPARE_TO = TYPE_DECLARATION_OPERATION_COUNT + 3;

	/**
	 * The operation id for the '<em>No Covariant Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___NO_COVARIANT_COMPARE_TO__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 4;

	/**
	 * The operation id for the '<em>Hash Code And Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___HASH_CODE_AND_EQUALS__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 5;

	/**
	 * The operation id for the '<em>Serial UID In Serializable Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___SERIAL_UID_IN_SERIALIZABLE_CLASS__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 6;

	/**
	 * The operation id for the '<em>Equals And Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___EQUALS_AND_COMPARE_TO__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 7;

	/**
	 * The operation id for the '<em>No Redundant Interface Impl</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___NO_REDUNDANT_INTERFACE_IMPL__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 8;

	/**
	 * The operation id for the '<em>Clone In Cloneable</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___CLONE_IN_CLONEABLE__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 9;

	/**
	 * The operation id for the '<em>No Covariant Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___NO_COVARIANT_EQUALS__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 10;

	/**
	 * The operation id for the '<em>No Obscured Variables</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___NO_OBSCURED_VARIABLES__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 11;

	/**
	 * The operation id for the '<em>Comparator Implements Serializable</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION___COMPARATOR_IMPLEMENTS_SERIALIZABLE__DIAGNOSTICCHAIN_MAP = TYPE_DECLARATION_OPERATION_COUNT + 12;

	/**
	 * The number of operations of the '<em>Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASS_DECLARATION_OPERATION_COUNT = TYPE_DECLARATION_OPERATION_COUNT + 13;

	/**
	 * The meta object id for the '{@link javaMM.impl.CompilationUnitImpl <em>Compilation Unit</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.CompilationUnitImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getCompilationUnit()
	 * @generated
	 */
	int COMPILATION_UNIT = 34;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Original File Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__ORIGINAL_FILE_PATH = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Comment List</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__COMMENT_LIST = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Imports</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__IMPORTS = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Package</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__PACKAGE = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT__TYPES = NAMED_ELEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Compilation Unit</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 5;

	/**
	 * The number of operations of the '<em>Compilation Unit</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPILATION_UNIT_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ContinueStatementImpl <em>Continue Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ContinueStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getContinueStatement()
	 * @generated
	 */
	int CONTINUE_STATEMENT = 35;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Label</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT__LABEL = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Continue Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Continue Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONTINUE_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.DoStatementImpl <em>Do Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.DoStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getDoStatement()
	 * @generated
	 */
	int DO_STATEMENT = 36;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Do Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Do Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DO_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.EmptyStatementImpl <em>Empty Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.EmptyStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getEmptyStatement()
	 * @generated
	 */
	int EMPTY_STATEMENT = 37;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EMPTY_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EMPTY_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EMPTY_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The number of structural features of the '<em>Empty Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EMPTY_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Empty Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EMPTY_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.EnhancedForStatementImpl <em>Enhanced For Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.EnhancedForStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getEnhancedForStatement()
	 * @generated
	 */
	int ENHANCED_FOR_STATEMENT = 38;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Parameter</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT__PARAMETER = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Enhanced For Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Enhanced For Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENHANCED_FOR_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.EnumConstantDeclarationImpl <em>Enum Constant Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.EnumConstantDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getEnumConstantDeclaration()
	 * @generated
	 */
	int ENUM_CONSTANT_DECLARATION = 39;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__EXTRA_ARRAY_DIMENSIONS = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__INITIALIZER = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__USAGE_IN_VARIABLE_ACCESS = BODY_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ANONYMOUS_CLASS_DECLARATION = BODY_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION__ARGUMENTS = BODY_DECLARATION_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Enum Constant Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 5;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Enum Constant Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_CONSTANT_DECLARATION_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.EnumDeclarationImpl <em>Enum Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.EnumDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getEnumDeclaration()
	 * @generated
	 */
	int ENUM_DECLARATION = 40;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__COMMENTS = ABSTRACT_TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__NAME = ABSTRACT_TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__PROXY = ABSTRACT_TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ANNOTATIONS = ABSTRACT_TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__MODIFIER = ABSTRACT_TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__USAGES_IN_TYPE_ACCESS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__BODY_DECLARATIONS = ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__COMMENTS_BEFORE_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__COMMENTS_AFTER_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__PACKAGE = ABSTRACT_TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__SUPER_INTERFACES = ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Enum Constants</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION__ENUM_CONSTANTS = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Enum Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION_FEATURE_COUNT = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION___IMPLEMENTS____STRING = ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Enum Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_DECLARATION_OPERATION_COUNT = ABSTRACT_TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ExpressionStatementImpl <em>Expression Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ExpressionStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getExpressionStatement()
	 * @generated
	 */
	int EXPRESSION_STATEMENT = 42;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Expression Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Expression Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPRESSION_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.FieldAccessImpl <em>Field Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.FieldAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getFieldAccess()
	 * @generated
	 */
	int FIELD_ACCESS = 43;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Field</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS__FIELD = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS__EXPRESSION = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Field Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Field Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.FieldDeclarationImpl <em>Field Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.FieldDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getFieldDeclaration()
	 * @generated
	 */
	int FIELD_DECLARATION = 44;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__TYPE = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Fragments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION__FRAGMENTS = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Field Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Field Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FIELD_DECLARATION_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ForStatementImpl <em>For Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ForStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getForStatement()
	 * @generated
	 */
	int FOR_STATEMENT = 45;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Updaters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__UPDATERS = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Initializers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__INITIALIZERS = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>For Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>For Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOR_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.IfStatementImpl <em>If Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.IfStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getIfStatement()
	 * @generated
	 */
	int IF_STATEMENT = 46;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Then Statement</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__THEN_STATEMENT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Else Statement</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT__ELSE_STATEMENT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>If Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The operation id for the '<em>No Dead Code</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT___NO_DEAD_CODE__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>No Useless Control Flow</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT___NO_USELESS_CONTROL_FLOW__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The number of operations of the '<em>If Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IF_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 2;

	/**
	 * The meta object id for the '{@link javaMM.impl.ImportDeclarationImpl <em>Import Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ImportDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getImportDeclaration()
	 * @generated
	 */
	int IMPORT_DECLARATION = 47;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Static</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION__STATIC = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Imported Element</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION__IMPORTED_ELEMENT = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Import Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Import Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_DECLARATION_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.InfixExpressionImpl <em>Infix Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.InfixExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getInfixExpression()
	 * @generated
	 */
	int INFIX_EXPRESSION = 48;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__OPERATOR = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Right Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__RIGHT_OPERAND = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Left Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__LEFT_OPERAND = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Extended Operands</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION__EXTENDED_OPERANDS = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Infix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 4;

	/**
	 * The operation id for the '<em>Operator Is Equality</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION___OPERATOR_IS_EQUALITY = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>Equals Not On Literals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION___EQUALS_NOT_ON_LITERALS__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 1;

	/**
	 * The operation id for the '<em>No Redundant Comparison</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION___NO_REDUNDANT_COMPARISON__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 2;

	/**
	 * The operation id for the '<em>Equals Not On Strings</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION___EQUALS_NOT_ON_STRINGS__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 3;

	/**
	 * The number of operations of the '<em>Infix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INFIX_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 4;

	/**
	 * The meta object id for the '{@link javaMM.impl.InitializerImpl <em>Initializer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.InitializerImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getInitializer()
	 * @generated
	 */
	int INITIALIZER = 49;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__COMMENTS = BODY_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__ORIGINAL_COMPILATION_UNIT = BODY_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__ORIGINAL_CLASS_FILE = BODY_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__NAME = BODY_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__PROXY = BODY_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__USAGES_IN_IMPORTS = BODY_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__ABSTRACT_TYPE_DECLARATION = BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__ANNOTATIONS = BODY_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__ANONYMOUS_CLASS_DECLARATION_OWNER = BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__MODIFIER = BODY_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER__BODY = BODY_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Initializer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER_FEATURE_COUNT = BODY_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Initializer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INITIALIZER_OPERATION_COUNT = BODY_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.InstanceofExpressionImpl <em>Instanceof Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.InstanceofExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getInstanceofExpression()
	 * @generated
	 */
	int INSTANCEOF_EXPRESSION = 50;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Right Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION__RIGHT_OPERAND = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Left Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION__LEFT_OPERAND = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Instanceof Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Instanceof Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INSTANCEOF_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.InterfaceDeclarationImpl <em>Interface Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.InterfaceDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getInterfaceDeclaration()
	 * @generated
	 */
	int INTERFACE_DECLARATION = 51;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__COMMENTS = TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__ORIGINAL_COMPILATION_UNIT = TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__ORIGINAL_CLASS_FILE = TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__NAME = TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__PROXY = TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__USAGES_IN_IMPORTS = TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__ABSTRACT_TYPE_DECLARATION = TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__ANNOTATIONS = TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__MODIFIER = TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__USAGES_IN_TYPE_ACCESS = TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__BODY_DECLARATIONS = TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__COMMENTS_BEFORE_BODY = TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__COMMENTS_AFTER_BODY = TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__PACKAGE = TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__SUPER_INTERFACES = TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION__TYPE_PARAMETERS = TYPE_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The number of structural features of the '<em>Interface Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION_FEATURE_COUNT = TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION___IMPLEMENTS____STRING = TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Interface Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERFACE_DECLARATION_OPERATION_COUNT = TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.JavadocImpl <em>Javadoc</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.JavadocImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getJavadoc()
	 * @generated
	 */
	int JAVADOC = 52;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__COMMENTS = COMMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__ORIGINAL_COMPILATION_UNIT = COMMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__ORIGINAL_CLASS_FILE = COMMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Content</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__CONTENT = COMMENT__CONTENT;

	/**
	 * The feature id for the '<em><b>Enclosed By Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__ENCLOSED_BY_PARENT = COMMENT__ENCLOSED_BY_PARENT;

	/**
	 * The feature id for the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__PREFIX_OF_PARENT = COMMENT__PREFIX_OF_PARENT;

	/**
	 * The feature id for the '<em><b>Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC__TAGS = COMMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Javadoc</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC_FEATURE_COUNT = COMMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Javadoc</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int JAVADOC_OPERATION_COUNT = COMMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.LabeledStatementImpl <em>Labeled Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.LabeledStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getLabeledStatement()
	 * @generated
	 */
	int LABELED_STATEMENT = 53;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__BODY = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Usages In Break Statements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__USAGES_IN_BREAK_STATEMENTS = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Usages In Continue Statements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT__USAGES_IN_CONTINUE_STATEMENTS = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Labeled Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Labeled Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LABELED_STATEMENT_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.LineCommentImpl <em>Line Comment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.LineCommentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getLineComment()
	 * @generated
	 */
	int LINE_COMMENT = 54;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__COMMENTS = COMMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__ORIGINAL_COMPILATION_UNIT = COMMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__ORIGINAL_CLASS_FILE = COMMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Content</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__CONTENT = COMMENT__CONTENT;

	/**
	 * The feature id for the '<em><b>Enclosed By Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__ENCLOSED_BY_PARENT = COMMENT__ENCLOSED_BY_PARENT;

	/**
	 * The feature id for the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT__PREFIX_OF_PARENT = COMMENT__PREFIX_OF_PARENT;

	/**
	 * The number of structural features of the '<em>Line Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT_FEATURE_COUNT = COMMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Line Comment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LINE_COMMENT_OPERATION_COUNT = COMMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ManifestImpl <em>Manifest</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ManifestImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getManifest()
	 * @generated
	 */
	int MANIFEST = 55;

	/**
	 * The feature id for the '<em><b>Main Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST__MAIN_ATTRIBUTES = 0;

	/**
	 * The feature id for the '<em><b>Entry Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST__ENTRY_ATTRIBUTES = 1;

	/**
	 * The number of structural features of the '<em>Manifest</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Manifest</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ManifestAttributeImpl <em>Manifest Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ManifestAttributeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getManifestAttribute()
	 * @generated
	 */
	int MANIFEST_ATTRIBUTE = 56;

	/**
	 * The feature id for the '<em><b>Key</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ATTRIBUTE__KEY = 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ATTRIBUTE__VALUE = 1;

	/**
	 * The number of structural features of the '<em>Manifest Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ATTRIBUTE_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Manifest Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ATTRIBUTE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ManifestEntryImpl <em>Manifest Entry</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ManifestEntryImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getManifestEntry()
	 * @generated
	 */
	int MANIFEST_ENTRY = 57;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ENTRY__NAME = 0;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ENTRY__ATTRIBUTES = 1;

	/**
	 * The number of structural features of the '<em>Manifest Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ENTRY_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Manifest Entry</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANIFEST_ENTRY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.MemberRefImpl <em>Member Ref</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.MemberRefImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getMemberRef()
	 * @generated
	 */
	int MEMBER_REF = 58;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Member</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF__MEMBER = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF__QUALIFIER = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Member Ref</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Member Ref</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_REF_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.MethodDeclarationImpl <em>Method Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.MethodDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getMethodDeclaration()
	 * @generated
	 */
	int METHOD_DECLARATION = 59;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__COMMENTS = ABSTRACT_METHOD_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_METHOD_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__NAME = ABSTRACT_METHOD_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__PROXY = ABSTRACT_METHOD_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_METHOD_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__ANNOTATIONS = ABSTRACT_METHOD_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__MODIFIER = ABSTRACT_METHOD_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__BODY = ABSTRACT_METHOD_DECLARATION__BODY;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__PARAMETERS = ABSTRACT_METHOD_DECLARATION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__THROWN_EXCEPTIONS = ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__TYPE_PARAMETERS = ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS = ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__USAGES = ABSTRACT_METHOD_DECLARATION__USAGES;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Return Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__RETURN_TYPE = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Redefined Method Declaration</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Redefinitions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION__REDEFINITIONS = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION_FEATURE_COUNT = ABSTRACT_METHOD_DECLARATION_FEATURE_COUNT + 4;

	/**
	 * The operation id for the '<em>Parameters Effectively Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP = ABSTRACT_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Local Method Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP = ABSTRACT_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Should Start With Lower Case</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION___SHOULD_START_WITH_LOWER_CASE__DIAGNOSTICCHAIN_MAP = ABSTRACT_METHOD_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_DECLARATION_OPERATION_COUNT = ABSTRACT_METHOD_DECLARATION_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.MethodInvocationImpl <em>Method Invocation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.MethodInvocationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getMethodInvocation()
	 * @generated
	 */
	int METHOD_INVOCATION = 60;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__METHOD = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__ARGUMENTS = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__TYPE_ARGUMENTS = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION__EXPRESSION = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 4;

	/**
	 * The operation id for the '<em>Does Not Call Run Finalizers</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION___DOES_NOT_CALL_RUN_FINALIZERS__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The operation id for the '<em>Does Not Call Finalize</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION___DOES_NOT_CALL_FINALIZE__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 1;

	/**
	 * The operation id for the '<em>Does Not Call Exit</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION___DOES_NOT_CALL_EXIT__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 2;

	/**
	 * The number of operations of the '<em>Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_INVOCATION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 3;

	/**
	 * The meta object id for the '{@link javaMM.impl.MethodRefImpl <em>Method Ref</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.MethodRefImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getMethodRef()
	 * @generated
	 */
	int METHOD_REF = 61;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__METHOD = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__QUALIFIER = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF__PARAMETERS = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Method Ref</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Method Ref</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.MethodRefParameterImpl <em>Method Ref Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.MethodRefParameterImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getMethodRefParameter()
	 * @generated
	 */
	int METHOD_REF_PARAMETER = 62;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__NAME = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Varargs</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__VARARGS = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER__TYPE = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Method Ref Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Method Ref Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int METHOD_REF_PARAMETER_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ModelImpl <em>Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ModelImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getModel()
	 * @generated
	 */
	int MODEL = 63;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__NAME = 0;

	/**
	 * The feature id for the '<em><b>Owned Elements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__OWNED_ELEMENTS = 1;

	/**
	 * The feature id for the '<em><b>Orphan Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__ORPHAN_TYPES = 2;

	/**
	 * The feature id for the '<em><b>Unresolved Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__UNRESOLVED_ITEMS = 3;

	/**
	 * The feature id for the '<em><b>Compilation Units</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__COMPILATION_UNITS = 4;

	/**
	 * The feature id for the '<em><b>Class Files</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__CLASS_FILES = 5;

	/**
	 * The feature id for the '<em><b>Archives</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL__ARCHIVES = 6;

	/**
	 * The number of structural features of the '<em>Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ModifierImpl <em>Modifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ModifierImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getModifier()
	 * @generated
	 */
	int MODIFIER = 64;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Visibility</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__VISIBILITY = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Inheritance</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__INHERITANCE = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Static</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__STATIC = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__TRANSIENT = AST_NODE_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__VOLATILE = AST_NODE_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Native</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__NATIVE = AST_NODE_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Strictfp</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__STRICTFP = AST_NODE_FEATURE_COUNT + 6;

	/**
	 * The feature id for the '<em><b>Synchronized</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__SYNCHRONIZED = AST_NODE_FEATURE_COUNT + 7;

	/**
	 * The feature id for the '<em><b>Body Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__BODY_DECLARATION = AST_NODE_FEATURE_COUNT + 8;

	/**
	 * The feature id for the '<em><b>Single Variable Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__SINGLE_VARIABLE_DECLARATION = AST_NODE_FEATURE_COUNT + 9;

	/**
	 * The feature id for the '<em><b>Variable Declaration Statement</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__VARIABLE_DECLARATION_STATEMENT = AST_NODE_FEATURE_COUNT + 10;

	/**
	 * The feature id for the '<em><b>Variable Declaration Expression</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER__VARIABLE_DECLARATION_EXPRESSION = AST_NODE_FEATURE_COUNT + 11;

	/**
	 * The number of structural features of the '<em>Modifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 12;

	/**
	 * The operation id for the '<em>Is Local</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER___IS_LOCAL = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Modifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODIFIER_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.NamespaceAccessImpl <em>Namespace Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.NamespaceAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getNamespaceAccess()
	 * @generated
	 */
	int NAMESPACE_ACCESS = 66;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMESPACE_ACCESS__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMESPACE_ACCESS__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMESPACE_ACCESS__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The number of structural features of the '<em>Namespace Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMESPACE_ACCESS_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Namespace Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMESPACE_ACCESS_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.NumberLiteralImpl <em>Number Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.NumberLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getNumberLiteral()
	 * @generated
	 */
	int NUMBER_LITERAL = 67;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Token Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL__TOKEN_VALUE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Number Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Number Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NUMBER_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.NullLiteralImpl <em>Null Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.NullLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getNullLiteral()
	 * @generated
	 */
	int NULL_LITERAL = 68;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NULL_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NULL_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NULL_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The number of structural features of the '<em>Null Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NULL_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Null Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NULL_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PackageImpl <em>Package</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PackageImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPackage()
	 * @generated
	 */
	int PACKAGE = 69;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Owned Elements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__OWNED_ELEMENTS = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Model</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__MODEL = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Owned Packages</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__OWNED_PACKAGES = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__PACKAGE = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Usages In Package Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE__USAGES_IN_PACKAGE_ACCESS = NAMED_ELEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 5;

	/**
	 * The number of operations of the '<em>Package</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PackageAccessImpl <em>Package Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PackageAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPackageAccess()
	 * @generated
	 */
	int PACKAGE_ACCESS = 70;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS__COMMENTS = NAMESPACE_ACCESS__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS__ORIGINAL_COMPILATION_UNIT = NAMESPACE_ACCESS__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS__ORIGINAL_CLASS_FILE = NAMESPACE_ACCESS__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Package</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS__PACKAGE = NAMESPACE_ACCESS_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS__QUALIFIER = NAMESPACE_ACCESS_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Package Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS_FEATURE_COUNT = NAMESPACE_ACCESS_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Package Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PACKAGE_ACCESS_OPERATION_COUNT = NAMESPACE_ACCESS_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ParameterizedTypeImpl <em>Parameterized Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ParameterizedTypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getParameterizedType()
	 * @generated
	 */
	int PARAMETERIZED_TYPE = 71;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__TYPE = TYPE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE__TYPE_ARGUMENTS = TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Parameterized Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE_FEATURE_COUNT = TYPE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Parameterized Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETERIZED_TYPE_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ParenthesizedExpressionImpl <em>Parenthesized Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ParenthesizedExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getParenthesizedExpression()
	 * @generated
	 */
	int PARENTHESIZED_EXPRESSION = 72;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION__EXPRESSION = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Parenthesized Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Parenthesized Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARENTHESIZED_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PostfixExpressionImpl <em>Postfix Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PostfixExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPostfixExpression()
	 * @generated
	 */
	int POSTFIX_EXPRESSION = 73;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION__OPERATOR = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION__OPERAND = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Postfix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Postfix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int POSTFIX_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrefixExpressionImpl <em>Prefix Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrefixExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrefixExpression()
	 * @generated
	 */
	int PREFIX_EXPRESSION = 74;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION__OPERATOR = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION__OPERAND = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Prefix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Prefix Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PREFIX_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeImpl <em>Primitive Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveType()
	 * @generated
	 */
	int PRIMITIVE_TYPE = 75;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FEATURE_COUNT = TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeBooleanImpl <em>Primitive Type Boolean</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeBooleanImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeBoolean()
	 * @generated
	 */
	int PRIMITIVE_TYPE_BOOLEAN = 76;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BOOLEAN_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeByteImpl <em>Primitive Type Byte</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeByteImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeByte()
	 * @generated
	 */
	int PRIMITIVE_TYPE_BYTE = 77;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Byte</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Byte</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_BYTE_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeCharImpl <em>Primitive Type Char</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeCharImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeChar()
	 * @generated
	 */
	int PRIMITIVE_TYPE_CHAR = 78;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Char</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Char</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_CHAR_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeDoubleImpl <em>Primitive Type Double</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeDoubleImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeDouble()
	 * @generated
	 */
	int PRIMITIVE_TYPE_DOUBLE = 79;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Double</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Double</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_DOUBLE_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeShortImpl <em>Primitive Type Short</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeShortImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeShort()
	 * @generated
	 */
	int PRIMITIVE_TYPE_SHORT = 80;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Short</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Short</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_SHORT_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeFloatImpl <em>Primitive Type Float</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeFloatImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeFloat()
	 * @generated
	 */
	int PRIMITIVE_TYPE_FLOAT = 81;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Float</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Float</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_FLOAT_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeIntImpl <em>Primitive Type Int</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeIntImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeInt()
	 * @generated
	 */
	int PRIMITIVE_TYPE_INT = 82;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Int</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Int</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_INT_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeLongImpl <em>Primitive Type Long</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeLongImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeLong()
	 * @generated
	 */
	int PRIMITIVE_TYPE_LONG = 83;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Long</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Long</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_LONG_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.PrimitiveTypeVoidImpl <em>Primitive Type Void</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.PrimitiveTypeVoidImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeVoid()
	 * @generated
	 */
	int PRIMITIVE_TYPE_VOID = 84;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__COMMENTS = PRIMITIVE_TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__ORIGINAL_COMPILATION_UNIT = PRIMITIVE_TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__ORIGINAL_CLASS_FILE = PRIMITIVE_TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__NAME = PRIMITIVE_TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__PROXY = PRIMITIVE_TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__USAGES_IN_IMPORTS = PRIMITIVE_TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID__USAGES_IN_TYPE_ACCESS = PRIMITIVE_TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Primitive Type Void</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID_FEATURE_COUNT = PRIMITIVE_TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Primitive Type Void</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PRIMITIVE_TYPE_VOID_OPERATION_COUNT = PRIMITIVE_TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ReturnStatementImpl <em>Return Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ReturnStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getReturnStatement()
	 * @generated
	 */
	int RETURN_STATEMENT = 85;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Return Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Return Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURN_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.SingleVariableAccessImpl <em>Single Variable Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SingleVariableAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSingleVariableAccess()
	 * @generated
	 */
	int SINGLE_VARIABLE_ACCESS = 86;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS__VARIABLE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS__QUALIFIER = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Single Variable Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Single Variable Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.VariableDeclarationImpl <em>Variable Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.VariableDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclaration()
	 * @generated
	 */
	int VARIABLE_DECLARATION = 120;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__INITIALIZER = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS = NAMED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.SingleVariableDeclarationImpl <em>Single Variable Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SingleVariableDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSingleVariableDeclaration()
	 * @generated
	 */
	int SINGLE_VARIABLE_DECLARATION = 87;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__COMMENTS = VARIABLE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT = VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE = VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__NAME = VARIABLE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__PROXY = VARIABLE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__USAGES_IN_IMPORTS = VARIABLE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS = VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__INITIALIZER = VARIABLE_DECLARATION__INITIALIZER;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS = VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__MODIFIER = VARIABLE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Varargs</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__VARARGS = VARIABLE_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__TYPE = VARIABLE_DECLARATION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__ANNOTATIONS = VARIABLE_DECLARATION_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Method Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION = VARIABLE_DECLARATION_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Catch Clause</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE = VARIABLE_DECLARATION_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Enhanced For Statement</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT = VARIABLE_DECLARATION_FEATURE_COUNT + 6;

	/**
	 * The number of structural features of the '<em>Single Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION_FEATURE_COUNT = VARIABLE_DECLARATION_FEATURE_COUNT + 7;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Single Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SINGLE_VARIABLE_DECLARATION_OPERATION_COUNT = VARIABLE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.StringLiteralImpl <em>String Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.StringLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getStringLiteral()
	 * @generated
	 */
	int STRING_LITERAL = 89;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Escaped Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL__ESCAPED_VALUE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>String Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>String Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRING_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.SuperConstructorInvocationImpl <em>Super Constructor Invocation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SuperConstructorInvocationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSuperConstructorInvocation()
	 * @generated
	 */
	int SUPER_CONSTRUCTOR_INVOCATION = 90;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__METHOD = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__ARGUMENTS = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__TYPE_ARGUMENTS = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION__EXPRESSION = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Super Constructor Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 4;

	/**
	 * The operation id for the '<em>No Redundant Super Call</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION___NO_REDUNDANT_SUPER_CALL__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Super Constructor Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_CONSTRUCTOR_INVOCATION_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.SuperFieldAccessImpl <em>Super Field Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SuperFieldAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSuperFieldAccess()
	 * @generated
	 */
	int SUPER_FIELD_ACCESS = 91;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS__COMMENTS = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS__QUALIFIER = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__QUALIFIER;

	/**
	 * The feature id for the '<em><b>Field</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS__FIELD = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Super Field Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS_FEATURE_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Super Field Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_FIELD_ACCESS_OPERATION_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.SuperMethodInvocationImpl <em>Super Method Invocation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SuperMethodInvocationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSuperMethodInvocation()
	 * @generated
	 */
	int SUPER_METHOD_INVOCATION = 92;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__COMMENTS = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__QUALIFIER = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__QUALIFIER;

	/**
	 * The feature id for the '<em><b>Method</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__METHOD = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__ARGUMENTS = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Type Arguments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION__TYPE_ARGUMENTS = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Super Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION_FEATURE_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Super Method Invocation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUPER_METHOD_INVOCATION_OPERATION_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.SwitchCaseImpl <em>Switch Case</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SwitchCaseImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSwitchCase()
	 * @generated
	 */
	int SWITCH_CASE = 93;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Default</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE__DEFAULT = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE__EXPRESSION = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Switch Case</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Switch Case</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_CASE_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.SwitchStatementImpl <em>Switch Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SwitchStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSwitchStatement()
	 * @generated
	 */
	int SWITCH_STATEMENT = 94;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Statements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT__STATEMENTS = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Switch Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>More Than3 Cases</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT___MORE_THAN3_CASES__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Switch Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SWITCH_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.SynchronizedStatementImpl <em>Synchronized Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.SynchronizedStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getSynchronizedStatement()
	 * @generated
	 */
	int SYNCHRONIZED_STATEMENT = 95;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Synchronized Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Has Statements</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT___HAS_STATEMENTS__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Synchronized Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SYNCHRONIZED_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.TagElementImpl <em>Tag Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TagElementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTagElement()
	 * @generated
	 */
	int TAG_ELEMENT = 96;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Tag Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT__TAG_NAME = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Fragments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT__FRAGMENTS = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Tag Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Tag Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TAG_ELEMENT_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TextElementImpl <em>Text Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TextElementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTextElement()
	 * @generated
	 */
	int TEXT_ELEMENT = 97;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT__COMMENTS = AST_NODE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT__ORIGINAL_COMPILATION_UNIT = AST_NODE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT__ORIGINAL_CLASS_FILE = AST_NODE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Text</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT__TEXT = AST_NODE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Text Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT_FEATURE_COUNT = AST_NODE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Text Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEXT_ELEMENT_OPERATION_COUNT = AST_NODE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ThisExpressionImpl <em>This Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ThisExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getThisExpression()
	 * @generated
	 */
	int THIS_EXPRESSION = 98;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION__COMMENTS = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION__QUALIFIER = ABSTRACT_TYPE_QUALIFIED_EXPRESSION__QUALIFIER;

	/**
	 * The number of structural features of the '<em>This Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION_FEATURE_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>This Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THIS_EXPRESSION_OPERATION_COUNT = ABSTRACT_TYPE_QUALIFIED_EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.ThrowStatementImpl <em>Throw Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.ThrowStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getThrowStatement()
	 * @generated
	 */
	int THROW_STATEMENT = 99;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Throw Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Throw Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int THROW_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TryStatementImpl <em>Try Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TryStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTryStatement()
	 * @generated
	 */
	int TRY_STATEMENT = 100;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Finally</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__FINALLY = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Catch Clauses</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT__CATCH_CLAUSES = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Try Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Try Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRY_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeAccessImpl <em>Type Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTypeAccess()
	 * @generated
	 */
	int TYPE_ACCESS = 102;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS__TYPE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS__QUALIFIER = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Type Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Type Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeDeclarationStatementImpl <em>Type Declaration Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeDeclarationStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTypeDeclarationStatement()
	 * @generated
	 */
	int TYPE_DECLARATION_STATEMENT = 104;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Declaration</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT__DECLARATION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Type Declaration Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Type Declaration Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_DECLARATION_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeLiteralImpl <em>Type Literal</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeLiteralImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTypeLiteral()
	 * @generated
	 */
	int TYPE_LITERAL = 105;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL__TYPE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Type Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Type Literal</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_LITERAL_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.TypeParameterImpl <em>Type Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.TypeParameterImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getTypeParameter()
	 * @generated
	 */
	int TYPE_PARAMETER = 106;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Bounds</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER__BOUNDS = TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Type Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER_FEATURE_COUNT = TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Type Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPE_PARAMETER_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedItemImpl <em>Unresolved Item</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedItemImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedItem()
	 * @generated
	 */
	int UNRESOLVED_ITEM = 107;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__COMMENTS = NAMED_ELEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__ORIGINAL_COMPILATION_UNIT = NAMED_ELEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__ORIGINAL_CLASS_FILE = NAMED_ELEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__PROXY = NAMED_ELEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM__USAGES_IN_IMPORTS = NAMED_ELEMENT__USAGES_IN_IMPORTS;

	/**
	 * The number of structural features of the '<em>Unresolved Item</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Unresolved Item</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedItemAccessImpl <em>Unresolved Item Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedItemAccessImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedItemAccess()
	 * @generated
	 */
	int UNRESOLVED_ITEM_ACCESS = 108;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Element</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS__ELEMENT = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS__QUALIFIER = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Unresolved Item Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Unresolved Item Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ITEM_ACCESS_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedAnnotationDeclarationImpl <em>Unresolved Annotation Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedAnnotationDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedAnnotationDeclaration()
	 * @generated
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION = 109;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__COMMENTS = ANNOTATION_TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__ORIGINAL_COMPILATION_UNIT = ANNOTATION_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__ORIGINAL_CLASS_FILE = ANNOTATION_TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__NAME = ANNOTATION_TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__PROXY = ANNOTATION_TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__USAGES_IN_IMPORTS = ANNOTATION_TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__ABSTRACT_TYPE_DECLARATION = ANNOTATION_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__ANNOTATIONS = ANNOTATION_TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ANNOTATION_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__MODIFIER = ANNOTATION_TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__USAGES_IN_TYPE_ACCESS = ANNOTATION_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__BODY_DECLARATIONS = ANNOTATION_TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__COMMENTS_BEFORE_BODY = ANNOTATION_TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__COMMENTS_AFTER_BODY = ANNOTATION_TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__PACKAGE = ANNOTATION_TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION__SUPER_INTERFACES = ANNOTATION_TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The number of structural features of the '<em>Unresolved Annotation Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION_FEATURE_COUNT = ANNOTATION_TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION___IMPLEMENTS____STRING = ANNOTATION_TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Unresolved Annotation Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_DECLARATION_OPERATION_COUNT = ANNOTATION_TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedAnnotationTypeMemberDeclarationImpl <em>Unresolved Annotation Type Member Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedAnnotationTypeMemberDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedAnnotationTypeMemberDeclaration()
	 * @generated
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION = 110;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__COMMENTS = ANNOTATION_TYPE_MEMBER_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_COMPILATION_UNIT = ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_CLASS_FILE = ANNOTATION_TYPE_MEMBER_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__NAME = ANNOTATION_TYPE_MEMBER_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__PROXY = ANNOTATION_TYPE_MEMBER_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES_IN_IMPORTS = ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__ABSTRACT_TYPE_DECLARATION = ANNOTATION_TYPE_MEMBER_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__ANNOTATIONS = ANNOTATION_TYPE_MEMBER_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ANNOTATION_TYPE_MEMBER_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__MODIFIER = ANNOTATION_TYPE_MEMBER_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Default</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__DEFAULT = ANNOTATION_TYPE_MEMBER_DECLARATION__DEFAULT;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__TYPE = ANNOTATION_TYPE_MEMBER_DECLARATION__TYPE;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES = ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES;

	/**
	 * The number of structural features of the '<em>Unresolved Annotation Type Member Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION_FEATURE_COUNT = ANNOTATION_TYPE_MEMBER_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Unresolved Annotation Type Member Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION_OPERATION_COUNT = ANNOTATION_TYPE_MEMBER_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedClassDeclarationImpl <em>Unresolved Class Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedClassDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedClassDeclaration()
	 * @generated
	 */
	int UNRESOLVED_CLASS_DECLARATION = 111;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__COMMENTS = CLASS_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__ORIGINAL_COMPILATION_UNIT = CLASS_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__ORIGINAL_CLASS_FILE = CLASS_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__NAME = CLASS_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__PROXY = CLASS_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__USAGES_IN_IMPORTS = CLASS_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__ABSTRACT_TYPE_DECLARATION = CLASS_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__ANNOTATIONS = CLASS_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = CLASS_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__MODIFIER = CLASS_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__USAGES_IN_TYPE_ACCESS = CLASS_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__BODY_DECLARATIONS = CLASS_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__COMMENTS_BEFORE_BODY = CLASS_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__COMMENTS_AFTER_BODY = CLASS_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__PACKAGE = CLASS_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__SUPER_INTERFACES = CLASS_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__TYPE_PARAMETERS = CLASS_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Super Class</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION__SUPER_CLASS = CLASS_DECLARATION__SUPER_CLASS;

	/**
	 * The number of structural features of the '<em>Unresolved Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION_FEATURE_COUNT = CLASS_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___IMPLEMENTS____STRING = CLASS_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The operation id for the '<em>Get Methods</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___GET_METHODS = CLASS_DECLARATION___GET_METHODS;

	/**
	 * The operation id for the '<em>Has Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___HAS_EQUALS = CLASS_DECLARATION___HAS_EQUALS;

	/**
	 * The operation id for the '<em>Has Hashcode</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___HAS_HASHCODE = CLASS_DECLARATION___HAS_HASHCODE;

	/**
	 * The operation id for the '<em>Has Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___HAS_COMPARE_TO = CLASS_DECLARATION___HAS_COMPARE_TO;

	/**
	 * The operation id for the '<em>No Covariant Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___NO_COVARIANT_COMPARE_TO__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___NO_COVARIANT_COMPARE_TO__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Hash Code And Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___HASH_CODE_AND_EQUALS__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___HASH_CODE_AND_EQUALS__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Serial UID In Serializable Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___SERIAL_UID_IN_SERIALIZABLE_CLASS__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___SERIAL_UID_IN_SERIALIZABLE_CLASS__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Equals And Compare To</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___EQUALS_AND_COMPARE_TO__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___EQUALS_AND_COMPARE_TO__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>No Redundant Interface Impl</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___NO_REDUNDANT_INTERFACE_IMPL__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___NO_REDUNDANT_INTERFACE_IMPL__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Clone In Cloneable</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___CLONE_IN_CLONEABLE__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___CLONE_IN_CLONEABLE__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>No Covariant Equals</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___NO_COVARIANT_EQUALS__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___NO_COVARIANT_EQUALS__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>No Obscured Variables</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___NO_OBSCURED_VARIABLES__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___NO_OBSCURED_VARIABLES__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Comparator Implements Serializable</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION___COMPARATOR_IMPLEMENTS_SERIALIZABLE__DIAGNOSTICCHAIN_MAP = CLASS_DECLARATION___COMPARATOR_IMPLEMENTS_SERIALIZABLE__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Unresolved Class Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_CLASS_DECLARATION_OPERATION_COUNT = CLASS_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedEnumDeclarationImpl <em>Unresolved Enum Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedEnumDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedEnumDeclaration()
	 * @generated
	 */
	int UNRESOLVED_ENUM_DECLARATION = 112;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__COMMENTS = ENUM_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ORIGINAL_COMPILATION_UNIT = ENUM_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ORIGINAL_CLASS_FILE = ENUM_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__NAME = ENUM_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__PROXY = ENUM_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__USAGES_IN_IMPORTS = ENUM_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ABSTRACT_TYPE_DECLARATION = ENUM_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ANNOTATIONS = ENUM_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ENUM_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__MODIFIER = ENUM_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__USAGES_IN_TYPE_ACCESS = ENUM_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__BODY_DECLARATIONS = ENUM_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__COMMENTS_BEFORE_BODY = ENUM_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__COMMENTS_AFTER_BODY = ENUM_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__PACKAGE = ENUM_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__SUPER_INTERFACES = ENUM_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Enum Constants</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION__ENUM_CONSTANTS = ENUM_DECLARATION__ENUM_CONSTANTS;

	/**
	 * The number of structural features of the '<em>Unresolved Enum Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION_FEATURE_COUNT = ENUM_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION___IMPLEMENTS____STRING = ENUM_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Unresolved Enum Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_ENUM_DECLARATION_OPERATION_COUNT = ENUM_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedInterfaceDeclarationImpl <em>Unresolved Interface Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedInterfaceDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedInterfaceDeclaration()
	 * @generated
	 */
	int UNRESOLVED_INTERFACE_DECLARATION = 113;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__COMMENTS = INTERFACE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__ORIGINAL_COMPILATION_UNIT = INTERFACE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__ORIGINAL_CLASS_FILE = INTERFACE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__NAME = INTERFACE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__PROXY = INTERFACE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__USAGES_IN_IMPORTS = INTERFACE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__ABSTRACT_TYPE_DECLARATION = INTERFACE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__ANNOTATIONS = INTERFACE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = INTERFACE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__MODIFIER = INTERFACE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__USAGES_IN_TYPE_ACCESS = INTERFACE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__BODY_DECLARATIONS = INTERFACE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__COMMENTS_BEFORE_BODY = INTERFACE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__COMMENTS_AFTER_BODY = INTERFACE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__PACKAGE = INTERFACE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__SUPER_INTERFACES = INTERFACE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION__TYPE_PARAMETERS = INTERFACE_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The number of structural features of the '<em>Unresolved Interface Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION_FEATURE_COUNT = INTERFACE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION___IMPLEMENTS____STRING = INTERFACE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Unresolved Interface Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_INTERFACE_DECLARATION_OPERATION_COUNT = INTERFACE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedLabeledStatementImpl <em>Unresolved Labeled Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedLabeledStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedLabeledStatement()
	 * @generated
	 */
	int UNRESOLVED_LABELED_STATEMENT = 114;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__COMMENTS = LABELED_STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__ORIGINAL_COMPILATION_UNIT = LABELED_STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__ORIGINAL_CLASS_FILE = LABELED_STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__NAME = LABELED_STATEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__PROXY = LABELED_STATEMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__USAGES_IN_IMPORTS = LABELED_STATEMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__BODY = LABELED_STATEMENT__BODY;

	/**
	 * The feature id for the '<em><b>Usages In Break Statements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__USAGES_IN_BREAK_STATEMENTS = LABELED_STATEMENT__USAGES_IN_BREAK_STATEMENTS;

	/**
	 * The feature id for the '<em><b>Usages In Continue Statements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT__USAGES_IN_CONTINUE_STATEMENTS = LABELED_STATEMENT__USAGES_IN_CONTINUE_STATEMENTS;

	/**
	 * The number of structural features of the '<em>Unresolved Labeled Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT_FEATURE_COUNT = LABELED_STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Unresolved Labeled Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_LABELED_STATEMENT_OPERATION_COUNT = LABELED_STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedMethodDeclarationImpl <em>Unresolved Method Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedMethodDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedMethodDeclaration()
	 * @generated
	 */
	int UNRESOLVED_METHOD_DECLARATION = 115;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__COMMENTS = METHOD_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT = METHOD_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__ORIGINAL_CLASS_FILE = METHOD_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__NAME = METHOD_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__PROXY = METHOD_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__USAGES_IN_IMPORTS = METHOD_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION = METHOD_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__ANNOTATIONS = METHOD_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = METHOD_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__MODIFIER = METHOD_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__BODY = METHOD_DECLARATION__BODY;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__PARAMETERS = METHOD_DECLARATION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__THROWN_EXCEPTIONS = METHOD_DECLARATION__THROWN_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Type Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__TYPE_PARAMETERS = METHOD_DECLARATION__TYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS = METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS;

	/**
	 * The feature id for the '<em><b>Usages</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__USAGES = METHOD_DECLARATION__USAGES;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS = METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS;

	/**
	 * The feature id for the '<em><b>Return Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__RETURN_TYPE = METHOD_DECLARATION__RETURN_TYPE;

	/**
	 * The feature id for the '<em><b>Redefined Method Declaration</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION = METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION;

	/**
	 * The feature id for the '<em><b>Redefinitions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION__REDEFINITIONS = METHOD_DECLARATION__REDEFINITIONS;

	/**
	 * The number of structural features of the '<em>Unresolved Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION_FEATURE_COUNT = METHOD_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Parameters Effectively Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP = METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Local Method Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP = METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The operation id for the '<em>Should Start With Lower Case</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION___SHOULD_START_WITH_LOWER_CASE__DIAGNOSTICCHAIN_MAP = METHOD_DECLARATION___SHOULD_START_WITH_LOWER_CASE__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Unresolved Method Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_METHOD_DECLARATION_OPERATION_COUNT = METHOD_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedSingleVariableDeclarationImpl <em>Unresolved Single Variable Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedSingleVariableDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedSingleVariableDeclaration()
	 * @generated
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION = 116;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__COMMENTS = SINGLE_VARIABLE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT = SINGLE_VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE = SINGLE_VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__NAME = SINGLE_VARIABLE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__PROXY = SINGLE_VARIABLE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__USAGES_IN_IMPORTS = SINGLE_VARIABLE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS = SINGLE_VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__INITIALIZER = SINGLE_VARIABLE_DECLARATION__INITIALIZER;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS = SINGLE_VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__MODIFIER = SINGLE_VARIABLE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Varargs</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__VARARGS = SINGLE_VARIABLE_DECLARATION__VARARGS;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__TYPE = SINGLE_VARIABLE_DECLARATION__TYPE;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__ANNOTATIONS = SINGLE_VARIABLE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Method Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION = SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION;

	/**
	 * The feature id for the '<em><b>Catch Clause</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE = SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE;

	/**
	 * The feature id for the '<em><b>Enhanced For Statement</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT = SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT;

	/**
	 * The number of structural features of the '<em>Unresolved Single Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION_FEATURE_COUNT = SINGLE_VARIABLE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = SINGLE_VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Unresolved Single Variable Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_SINGLE_VARIABLE_DECLARATION_OPERATION_COUNT = SINGLE_VARIABLE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedTypeImpl <em>Unresolved Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedTypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedType()
	 * @generated
	 */
	int UNRESOLVED_TYPE = 117;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The number of structural features of the '<em>Unresolved Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_FEATURE_COUNT = TYPE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Unresolved Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedTypeDeclarationImpl <em>Unresolved Type Declaration</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedTypeDeclarationImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedTypeDeclaration()
	 * @generated
	 */
	int UNRESOLVED_TYPE_DECLARATION = 118;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__COMMENTS = ABSTRACT_TYPE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT = ABSTRACT_TYPE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__ORIGINAL_CLASS_FILE = ABSTRACT_TYPE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__NAME = ABSTRACT_TYPE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__PROXY = ABSTRACT_TYPE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__USAGES_IN_IMPORTS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Abstract Type Declaration</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION = ABSTRACT_TYPE_DECLARATION__ABSTRACT_TYPE_DECLARATION;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__ANNOTATIONS = ABSTRACT_TYPE_DECLARATION__ANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = ABSTRACT_TYPE_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__MODIFIER = ABSTRACT_TYPE_DECLARATION__MODIFIER;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS = ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Body Declarations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__BODY_DECLARATIONS = ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS;

	/**
	 * The feature id for the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__COMMENTS_BEFORE_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY;

	/**
	 * The feature id for the '<em><b>Comments After Body</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__COMMENTS_AFTER_BODY = ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY;

	/**
	 * The feature id for the '<em><b>Package</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__PACKAGE = ABSTRACT_TYPE_DECLARATION__PACKAGE;

	/**
	 * The feature id for the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION__SUPER_INTERFACES = ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES;

	/**
	 * The number of structural features of the '<em>Unresolved Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION_FEATURE_COUNT = ABSTRACT_TYPE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Implements </em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION___IMPLEMENTS____STRING = ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING;

	/**
	 * The number of operations of the '<em>Unresolved Type Declaration</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_TYPE_DECLARATION_OPERATION_COUNT = ABSTRACT_TYPE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.VariableDeclarationFragmentImpl <em>Variable Declaration Fragment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.VariableDeclarationFragmentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationFragment()
	 * @generated
	 */
	int VARIABLE_DECLARATION_FRAGMENT = 122;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__COMMENTS = VARIABLE_DECLARATION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_COMPILATION_UNIT = VARIABLE_DECLARATION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_CLASS_FILE = VARIABLE_DECLARATION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__NAME = VARIABLE_DECLARATION__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__PROXY = VARIABLE_DECLARATION__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__USAGES_IN_IMPORTS = VARIABLE_DECLARATION__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__EXTRA_ARRAY_DIMENSIONS = VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__INITIALIZER = VARIABLE_DECLARATION__INITIALIZER;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__USAGE_IN_VARIABLE_ACCESS = VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS;

	/**
	 * The feature id for the '<em><b>Variables Container</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT__VARIABLES_CONTAINER = VARIABLE_DECLARATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Variable Declaration Fragment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT_FEATURE_COUNT = VARIABLE_DECLARATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Variable Declaration Fragment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_FRAGMENT_OPERATION_COUNT = VARIABLE_DECLARATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.UnresolvedVariableDeclarationFragmentImpl <em>Unresolved Variable Declaration Fragment</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.UnresolvedVariableDeclarationFragmentImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedVariableDeclarationFragment()
	 * @generated
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT = 119;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__COMMENTS = VARIABLE_DECLARATION_FRAGMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_COMPILATION_UNIT = VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_CLASS_FILE = VARIABLE_DECLARATION_FRAGMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__NAME = VARIABLE_DECLARATION_FRAGMENT__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__PROXY = VARIABLE_DECLARATION_FRAGMENT__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__USAGES_IN_IMPORTS = VARIABLE_DECLARATION_FRAGMENT__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__EXTRA_ARRAY_DIMENSIONS = VARIABLE_DECLARATION_FRAGMENT__EXTRA_ARRAY_DIMENSIONS;

	/**
	 * The feature id for the '<em><b>Initializer</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__INITIALIZER = VARIABLE_DECLARATION_FRAGMENT__INITIALIZER;

	/**
	 * The feature id for the '<em><b>Usage In Variable Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__USAGE_IN_VARIABLE_ACCESS = VARIABLE_DECLARATION_FRAGMENT__USAGE_IN_VARIABLE_ACCESS;

	/**
	 * The feature id for the '<em><b>Variables Container</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT__VARIABLES_CONTAINER = VARIABLE_DECLARATION_FRAGMENT__VARIABLES_CONTAINER;

	/**
	 * The number of structural features of the '<em>Unresolved Variable Declaration Fragment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT_FEATURE_COUNT = VARIABLE_DECLARATION_FRAGMENT_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Variable Is Used</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = VARIABLE_DECLARATION_FRAGMENT___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP;

	/**
	 * The number of operations of the '<em>Unresolved Variable Declaration Fragment</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT_OPERATION_COUNT = VARIABLE_DECLARATION_FRAGMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.VariableDeclarationExpressionImpl <em>Variable Declaration Expression</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.VariableDeclarationExpressionImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationExpression()
	 * @generated
	 */
	int VARIABLE_DECLARATION_EXPRESSION = 121;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__COMMENTS = EXPRESSION__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__ORIGINAL_COMPILATION_UNIT = EXPRESSION__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__ORIGINAL_CLASS_FILE = EXPRESSION__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__TYPE = EXPRESSION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Fragments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS = EXPRESSION_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__MODIFIER = EXPRESSION_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS = EXPRESSION_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Variable Declaration Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION_FEATURE_COUNT = EXPRESSION_FEATURE_COUNT + 4;

	/**
	 * The operation id for the '<em>Public Variable Is Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION___PUBLIC_VARIABLE_IS_FINAL__DIAGNOSTICCHAIN_MAP = EXPRESSION_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Variable Declaration Expression</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_EXPRESSION_OPERATION_COUNT = EXPRESSION_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.VariableDeclarationStatementImpl <em>Variable Declaration Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.VariableDeclarationStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationStatement()
	 * @generated
	 */
	int VARIABLE_DECLARATION_STATEMENT = 123;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__TYPE = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Fragments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__FRAGMENTS = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__EXTRA_ARRAY_DIMENSIONS = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Modifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__MODIFIER = STATEMENT_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT__ANNOTATIONS = STATEMENT_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Variable Declaration Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 5;

	/**
	 * The operation id for the '<em>Public Variable Is Final</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT___PUBLIC_VARIABLE_IS_FINAL__DIAGNOSTICCHAIN_MAP = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The number of operations of the '<em>Variable Declaration Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_DECLARATION_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 1;

	/**
	 * The meta object id for the '{@link javaMM.impl.WildCardTypeImpl <em>Wild Card Type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.WildCardTypeImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getWildCardType()
	 * @generated
	 */
	int WILD_CARD_TYPE = 124;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__COMMENTS = TYPE__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__ORIGINAL_COMPILATION_UNIT = TYPE__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__ORIGINAL_CLASS_FILE = TYPE__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__NAME = TYPE__NAME;

	/**
	 * The feature id for the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__PROXY = TYPE__PROXY;

	/**
	 * The feature id for the '<em><b>Usages In Imports</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__USAGES_IN_IMPORTS = TYPE__USAGES_IN_IMPORTS;

	/**
	 * The feature id for the '<em><b>Usages In Type Access</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__USAGES_IN_TYPE_ACCESS = TYPE__USAGES_IN_TYPE_ACCESS;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__UPPER_BOUND = TYPE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Bound</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE__BOUND = TYPE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Wild Card Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE_FEATURE_COUNT = TYPE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Wild Card Type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WILD_CARD_TYPE_OPERATION_COUNT = TYPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.impl.WhileStatementImpl <em>While Statement</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.impl.WhileStatementImpl
	 * @see javaMM.impl.JavaMMPackageImpl#getWhileStatement()
	 * @generated
	 */
	int WHILE_STATEMENT = 125;

	/**
	 * The feature id for the '<em><b>Comments</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT__COMMENTS = STATEMENT__COMMENTS;

	/**
	 * The feature id for the '<em><b>Original Compilation Unit</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT__ORIGINAL_COMPILATION_UNIT = STATEMENT__ORIGINAL_COMPILATION_UNIT;

	/**
	 * The feature id for the '<em><b>Original Class File</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT__ORIGINAL_CLASS_FILE = STATEMENT__ORIGINAL_CLASS_FILE;

	/**
	 * The feature id for the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT__EXPRESSION = STATEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT__BODY = STATEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>While Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT_FEATURE_COUNT = STATEMENT_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>While Statement</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int WHILE_STATEMENT_OPERATION_COUNT = STATEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link javaMM.AssignmentKind <em>Assignment Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.AssignmentKind
	 * @see javaMM.impl.JavaMMPackageImpl#getAssignmentKind()
	 * @generated
	 */
	int ASSIGNMENT_KIND = 126;

	/**
	 * The meta object id for the '{@link javaMM.InfixExpressionKind <em>Infix Expression Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.InfixExpressionKind
	 * @see javaMM.impl.JavaMMPackageImpl#getInfixExpressionKind()
	 * @generated
	 */
	int INFIX_EXPRESSION_KIND = 127;

	/**
	 * The meta object id for the '{@link javaMM.InheritanceKind <em>Inheritance Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.InheritanceKind
	 * @see javaMM.impl.JavaMMPackageImpl#getInheritanceKind()
	 * @generated
	 */
	int INHERITANCE_KIND = 128;

	/**
	 * The meta object id for the '{@link javaMM.PostfixExpressionKind <em>Postfix Expression Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.PostfixExpressionKind
	 * @see javaMM.impl.JavaMMPackageImpl#getPostfixExpressionKind()
	 * @generated
	 */
	int POSTFIX_EXPRESSION_KIND = 129;

	/**
	 * The meta object id for the '{@link javaMM.PrefixExpressionKind <em>Prefix Expression Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.PrefixExpressionKind
	 * @see javaMM.impl.JavaMMPackageImpl#getPrefixExpressionKind()
	 * @generated
	 */
	int PREFIX_EXPRESSION_KIND = 130;

	/**
	 * The meta object id for the '{@link javaMM.VisibilityKind <em>Visibility Kind</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see javaMM.VisibilityKind
	 * @see javaMM.impl.JavaMMPackageImpl#getVisibilityKind()
	 * @generated
	 */
	int VISIBILITY_KIND = 131;


	/**
	 * Returns the meta object for class '{@link javaMM.AbstractMethodDeclaration <em>Abstract Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract Method Declaration</em>'.
	 * @see javaMM.AbstractMethodDeclaration
	 * @generated
	 */
	EClass getAbstractMethodDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AbstractMethodDeclaration#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getBody()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_Body();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractMethodDeclaration#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameters</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getParameters()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_Parameters();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractMethodDeclaration#getThrownExceptions <em>Thrown Exceptions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Thrown Exceptions</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getThrownExceptions()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_ThrownExceptions();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractMethodDeclaration#getTypeParameters <em>Type Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Type Parameters</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getTypeParameters()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_TypeParameters();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.AbstractMethodDeclaration#getUsagesInDocComments <em>Usages In Doc Comments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Doc Comments</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getUsagesInDocComments()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_UsagesInDocComments();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.AbstractMethodDeclaration#getUsages <em>Usages</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages</em>'.
	 * @see javaMM.AbstractMethodDeclaration#getUsages()
	 * @see #getAbstractMethodDeclaration()
	 * @generated
	 */
	EReference getAbstractMethodDeclaration_Usages();

	/**
	 * Returns the meta object for the '{@link javaMM.AbstractMethodDeclaration#parametersEffectivelyFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Parameters Effectively Final</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Parameters Effectively Final</em>' operation.
	 * @see javaMM.AbstractMethodDeclaration#parametersEffectivelyFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getAbstractMethodDeclaration__ParametersEffectivelyFinal__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.AbstractMethodDeclaration#localMethodIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Local Method Is Used</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Local Method Is Used</em>' operation.
	 * @see javaMM.AbstractMethodDeclaration#localMethodIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getAbstractMethodDeclaration__LocalMethodIsUsed__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.AbstractMethodInvocation <em>Abstract Method Invocation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract Method Invocation</em>'.
	 * @see javaMM.AbstractMethodInvocation
	 * @generated
	 */
	EClass getAbstractMethodInvocation();

	/**
	 * Returns the meta object for the reference '{@link javaMM.AbstractMethodInvocation#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Method</em>'.
	 * @see javaMM.AbstractMethodInvocation#getMethod()
	 * @see #getAbstractMethodInvocation()
	 * @generated
	 */
	EReference getAbstractMethodInvocation_Method();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractMethodInvocation#getArguments <em>Arguments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Arguments</em>'.
	 * @see javaMM.AbstractMethodInvocation#getArguments()
	 * @see #getAbstractMethodInvocation()
	 * @generated
	 */
	EReference getAbstractMethodInvocation_Arguments();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractMethodInvocation#getTypeArguments <em>Type Arguments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Type Arguments</em>'.
	 * @see javaMM.AbstractMethodInvocation#getTypeArguments()
	 * @see #getAbstractMethodInvocation()
	 * @generated
	 */
	EReference getAbstractMethodInvocation_TypeArguments();

	/**
	 * Returns the meta object for class '{@link javaMM.AbstractTypeDeclaration <em>Abstract Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract Type Declaration</em>'.
	 * @see javaMM.AbstractTypeDeclaration
	 * @generated
	 */
	EClass getAbstractTypeDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractTypeDeclaration#getBodyDeclarations <em>Body Declarations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Body Declarations</em>'.
	 * @see javaMM.AbstractTypeDeclaration#getBodyDeclarations()
	 * @see #getAbstractTypeDeclaration()
	 * @generated
	 */
	EReference getAbstractTypeDeclaration_BodyDeclarations();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractTypeDeclaration#getCommentsBeforeBody <em>Comments Before Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Comments Before Body</em>'.
	 * @see javaMM.AbstractTypeDeclaration#getCommentsBeforeBody()
	 * @see #getAbstractTypeDeclaration()
	 * @generated
	 */
	EReference getAbstractTypeDeclaration_CommentsBeforeBody();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractTypeDeclaration#getCommentsAfterBody <em>Comments After Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Comments After Body</em>'.
	 * @see javaMM.AbstractTypeDeclaration#getCommentsAfterBody()
	 * @see #getAbstractTypeDeclaration()
	 * @generated
	 */
	EReference getAbstractTypeDeclaration_CommentsAfterBody();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.AbstractTypeDeclaration#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Package</em>'.
	 * @see javaMM.AbstractTypeDeclaration#getPackage()
	 * @see #getAbstractTypeDeclaration()
	 * @generated
	 */
	EReference getAbstractTypeDeclaration_Package();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractTypeDeclaration#getSuperInterfaces <em>Super Interfaces</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Super Interfaces</em>'.
	 * @see javaMM.AbstractTypeDeclaration#getSuperInterfaces()
	 * @see #getAbstractTypeDeclaration()
	 * @generated
	 */
	EReference getAbstractTypeDeclaration_SuperInterfaces();

	/**
	 * Returns the meta object for the '{@link javaMM.AbstractTypeDeclaration#implements_(java.lang.String) <em>Implements </em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Implements </em>' operation.
	 * @see javaMM.AbstractTypeDeclaration#implements_(java.lang.String)
	 * @generated
	 */
	EOperation getAbstractTypeDeclaration__Implements___String();

	/**
	 * Returns the meta object for class '{@link javaMM.AbstractTypeQualifiedExpression <em>Abstract Type Qualified Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract Type Qualified Expression</em>'.
	 * @see javaMM.AbstractTypeQualifiedExpression
	 * @generated
	 */
	EClass getAbstractTypeQualifiedExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AbstractTypeQualifiedExpression#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.AbstractTypeQualifiedExpression#getQualifier()
	 * @see #getAbstractTypeQualifiedExpression()
	 * @generated
	 */
	EReference getAbstractTypeQualifiedExpression_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.AbstractVariablesContainer <em>Abstract Variables Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Abstract Variables Container</em>'.
	 * @see javaMM.AbstractVariablesContainer
	 * @generated
	 */
	EClass getAbstractVariablesContainer();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AbstractVariablesContainer#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.AbstractVariablesContainer#getType()
	 * @see #getAbstractVariablesContainer()
	 * @generated
	 */
	EReference getAbstractVariablesContainer_Type();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AbstractVariablesContainer#getFragments <em>Fragments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Fragments</em>'.
	 * @see javaMM.AbstractVariablesContainer#getFragments()
	 * @see #getAbstractVariablesContainer()
	 * @generated
	 */
	EReference getAbstractVariablesContainer_Fragments();

	/**
	 * Returns the meta object for class '{@link javaMM.Annotation <em>Annotation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Annotation</em>'.
	 * @see javaMM.Annotation
	 * @generated
	 */
	EClass getAnnotation();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.Annotation#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.Annotation#getType()
	 * @see #getAnnotation()
	 * @generated
	 */
	EReference getAnnotation_Type();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Annotation#getValues <em>Values</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Values</em>'.
	 * @see javaMM.Annotation#getValues()
	 * @see #getAnnotation()
	 * @generated
	 */
	EReference getAnnotation_Values();

	/**
	 * Returns the meta object for class '{@link javaMM.Archive <em>Archive</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Archive</em>'.
	 * @see javaMM.Archive
	 * @generated
	 */
	EClass getArchive();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Archive#getOriginalFilePath <em>Original File Path</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Original File Path</em>'.
	 * @see javaMM.Archive#getOriginalFilePath()
	 * @see #getArchive()
	 * @generated
	 */
	EAttribute getArchive_OriginalFilePath();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Archive#getClassFiles <em>Class Files</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Class Files</em>'.
	 * @see javaMM.Archive#getClassFiles()
	 * @see #getArchive()
	 * @generated
	 */
	EReference getArchive_ClassFiles();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.Archive#getManifest <em>Manifest</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Manifest</em>'.
	 * @see javaMM.Archive#getManifest()
	 * @see #getArchive()
	 * @generated
	 */
	EReference getArchive_Manifest();

	/**
	 * Returns the meta object for class '{@link javaMM.AssertStatement <em>Assert Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Assert Statement</em>'.
	 * @see javaMM.AssertStatement
	 * @generated
	 */
	EClass getAssertStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AssertStatement#getMessage <em>Message</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Message</em>'.
	 * @see javaMM.AssertStatement#getMessage()
	 * @see #getAssertStatement()
	 * @generated
	 */
	EReference getAssertStatement_Message();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AssertStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.AssertStatement#getExpression()
	 * @see #getAssertStatement()
	 * @generated
	 */
	EReference getAssertStatement_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.ASTNode <em>AST Node</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>AST Node</em>'.
	 * @see javaMM.ASTNode
	 * @generated
	 */
	EClass getASTNode();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ASTNode#getComments <em>Comments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Comments</em>'.
	 * @see javaMM.ASTNode#getComments()
	 * @see #getASTNode()
	 * @generated
	 */
	EReference getASTNode_Comments();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ASTNode#getOriginalCompilationUnit <em>Original Compilation Unit</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Original Compilation Unit</em>'.
	 * @see javaMM.ASTNode#getOriginalCompilationUnit()
	 * @see #getASTNode()
	 * @generated
	 */
	EReference getASTNode_OriginalCompilationUnit();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ASTNode#getOriginalClassFile <em>Original Class File</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Original Class File</em>'.
	 * @see javaMM.ASTNode#getOriginalClassFile()
	 * @see #getASTNode()
	 * @generated
	 */
	EReference getASTNode_OriginalClassFile();

	/**
	 * Returns the meta object for class '{@link javaMM.AnnotationMemberValuePair <em>Annotation Member Value Pair</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Annotation Member Value Pair</em>'.
	 * @see javaMM.AnnotationMemberValuePair
	 * @generated
	 */
	EClass getAnnotationMemberValuePair();

	/**
	 * Returns the meta object for the reference '{@link javaMM.AnnotationMemberValuePair#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member</em>'.
	 * @see javaMM.AnnotationMemberValuePair#getMember()
	 * @see #getAnnotationMemberValuePair()
	 * @generated
	 */
	EReference getAnnotationMemberValuePair_Member();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AnnotationMemberValuePair#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Value</em>'.
	 * @see javaMM.AnnotationMemberValuePair#getValue()
	 * @see #getAnnotationMemberValuePair()
	 * @generated
	 */
	EReference getAnnotationMemberValuePair_Value();

	/**
	 * Returns the meta object for class '{@link javaMM.AnnotationTypeDeclaration <em>Annotation Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Annotation Type Declaration</em>'.
	 * @see javaMM.AnnotationTypeDeclaration
	 * @generated
	 */
	EClass getAnnotationTypeDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.AnnotationTypeMemberDeclaration <em>Annotation Type Member Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Annotation Type Member Declaration</em>'.
	 * @see javaMM.AnnotationTypeMemberDeclaration
	 * @generated
	 */
	EClass getAnnotationTypeMemberDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AnnotationTypeMemberDeclaration#getDefault <em>Default</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Default</em>'.
	 * @see javaMM.AnnotationTypeMemberDeclaration#getDefault()
	 * @see #getAnnotationTypeMemberDeclaration()
	 * @generated
	 */
	EReference getAnnotationTypeMemberDeclaration_Default();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.AnnotationTypeMemberDeclaration#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.AnnotationTypeMemberDeclaration#getType()
	 * @see #getAnnotationTypeMemberDeclaration()
	 * @generated
	 */
	EReference getAnnotationTypeMemberDeclaration_Type();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.AnnotationTypeMemberDeclaration#getUsages <em>Usages</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages</em>'.
	 * @see javaMM.AnnotationTypeMemberDeclaration#getUsages()
	 * @see #getAnnotationTypeMemberDeclaration()
	 * @generated
	 */
	EReference getAnnotationTypeMemberDeclaration_Usages();

	/**
	 * Returns the meta object for class '{@link javaMM.AnonymousClassDeclaration <em>Anonymous Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Anonymous Class Declaration</em>'.
	 * @see javaMM.AnonymousClassDeclaration
	 * @generated
	 */
	EClass getAnonymousClassDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.AnonymousClassDeclaration#getBodyDeclarations <em>Body Declarations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Body Declarations</em>'.
	 * @see javaMM.AnonymousClassDeclaration#getBodyDeclarations()
	 * @see #getAnonymousClassDeclaration()
	 * @generated
	 */
	EReference getAnonymousClassDeclaration_BodyDeclarations();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.AnonymousClassDeclaration#getClassInstanceCreation <em>Class Instance Creation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Class Instance Creation</em>'.
	 * @see javaMM.AnonymousClassDeclaration#getClassInstanceCreation()
	 * @see #getAnonymousClassDeclaration()
	 * @generated
	 */
	EReference getAnonymousClassDeclaration_ClassInstanceCreation();

	/**
	 * Returns the meta object for class '{@link javaMM.ArrayAccess <em>Array Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Access</em>'.
	 * @see javaMM.ArrayAccess
	 * @generated
	 */
	EClass getArrayAccess();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayAccess#getArray <em>Array</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Array</em>'.
	 * @see javaMM.ArrayAccess#getArray()
	 * @see #getArrayAccess()
	 * @generated
	 */
	EReference getArrayAccess_Array();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayAccess#getIndex <em>Index</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Index</em>'.
	 * @see javaMM.ArrayAccess#getIndex()
	 * @see #getArrayAccess()
	 * @generated
	 */
	EReference getArrayAccess_Index();

	/**
	 * Returns the meta object for class '{@link javaMM.ArrayCreation <em>Array Creation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Creation</em>'.
	 * @see javaMM.ArrayCreation
	 * @generated
	 */
	EClass getArrayCreation();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ArrayCreation#getDimensions <em>Dimensions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Dimensions</em>'.
	 * @see javaMM.ArrayCreation#getDimensions()
	 * @see #getArrayCreation()
	 * @generated
	 */
	EReference getArrayCreation_Dimensions();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayCreation#getInitializer <em>Initializer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Initializer</em>'.
	 * @see javaMM.ArrayCreation#getInitializer()
	 * @see #getArrayCreation()
	 * @generated
	 */
	EReference getArrayCreation_Initializer();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayCreation#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.ArrayCreation#getType()
	 * @see #getArrayCreation()
	 * @generated
	 */
	EReference getArrayCreation_Type();

	/**
	 * Returns the meta object for class '{@link javaMM.ArrayInitializer <em>Array Initializer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Initializer</em>'.
	 * @see javaMM.ArrayInitializer
	 * @generated
	 */
	EClass getArrayInitializer();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ArrayInitializer#getExpressions <em>Expressions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Expressions</em>'.
	 * @see javaMM.ArrayInitializer#getExpressions()
	 * @see #getArrayInitializer()
	 * @generated
	 */
	EReference getArrayInitializer_Expressions();

	/**
	 * Returns the meta object for class '{@link javaMM.ArrayLengthAccess <em>Array Length Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Length Access</em>'.
	 * @see javaMM.ArrayLengthAccess
	 * @generated
	 */
	EClass getArrayLengthAccess();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayLengthAccess#getArray <em>Array</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Array</em>'.
	 * @see javaMM.ArrayLengthAccess#getArray()
	 * @see #getArrayLengthAccess()
	 * @generated
	 */
	EReference getArrayLengthAccess_Array();

	/**
	 * Returns the meta object for class '{@link javaMM.ArrayType <em>Array Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Type</em>'.
	 * @see javaMM.ArrayType
	 * @generated
	 */
	EClass getArrayType();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ArrayType#getDimensions <em>Dimensions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Dimensions</em>'.
	 * @see javaMM.ArrayType#getDimensions()
	 * @see #getArrayType()
	 * @generated
	 */
	EAttribute getArrayType_Dimensions();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ArrayType#getElementType <em>Element Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Element Type</em>'.
	 * @see javaMM.ArrayType#getElementType()
	 * @see #getArrayType()
	 * @generated
	 */
	EReference getArrayType_ElementType();

	/**
	 * Returns the meta object for class '{@link javaMM.Assignment <em>Assignment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Assignment</em>'.
	 * @see javaMM.Assignment
	 * @generated
	 */
	EClass getAssignment();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.Assignment#getLeftHandSide <em>Left Hand Side</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Left Hand Side</em>'.
	 * @see javaMM.Assignment#getLeftHandSide()
	 * @see #getAssignment()
	 * @generated
	 */
	EReference getAssignment_LeftHandSide();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Assignment#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see javaMM.Assignment#getOperator()
	 * @see #getAssignment()
	 * @generated
	 */
	EAttribute getAssignment_Operator();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.Assignment#getRightHandSide <em>Right Hand Side</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Right Hand Side</em>'.
	 * @see javaMM.Assignment#getRightHandSide()
	 * @see #getAssignment()
	 * @generated
	 */
	EReference getAssignment_RightHandSide();

	/**
	 * Returns the meta object for the '{@link javaMM.Assignment#noRedundantAssignment(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Redundant Assignment</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Redundant Assignment</em>' operation.
	 * @see javaMM.Assignment#noRedundantAssignment(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getAssignment__NoRedundantAssignment__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.BodyDeclaration <em>Body Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Body Declaration</em>'.
	 * @see javaMM.BodyDeclaration
	 * @generated
	 */
	EClass getBodyDeclaration();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.BodyDeclaration#getAbstractTypeDeclaration <em>Abstract Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Abstract Type Declaration</em>'.
	 * @see javaMM.BodyDeclaration#getAbstractTypeDeclaration()
	 * @see #getBodyDeclaration()
	 * @generated
	 */
	EReference getBodyDeclaration_AbstractTypeDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.BodyDeclaration#getAnnotations <em>Annotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Annotations</em>'.
	 * @see javaMM.BodyDeclaration#getAnnotations()
	 * @see #getBodyDeclaration()
	 * @generated
	 */
	EReference getBodyDeclaration_Annotations();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.BodyDeclaration#getAnonymousClassDeclarationOwner <em>Anonymous Class Declaration Owner</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Anonymous Class Declaration Owner</em>'.
	 * @see javaMM.BodyDeclaration#getAnonymousClassDeclarationOwner()
	 * @see #getBodyDeclaration()
	 * @generated
	 */
	EReference getBodyDeclaration_AnonymousClassDeclarationOwner();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.BodyDeclaration#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Modifier</em>'.
	 * @see javaMM.BodyDeclaration#getModifier()
	 * @see #getBodyDeclaration()
	 * @generated
	 */
	EReference getBodyDeclaration_Modifier();

	/**
	 * Returns the meta object for class '{@link javaMM.BooleanLiteral <em>Boolean Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Boolean Literal</em>'.
	 * @see javaMM.BooleanLiteral
	 * @generated
	 */
	EClass getBooleanLiteral();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.BooleanLiteral#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see javaMM.BooleanLiteral#getValue()
	 * @see #getBooleanLiteral()
	 * @generated
	 */
	EAttribute getBooleanLiteral_Value();

	/**
	 * Returns the meta object for class '{@link javaMM.BlockComment <em>Block Comment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Block Comment</em>'.
	 * @see javaMM.BlockComment
	 * @generated
	 */
	EClass getBlockComment();

	/**
	 * Returns the meta object for class '{@link javaMM.Block <em>Block</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Block</em>'.
	 * @see javaMM.Block
	 * @generated
	 */
	EClass getBlock();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Block#getStatements <em>Statements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Statements</em>'.
	 * @see javaMM.Block#getStatements()
	 * @see #getBlock()
	 * @generated
	 */
	EReference getBlock_Statements();

	/**
	 * Returns the meta object for the '{@link javaMM.Block#emptyBlockIsDocumented(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Empty Block Is Documented</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Empty Block Is Documented</em>' operation.
	 * @see javaMM.Block#emptyBlockIsDocumented(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getBlock__EmptyBlockIsDocumented__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.BreakStatement <em>Break Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Break Statement</em>'.
	 * @see javaMM.BreakStatement
	 * @generated
	 */
	EClass getBreakStatement();

	/**
	 * Returns the meta object for the reference '{@link javaMM.BreakStatement#getLabel <em>Label</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Label</em>'.
	 * @see javaMM.BreakStatement#getLabel()
	 * @see #getBreakStatement()
	 * @generated
	 */
	EReference getBreakStatement_Label();

	/**
	 * Returns the meta object for class '{@link javaMM.CastExpression <em>Cast Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cast Expression</em>'.
	 * @see javaMM.CastExpression
	 * @generated
	 */
	EClass getCastExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.CastExpression#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.CastExpression#getExpression()
	 * @see #getCastExpression()
	 * @generated
	 */
	EReference getCastExpression_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.CastExpression#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.CastExpression#getType()
	 * @see #getCastExpression()
	 * @generated
	 */
	EReference getCastExpression_Type();

	/**
	 * Returns the meta object for class '{@link javaMM.CatchClause <em>Catch Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Catch Clause</em>'.
	 * @see javaMM.CatchClause
	 * @generated
	 */
	EClass getCatchClause();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.CatchClause#getException <em>Exception</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Exception</em>'.
	 * @see javaMM.CatchClause#getException()
	 * @see #getCatchClause()
	 * @generated
	 */
	EReference getCatchClause_Exception();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.CatchClause#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.CatchClause#getBody()
	 * @see #getCatchClause()
	 * @generated
	 */
	EReference getCatchClause_Body();

	/**
	 * Returns the meta object for the '{@link javaMM.CatchClause#doesNotCatchDubiousExceptions(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Does Not Catch Dubious Exceptions</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Does Not Catch Dubious Exceptions</em>' operation.
	 * @see javaMM.CatchClause#doesNotCatchDubiousExceptions(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getCatchClause__DoesNotCatchDubiousExceptions__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.CatchClause#exceptionIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Exception Is Used</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Exception Is Used</em>' operation.
	 * @see javaMM.CatchClause#exceptionIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getCatchClause__ExceptionIsUsed__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.CharacterLiteral <em>Character Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Character Literal</em>'.
	 * @see javaMM.CharacterLiteral
	 * @generated
	 */
	EClass getCharacterLiteral();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.CharacterLiteral#getEscapedValue <em>Escaped Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Escaped Value</em>'.
	 * @see javaMM.CharacterLiteral#getEscapedValue()
	 * @see #getCharacterLiteral()
	 * @generated
	 */
	EAttribute getCharacterLiteral_EscapedValue();

	/**
	 * Returns the meta object for class '{@link javaMM.ClassFile <em>Class File</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Class File</em>'.
	 * @see javaMM.ClassFile
	 * @generated
	 */
	EClass getClassFile();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ClassFile#getOriginalFilePath <em>Original File Path</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Original File Path</em>'.
	 * @see javaMM.ClassFile#getOriginalFilePath()
	 * @see #getClassFile()
	 * @generated
	 */
	EAttribute getClassFile_OriginalFilePath();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ClassFile#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Type</em>'.
	 * @see javaMM.ClassFile#getType()
	 * @see #getClassFile()
	 * @generated
	 */
	EReference getClassFile_Type();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ClassFile#getAttachedSource <em>Attached Source</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attached Source</em>'.
	 * @see javaMM.ClassFile#getAttachedSource()
	 * @see #getClassFile()
	 * @generated
	 */
	EReference getClassFile_AttachedSource();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ClassFile#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Package</em>'.
	 * @see javaMM.ClassFile#getPackage()
	 * @see #getClassFile()
	 * @generated
	 */
	EReference getClassFile_Package();

	/**
	 * Returns the meta object for class '{@link javaMM.ClassInstanceCreation <em>Class Instance Creation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Class Instance Creation</em>'.
	 * @see javaMM.ClassInstanceCreation
	 * @generated
	 */
	EClass getClassInstanceCreation();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ClassInstanceCreation#getAnonymousClassDeclaration <em>Anonymous Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Anonymous Class Declaration</em>'.
	 * @see javaMM.ClassInstanceCreation#getAnonymousClassDeclaration()
	 * @see #getClassInstanceCreation()
	 * @generated
	 */
	EReference getClassInstanceCreation_AnonymousClassDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ClassInstanceCreation#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ClassInstanceCreation#getExpression()
	 * @see #getClassInstanceCreation()
	 * @generated
	 */
	EReference getClassInstanceCreation_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ClassInstanceCreation#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.ClassInstanceCreation#getType()
	 * @see #getClassInstanceCreation()
	 * @generated
	 */
	EReference getClassInstanceCreation_Type();

	/**
	 * Returns the meta object for class '{@link javaMM.ConstructorDeclaration <em>Constructor Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Constructor Declaration</em>'.
	 * @see javaMM.ConstructorDeclaration
	 * @generated
	 */
	EClass getConstructorDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.ConditionalExpression <em>Conditional Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Conditional Expression</em>'.
	 * @see javaMM.ConditionalExpression
	 * @generated
	 */
	EClass getConditionalExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ConditionalExpression#getElseExpression <em>Else Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Else Expression</em>'.
	 * @see javaMM.ConditionalExpression#getElseExpression()
	 * @see #getConditionalExpression()
	 * @generated
	 */
	EReference getConditionalExpression_ElseExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ConditionalExpression#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ConditionalExpression#getExpression()
	 * @see #getConditionalExpression()
	 * @generated
	 */
	EReference getConditionalExpression_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ConditionalExpression#getThenExpression <em>Then Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Then Expression</em>'.
	 * @see javaMM.ConditionalExpression#getThenExpression()
	 * @see #getConditionalExpression()
	 * @generated
	 */
	EReference getConditionalExpression_ThenExpression();

	/**
	 * Returns the meta object for class '{@link javaMM.ConstructorInvocation <em>Constructor Invocation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Constructor Invocation</em>'.
	 * @see javaMM.ConstructorInvocation
	 * @generated
	 */
	EClass getConstructorInvocation();

	/**
	 * Returns the meta object for class '{@link javaMM.ClassDeclaration <em>Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Class Declaration</em>'.
	 * @see javaMM.ClassDeclaration
	 * @generated
	 */
	EClass getClassDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ClassDeclaration#getSuperClass <em>Super Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Super Class</em>'.
	 * @see javaMM.ClassDeclaration#getSuperClass()
	 * @see #getClassDeclaration()
	 * @generated
	 */
	EReference getClassDeclaration_SuperClass();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#getMethods() <em>Get Methods</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Get Methods</em>' operation.
	 * @see javaMM.ClassDeclaration#getMethods()
	 * @generated
	 */
	EOperation getClassDeclaration__GetMethods();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#hasEquals() <em>Has Equals</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Has Equals</em>' operation.
	 * @see javaMM.ClassDeclaration#hasEquals()
	 * @generated
	 */
	EOperation getClassDeclaration__HasEquals();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#hasHashcode() <em>Has Hashcode</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Has Hashcode</em>' operation.
	 * @see javaMM.ClassDeclaration#hasHashcode()
	 * @generated
	 */
	EOperation getClassDeclaration__HasHashcode();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#hasCompareTo() <em>Has Compare To</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Has Compare To</em>' operation.
	 * @see javaMM.ClassDeclaration#hasCompareTo()
	 * @generated
	 */
	EOperation getClassDeclaration__HasCompareTo();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#noCovariantCompareTo(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Covariant Compare To</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Covariant Compare To</em>' operation.
	 * @see javaMM.ClassDeclaration#noCovariantCompareTo(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__NoCovariantCompareTo__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#hashCodeAndEquals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Hash Code And Equals</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Hash Code And Equals</em>' operation.
	 * @see javaMM.ClassDeclaration#hashCodeAndEquals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__HashCodeAndEquals__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#serialUIDInSerializableClass(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Serial UID In Serializable Class</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Serial UID In Serializable Class</em>' operation.
	 * @see javaMM.ClassDeclaration#serialUIDInSerializableClass(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__SerialUIDInSerializableClass__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#equalsAndCompareTo(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Equals And Compare To</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Equals And Compare To</em>' operation.
	 * @see javaMM.ClassDeclaration#equalsAndCompareTo(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__EqualsAndCompareTo__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#noRedundantInterfaceImpl(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Redundant Interface Impl</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Redundant Interface Impl</em>' operation.
	 * @see javaMM.ClassDeclaration#noRedundantInterfaceImpl(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__NoRedundantInterfaceImpl__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#cloneInCloneable(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Clone In Cloneable</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Clone In Cloneable</em>' operation.
	 * @see javaMM.ClassDeclaration#cloneInCloneable(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__CloneInCloneable__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#noCovariantEquals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Covariant Equals</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Covariant Equals</em>' operation.
	 * @see javaMM.ClassDeclaration#noCovariantEquals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__NoCovariantEquals__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#noObscuredVariables(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Obscured Variables</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Obscured Variables</em>' operation.
	 * @see javaMM.ClassDeclaration#noObscuredVariables(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__NoObscuredVariables__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.ClassDeclaration#comparatorImplementsSerializable(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Comparator Implements Serializable</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Comparator Implements Serializable</em>' operation.
	 * @see javaMM.ClassDeclaration#comparatorImplementsSerializable(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getClassDeclaration__ComparatorImplementsSerializable__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.Comment <em>Comment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Comment</em>'.
	 * @see javaMM.Comment
	 * @generated
	 */
	EClass getComment();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Comment#getContent <em>Content</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Content</em>'.
	 * @see javaMM.Comment#getContent()
	 * @see #getComment()
	 * @generated
	 */
	EAttribute getComment_Content();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Comment#getEnclosedByParent <em>Enclosed By Parent</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Enclosed By Parent</em>'.
	 * @see javaMM.Comment#getEnclosedByParent()
	 * @see #getComment()
	 * @generated
	 */
	EAttribute getComment_EnclosedByParent();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Comment#getPrefixOfParent <em>Prefix Of Parent</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Prefix Of Parent</em>'.
	 * @see javaMM.Comment#getPrefixOfParent()
	 * @see #getComment()
	 * @generated
	 */
	EAttribute getComment_PrefixOfParent();

	/**
	 * Returns the meta object for class '{@link javaMM.CompilationUnit <em>Compilation Unit</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Compilation Unit</em>'.
	 * @see javaMM.CompilationUnit
	 * @generated
	 */
	EClass getCompilationUnit();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.CompilationUnit#getOriginalFilePath <em>Original File Path</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Original File Path</em>'.
	 * @see javaMM.CompilationUnit#getOriginalFilePath()
	 * @see #getCompilationUnit()
	 * @generated
	 */
	EAttribute getCompilationUnit_OriginalFilePath();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.CompilationUnit#getCommentList <em>Comment List</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Comment List</em>'.
	 * @see javaMM.CompilationUnit#getCommentList()
	 * @see #getCompilationUnit()
	 * @generated
	 */
	EReference getCompilationUnit_CommentList();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.CompilationUnit#getImports <em>Imports</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Imports</em>'.
	 * @see javaMM.CompilationUnit#getImports()
	 * @see #getCompilationUnit()
	 * @generated
	 */
	EReference getCompilationUnit_Imports();

	/**
	 * Returns the meta object for the reference '{@link javaMM.CompilationUnit#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Package</em>'.
	 * @see javaMM.CompilationUnit#getPackage()
	 * @see #getCompilationUnit()
	 * @generated
	 */
	EReference getCompilationUnit_Package();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.CompilationUnit#getTypes <em>Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Types</em>'.
	 * @see javaMM.CompilationUnit#getTypes()
	 * @see #getCompilationUnit()
	 * @generated
	 */
	EReference getCompilationUnit_Types();

	/**
	 * Returns the meta object for class '{@link javaMM.ContinueStatement <em>Continue Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Continue Statement</em>'.
	 * @see javaMM.ContinueStatement
	 * @generated
	 */
	EClass getContinueStatement();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ContinueStatement#getLabel <em>Label</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Label</em>'.
	 * @see javaMM.ContinueStatement#getLabel()
	 * @see #getContinueStatement()
	 * @generated
	 */
	EReference getContinueStatement_Label();

	/**
	 * Returns the meta object for class '{@link javaMM.DoStatement <em>Do Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Do Statement</em>'.
	 * @see javaMM.DoStatement
	 * @generated
	 */
	EClass getDoStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.DoStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.DoStatement#getExpression()
	 * @see #getDoStatement()
	 * @generated
	 */
	EReference getDoStatement_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.DoStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.DoStatement#getBody()
	 * @see #getDoStatement()
	 * @generated
	 */
	EReference getDoStatement_Body();

	/**
	 * Returns the meta object for class '{@link javaMM.EmptyStatement <em>Empty Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Empty Statement</em>'.
	 * @see javaMM.EmptyStatement
	 * @generated
	 */
	EClass getEmptyStatement();

	/**
	 * Returns the meta object for class '{@link javaMM.EnhancedForStatement <em>Enhanced For Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enhanced For Statement</em>'.
	 * @see javaMM.EnhancedForStatement
	 * @generated
	 */
	EClass getEnhancedForStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.EnhancedForStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.EnhancedForStatement#getBody()
	 * @see #getEnhancedForStatement()
	 * @generated
	 */
	EReference getEnhancedForStatement_Body();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.EnhancedForStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.EnhancedForStatement#getExpression()
	 * @see #getEnhancedForStatement()
	 * @generated
	 */
	EReference getEnhancedForStatement_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.EnhancedForStatement#getParameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Parameter</em>'.
	 * @see javaMM.EnhancedForStatement#getParameter()
	 * @see #getEnhancedForStatement()
	 * @generated
	 */
	EReference getEnhancedForStatement_Parameter();

	/**
	 * Returns the meta object for class '{@link javaMM.EnumConstantDeclaration <em>Enum Constant Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enum Constant Declaration</em>'.
	 * @see javaMM.EnumConstantDeclaration
	 * @generated
	 */
	EClass getEnumConstantDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.EnumConstantDeclaration#getAnonymousClassDeclaration <em>Anonymous Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Anonymous Class Declaration</em>'.
	 * @see javaMM.EnumConstantDeclaration#getAnonymousClassDeclaration()
	 * @see #getEnumConstantDeclaration()
	 * @generated
	 */
	EReference getEnumConstantDeclaration_AnonymousClassDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.EnumConstantDeclaration#getArguments <em>Arguments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Arguments</em>'.
	 * @see javaMM.EnumConstantDeclaration#getArguments()
	 * @see #getEnumConstantDeclaration()
	 * @generated
	 */
	EReference getEnumConstantDeclaration_Arguments();

	/**
	 * Returns the meta object for class '{@link javaMM.EnumDeclaration <em>Enum Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enum Declaration</em>'.
	 * @see javaMM.EnumDeclaration
	 * @generated
	 */
	EClass getEnumDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.EnumDeclaration#getEnumConstants <em>Enum Constants</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Enum Constants</em>'.
	 * @see javaMM.EnumDeclaration#getEnumConstants()
	 * @see #getEnumDeclaration()
	 * @generated
	 */
	EReference getEnumDeclaration_EnumConstants();

	/**
	 * Returns the meta object for class '{@link javaMM.Expression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Expression</em>'.
	 * @see javaMM.Expression
	 * @generated
	 */
	EClass getExpression();

	/**
	 * Returns the meta object for class '{@link javaMM.ExpressionStatement <em>Expression Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Expression Statement</em>'.
	 * @see javaMM.ExpressionStatement
	 * @generated
	 */
	EClass getExpressionStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ExpressionStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ExpressionStatement#getExpression()
	 * @see #getExpressionStatement()
	 * @generated
	 */
	EReference getExpressionStatement_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.FieldAccess <em>Field Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Field Access</em>'.
	 * @see javaMM.FieldAccess
	 * @generated
	 */
	EClass getFieldAccess();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.FieldAccess#getField <em>Field</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Field</em>'.
	 * @see javaMM.FieldAccess#getField()
	 * @see #getFieldAccess()
	 * @generated
	 */
	EReference getFieldAccess_Field();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.FieldAccess#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.FieldAccess#getExpression()
	 * @see #getFieldAccess()
	 * @generated
	 */
	EReference getFieldAccess_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.FieldDeclaration <em>Field Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Field Declaration</em>'.
	 * @see javaMM.FieldDeclaration
	 * @generated
	 */
	EClass getFieldDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.ForStatement <em>For Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>For Statement</em>'.
	 * @see javaMM.ForStatement
	 * @generated
	 */
	EClass getForStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ForStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ForStatement#getExpression()
	 * @see #getForStatement()
	 * @generated
	 */
	EReference getForStatement_Expression();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ForStatement#getUpdaters <em>Updaters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Updaters</em>'.
	 * @see javaMM.ForStatement#getUpdaters()
	 * @see #getForStatement()
	 * @generated
	 */
	EReference getForStatement_Updaters();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ForStatement#getInitializers <em>Initializers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Initializers</em>'.
	 * @see javaMM.ForStatement#getInitializers()
	 * @see #getForStatement()
	 * @generated
	 */
	EReference getForStatement_Initializers();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ForStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.ForStatement#getBody()
	 * @see #getForStatement()
	 * @generated
	 */
	EReference getForStatement_Body();

	/**
	 * Returns the meta object for class '{@link javaMM.IfStatement <em>If Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>If Statement</em>'.
	 * @see javaMM.IfStatement
	 * @generated
	 */
	EClass getIfStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.IfStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.IfStatement#getExpression()
	 * @see #getIfStatement()
	 * @generated
	 */
	EReference getIfStatement_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.IfStatement#getThenStatement <em>Then Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Then Statement</em>'.
	 * @see javaMM.IfStatement#getThenStatement()
	 * @see #getIfStatement()
	 * @generated
	 */
	EReference getIfStatement_ThenStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.IfStatement#getElseStatement <em>Else Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Else Statement</em>'.
	 * @see javaMM.IfStatement#getElseStatement()
	 * @see #getIfStatement()
	 * @generated
	 */
	EReference getIfStatement_ElseStatement();

	/**
	 * Returns the meta object for the '{@link javaMM.IfStatement#noDeadCode(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Dead Code</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Dead Code</em>' operation.
	 * @see javaMM.IfStatement#noDeadCode(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getIfStatement__NoDeadCode__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.IfStatement#noUselessControlFlow(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Useless Control Flow</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Useless Control Flow</em>' operation.
	 * @see javaMM.IfStatement#noUselessControlFlow(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getIfStatement__NoUselessControlFlow__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.ImportDeclaration <em>Import Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Import Declaration</em>'.
	 * @see javaMM.ImportDeclaration
	 * @generated
	 */
	EClass getImportDeclaration();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ImportDeclaration#getStatic <em>Static</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Static</em>'.
	 * @see javaMM.ImportDeclaration#getStatic()
	 * @see #getImportDeclaration()
	 * @generated
	 */
	EAttribute getImportDeclaration_Static();

	/**
	 * Returns the meta object for the reference '{@link javaMM.ImportDeclaration#getImportedElement <em>Imported Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Imported Element</em>'.
	 * @see javaMM.ImportDeclaration#getImportedElement()
	 * @see #getImportDeclaration()
	 * @generated
	 */
	EReference getImportDeclaration_ImportedElement();

	/**
	 * Returns the meta object for class '{@link javaMM.InfixExpression <em>Infix Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Infix Expression</em>'.
	 * @see javaMM.InfixExpression
	 * @generated
	 */
	EClass getInfixExpression();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.InfixExpression#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see javaMM.InfixExpression#getOperator()
	 * @see #getInfixExpression()
	 * @generated
	 */
	EAttribute getInfixExpression_Operator();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.InfixExpression#getRightOperand <em>Right Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Right Operand</em>'.
	 * @see javaMM.InfixExpression#getRightOperand()
	 * @see #getInfixExpression()
	 * @generated
	 */
	EReference getInfixExpression_RightOperand();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.InfixExpression#getLeftOperand <em>Left Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Left Operand</em>'.
	 * @see javaMM.InfixExpression#getLeftOperand()
	 * @see #getInfixExpression()
	 * @generated
	 */
	EReference getInfixExpression_LeftOperand();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.InfixExpression#getExtendedOperands <em>Extended Operands</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Extended Operands</em>'.
	 * @see javaMM.InfixExpression#getExtendedOperands()
	 * @see #getInfixExpression()
	 * @generated
	 */
	EReference getInfixExpression_ExtendedOperands();

	/**
	 * Returns the meta object for the '{@link javaMM.InfixExpression#operatorIsEquality() <em>Operator Is Equality</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Operator Is Equality</em>' operation.
	 * @see javaMM.InfixExpression#operatorIsEquality()
	 * @generated
	 */
	EOperation getInfixExpression__OperatorIsEquality();

	/**
	 * Returns the meta object for the '{@link javaMM.InfixExpression#equalsNotOnLiterals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Equals Not On Literals</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Equals Not On Literals</em>' operation.
	 * @see javaMM.InfixExpression#equalsNotOnLiterals(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getInfixExpression__EqualsNotOnLiterals__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.InfixExpression#noRedundantComparison(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Redundant Comparison</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Redundant Comparison</em>' operation.
	 * @see javaMM.InfixExpression#noRedundantComparison(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getInfixExpression__NoRedundantComparison__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.InfixExpression#equalsNotOnStrings(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Equals Not On Strings</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Equals Not On Strings</em>' operation.
	 * @see javaMM.InfixExpression#equalsNotOnStrings(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getInfixExpression__EqualsNotOnStrings__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.Initializer <em>Initializer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Initializer</em>'.
	 * @see javaMM.Initializer
	 * @generated
	 */
	EClass getInitializer();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.Initializer#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.Initializer#getBody()
	 * @see #getInitializer()
	 * @generated
	 */
	EReference getInitializer_Body();

	/**
	 * Returns the meta object for class '{@link javaMM.InstanceofExpression <em>Instanceof Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Instanceof Expression</em>'.
	 * @see javaMM.InstanceofExpression
	 * @generated
	 */
	EClass getInstanceofExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.InstanceofExpression#getRightOperand <em>Right Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Right Operand</em>'.
	 * @see javaMM.InstanceofExpression#getRightOperand()
	 * @see #getInstanceofExpression()
	 * @generated
	 */
	EReference getInstanceofExpression_RightOperand();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.InstanceofExpression#getLeftOperand <em>Left Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Left Operand</em>'.
	 * @see javaMM.InstanceofExpression#getLeftOperand()
	 * @see #getInstanceofExpression()
	 * @generated
	 */
	EReference getInstanceofExpression_LeftOperand();

	/**
	 * Returns the meta object for class '{@link javaMM.InterfaceDeclaration <em>Interface Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Interface Declaration</em>'.
	 * @see javaMM.InterfaceDeclaration
	 * @generated
	 */
	EClass getInterfaceDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.Javadoc <em>Javadoc</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Javadoc</em>'.
	 * @see javaMM.Javadoc
	 * @generated
	 */
	EClass getJavadoc();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Javadoc#getTags <em>Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tags</em>'.
	 * @see javaMM.Javadoc#getTags()
	 * @see #getJavadoc()
	 * @generated
	 */
	EReference getJavadoc_Tags();

	/**
	 * Returns the meta object for class '{@link javaMM.LabeledStatement <em>Labeled Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Labeled Statement</em>'.
	 * @see javaMM.LabeledStatement
	 * @generated
	 */
	EClass getLabeledStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.LabeledStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.LabeledStatement#getBody()
	 * @see #getLabeledStatement()
	 * @generated
	 */
	EReference getLabeledStatement_Body();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.LabeledStatement#getUsagesInBreakStatements <em>Usages In Break Statements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Break Statements</em>'.
	 * @see javaMM.LabeledStatement#getUsagesInBreakStatements()
	 * @see #getLabeledStatement()
	 * @generated
	 */
	EReference getLabeledStatement_UsagesInBreakStatements();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.LabeledStatement#getUsagesInContinueStatements <em>Usages In Continue Statements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Continue Statements</em>'.
	 * @see javaMM.LabeledStatement#getUsagesInContinueStatements()
	 * @see #getLabeledStatement()
	 * @generated
	 */
	EReference getLabeledStatement_UsagesInContinueStatements();

	/**
	 * Returns the meta object for class '{@link javaMM.LineComment <em>Line Comment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Line Comment</em>'.
	 * @see javaMM.LineComment
	 * @generated
	 */
	EClass getLineComment();

	/**
	 * Returns the meta object for class '{@link javaMM.Manifest <em>Manifest</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Manifest</em>'.
	 * @see javaMM.Manifest
	 * @generated
	 */
	EClass getManifest();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Manifest#getMainAttributes <em>Main Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Main Attributes</em>'.
	 * @see javaMM.Manifest#getMainAttributes()
	 * @see #getManifest()
	 * @generated
	 */
	EReference getManifest_MainAttributes();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Manifest#getEntryAttributes <em>Entry Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entry Attributes</em>'.
	 * @see javaMM.Manifest#getEntryAttributes()
	 * @see #getManifest()
	 * @generated
	 */
	EReference getManifest_EntryAttributes();

	/**
	 * Returns the meta object for class '{@link javaMM.ManifestAttribute <em>Manifest Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Manifest Attribute</em>'.
	 * @see javaMM.ManifestAttribute
	 * @generated
	 */
	EClass getManifestAttribute();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ManifestAttribute#getKey <em>Key</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Key</em>'.
	 * @see javaMM.ManifestAttribute#getKey()
	 * @see #getManifestAttribute()
	 * @generated
	 */
	EAttribute getManifestAttribute_Key();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ManifestAttribute#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see javaMM.ManifestAttribute#getValue()
	 * @see #getManifestAttribute()
	 * @generated
	 */
	EAttribute getManifestAttribute_Value();

	/**
	 * Returns the meta object for class '{@link javaMM.ManifestEntry <em>Manifest Entry</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Manifest Entry</em>'.
	 * @see javaMM.ManifestEntry
	 * @generated
	 */
	EClass getManifestEntry();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.ManifestEntry#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see javaMM.ManifestEntry#getName()
	 * @see #getManifestEntry()
	 * @generated
	 */
	EAttribute getManifestEntry_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ManifestEntry#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see javaMM.ManifestEntry#getAttributes()
	 * @see #getManifestEntry()
	 * @generated
	 */
	EReference getManifestEntry_Attributes();

	/**
	 * Returns the meta object for class '{@link javaMM.MemberRef <em>Member Ref</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Ref</em>'.
	 * @see javaMM.MemberRef
	 * @generated
	 */
	EClass getMemberRef();

	/**
	 * Returns the meta object for the reference '{@link javaMM.MemberRef#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member</em>'.
	 * @see javaMM.MemberRef#getMember()
	 * @see #getMemberRef()
	 * @generated
	 */
	EReference getMemberRef_Member();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.MemberRef#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.MemberRef#getQualifier()
	 * @see #getMemberRef()
	 * @generated
	 */
	EReference getMemberRef_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.MethodDeclaration <em>Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Method Declaration</em>'.
	 * @see javaMM.MethodDeclaration
	 * @generated
	 */
	EClass getMethodDeclaration();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.MethodDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Extra Array Dimensions</em>'.
	 * @see javaMM.MethodDeclaration#getExtraArrayDimensions()
	 * @see #getMethodDeclaration()
	 * @generated
	 */
	EAttribute getMethodDeclaration_ExtraArrayDimensions();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.MethodDeclaration#getReturnType <em>Return Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Return Type</em>'.
	 * @see javaMM.MethodDeclaration#getReturnType()
	 * @see #getMethodDeclaration()
	 * @generated
	 */
	EReference getMethodDeclaration_ReturnType();

	/**
	 * Returns the meta object for the reference '{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Redefined Method Declaration</em>'.
	 * @see javaMM.MethodDeclaration#getRedefinedMethodDeclaration()
	 * @see #getMethodDeclaration()
	 * @generated
	 */
	EReference getMethodDeclaration_RedefinedMethodDeclaration();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.MethodDeclaration#getRedefinitions <em>Redefinitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Redefinitions</em>'.
	 * @see javaMM.MethodDeclaration#getRedefinitions()
	 * @see #getMethodDeclaration()
	 * @generated
	 */
	EReference getMethodDeclaration_Redefinitions();

	/**
	 * Returns the meta object for the '{@link javaMM.MethodDeclaration#shouldStartWithLowerCase(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Should Start With Lower Case</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Should Start With Lower Case</em>' operation.
	 * @see javaMM.MethodDeclaration#shouldStartWithLowerCase(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getMethodDeclaration__ShouldStartWithLowerCase__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.MethodInvocation <em>Method Invocation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Method Invocation</em>'.
	 * @see javaMM.MethodInvocation
	 * @generated
	 */
	EClass getMethodInvocation();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.MethodInvocation#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.MethodInvocation#getExpression()
	 * @see #getMethodInvocation()
	 * @generated
	 */
	EReference getMethodInvocation_Expression();

	/**
	 * Returns the meta object for the '{@link javaMM.MethodInvocation#doesNotCallRunFinalizers(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Does Not Call Run Finalizers</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Does Not Call Run Finalizers</em>' operation.
	 * @see javaMM.MethodInvocation#doesNotCallRunFinalizers(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getMethodInvocation__DoesNotCallRunFinalizers__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.MethodInvocation#doesNotCallFinalize(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Does Not Call Finalize</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Does Not Call Finalize</em>' operation.
	 * @see javaMM.MethodInvocation#doesNotCallFinalize(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getMethodInvocation__DoesNotCallFinalize__DiagnosticChain_Map();

	/**
	 * Returns the meta object for the '{@link javaMM.MethodInvocation#doesNotCallExit(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Does Not Call Exit</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Does Not Call Exit</em>' operation.
	 * @see javaMM.MethodInvocation#doesNotCallExit(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getMethodInvocation__DoesNotCallExit__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.MethodRef <em>Method Ref</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Method Ref</em>'.
	 * @see javaMM.MethodRef
	 * @generated
	 */
	EClass getMethodRef();

	/**
	 * Returns the meta object for the reference '{@link javaMM.MethodRef#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Method</em>'.
	 * @see javaMM.MethodRef#getMethod()
	 * @see #getMethodRef()
	 * @generated
	 */
	EReference getMethodRef_Method();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.MethodRef#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.MethodRef#getQualifier()
	 * @see #getMethodRef()
	 * @generated
	 */
	EReference getMethodRef_Qualifier();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.MethodRef#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameters</em>'.
	 * @see javaMM.MethodRef#getParameters()
	 * @see #getMethodRef()
	 * @generated
	 */
	EReference getMethodRef_Parameters();

	/**
	 * Returns the meta object for class '{@link javaMM.MethodRefParameter <em>Method Ref Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Method Ref Parameter</em>'.
	 * @see javaMM.MethodRefParameter
	 * @generated
	 */
	EClass getMethodRefParameter();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.MethodRefParameter#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see javaMM.MethodRefParameter#getName()
	 * @see #getMethodRefParameter()
	 * @generated
	 */
	EAttribute getMethodRefParameter_Name();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.MethodRefParameter#getVarargs <em>Varargs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Varargs</em>'.
	 * @see javaMM.MethodRefParameter#getVarargs()
	 * @see #getMethodRefParameter()
	 * @generated
	 */
	EAttribute getMethodRefParameter_Varargs();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.MethodRefParameter#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.MethodRefParameter#getType()
	 * @see #getMethodRefParameter()
	 * @generated
	 */
	EReference getMethodRefParameter_Type();

	/**
	 * Returns the meta object for class '{@link javaMM.Model <em>Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Model</em>'.
	 * @see javaMM.Model
	 * @generated
	 */
	EClass getModel();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Model#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see javaMM.Model#getName()
	 * @see #getModel()
	 * @generated
	 */
	EAttribute getModel_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getOwnedElements <em>Owned Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Owned Elements</em>'.
	 * @see javaMM.Model#getOwnedElements()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_OwnedElements();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getOrphanTypes <em>Orphan Types</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Orphan Types</em>'.
	 * @see javaMM.Model#getOrphanTypes()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_OrphanTypes();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getUnresolvedItems <em>Unresolved Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Unresolved Items</em>'.
	 * @see javaMM.Model#getUnresolvedItems()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_UnresolvedItems();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getCompilationUnits <em>Compilation Units</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Compilation Units</em>'.
	 * @see javaMM.Model#getCompilationUnits()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_CompilationUnits();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getClassFiles <em>Class Files</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Class Files</em>'.
	 * @see javaMM.Model#getClassFiles()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_ClassFiles();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Model#getArchives <em>Archives</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Archives</em>'.
	 * @see javaMM.Model#getArchives()
	 * @see #getModel()
	 * @generated
	 */
	EReference getModel_Archives();

	/**
	 * Returns the meta object for class '{@link javaMM.Modifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Modifier</em>'.
	 * @see javaMM.Modifier
	 * @generated
	 */
	EClass getModifier();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getVisibility <em>Visibility</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Visibility</em>'.
	 * @see javaMM.Modifier#getVisibility()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Visibility();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getInheritance <em>Inheritance</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Inheritance</em>'.
	 * @see javaMM.Modifier#getInheritance()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Inheritance();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getStatic <em>Static</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Static</em>'.
	 * @see javaMM.Modifier#getStatic()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Static();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getTransient <em>Transient</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Transient</em>'.
	 * @see javaMM.Modifier#getTransient()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Transient();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getVolatile <em>Volatile</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Volatile</em>'.
	 * @see javaMM.Modifier#getVolatile()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Volatile();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getNative <em>Native</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Native</em>'.
	 * @see javaMM.Modifier#getNative()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Native();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getStrictfp <em>Strictfp</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Strictfp</em>'.
	 * @see javaMM.Modifier#getStrictfp()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Strictfp();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.Modifier#getSynchronized <em>Synchronized</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Synchronized</em>'.
	 * @see javaMM.Modifier#getSynchronized()
	 * @see #getModifier()
	 * @generated
	 */
	EAttribute getModifier_Synchronized();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Modifier#getBodyDeclaration <em>Body Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Body Declaration</em>'.
	 * @see javaMM.Modifier#getBodyDeclaration()
	 * @see #getModifier()
	 * @generated
	 */
	EReference getModifier_BodyDeclaration();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Modifier#getSingleVariableDeclaration <em>Single Variable Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Single Variable Declaration</em>'.
	 * @see javaMM.Modifier#getSingleVariableDeclaration()
	 * @see #getModifier()
	 * @generated
	 */
	EReference getModifier_SingleVariableDeclaration();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Modifier#getVariableDeclarationStatement <em>Variable Declaration Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Variable Declaration Statement</em>'.
	 * @see javaMM.Modifier#getVariableDeclarationStatement()
	 * @see #getModifier()
	 * @generated
	 */
	EReference getModifier_VariableDeclarationStatement();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Modifier#getVariableDeclarationExpression <em>Variable Declaration Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Variable Declaration Expression</em>'.
	 * @see javaMM.Modifier#getVariableDeclarationExpression()
	 * @see #getModifier()
	 * @generated
	 */
	EReference getModifier_VariableDeclarationExpression();

	/**
	 * Returns the meta object for the '{@link javaMM.Modifier#isLocal() <em>Is Local</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Is Local</em>' operation.
	 * @see javaMM.Modifier#isLocal()
	 * @generated
	 */
	EOperation getModifier__IsLocal();

	/**
	 * Returns the meta object for class '{@link javaMM.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Named Element</em>'.
	 * @see javaMM.NamedElement
	 * @generated
	 */
	EClass getNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.NamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see javaMM.NamedElement#getName()
	 * @see #getNamedElement()
	 * @generated
	 */
	EAttribute getNamedElement_Name();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.NamedElement#getProxy <em>Proxy</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Proxy</em>'.
	 * @see javaMM.NamedElement#getProxy()
	 * @see #getNamedElement()
	 * @generated
	 */
	EAttribute getNamedElement_Proxy();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.NamedElement#getUsagesInImports <em>Usages In Imports</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Imports</em>'.
	 * @see javaMM.NamedElement#getUsagesInImports()
	 * @see #getNamedElement()
	 * @generated
	 */
	EReference getNamedElement_UsagesInImports();

	/**
	 * Returns the meta object for class '{@link javaMM.NamespaceAccess <em>Namespace Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Namespace Access</em>'.
	 * @see javaMM.NamespaceAccess
	 * @generated
	 */
	EClass getNamespaceAccess();

	/**
	 * Returns the meta object for class '{@link javaMM.NumberLiteral <em>Number Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Number Literal</em>'.
	 * @see javaMM.NumberLiteral
	 * @generated
	 */
	EClass getNumberLiteral();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.NumberLiteral#getTokenValue <em>Token Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Token Value</em>'.
	 * @see javaMM.NumberLiteral#getTokenValue()
	 * @see #getNumberLiteral()
	 * @generated
	 */
	EAttribute getNumberLiteral_TokenValue();

	/**
	 * Returns the meta object for class '{@link javaMM.NullLiteral <em>Null Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Null Literal</em>'.
	 * @see javaMM.NullLiteral
	 * @generated
	 */
	EClass getNullLiteral();

	/**
	 * Returns the meta object for class '{@link javaMM.Package <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Package</em>'.
	 * @see javaMM.Package
	 * @generated
	 */
	EClass getPackage();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Package#getOwnedElements <em>Owned Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Owned Elements</em>'.
	 * @see javaMM.Package#getOwnedElements()
	 * @see #getPackage()
	 * @generated
	 */
	EReference getPackage_OwnedElements();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Package#getModel <em>Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Model</em>'.
	 * @see javaMM.Package#getModel()
	 * @see #getPackage()
	 * @generated
	 */
	EReference getPackage_Model();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.Package#getOwnedPackages <em>Owned Packages</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Owned Packages</em>'.
	 * @see javaMM.Package#getOwnedPackages()
	 * @see #getPackage()
	 * @generated
	 */
	EReference getPackage_OwnedPackages();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.Package#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Package</em>'.
	 * @see javaMM.Package#getPackage()
	 * @see #getPackage()
	 * @generated
	 */
	EReference getPackage_Package();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.Package#getUsagesInPackageAccess <em>Usages In Package Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Package Access</em>'.
	 * @see javaMM.Package#getUsagesInPackageAccess()
	 * @see #getPackage()
	 * @generated
	 */
	EReference getPackage_UsagesInPackageAccess();

	/**
	 * Returns the meta object for class '{@link javaMM.PackageAccess <em>Package Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Package Access</em>'.
	 * @see javaMM.PackageAccess
	 * @generated
	 */
	EClass getPackageAccess();

	/**
	 * Returns the meta object for the reference '{@link javaMM.PackageAccess#getPackage <em>Package</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Package</em>'.
	 * @see javaMM.PackageAccess#getPackage()
	 * @see #getPackageAccess()
	 * @generated
	 */
	EReference getPackageAccess_Package();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.PackageAccess#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.PackageAccess#getQualifier()
	 * @see #getPackageAccess()
	 * @generated
	 */
	EReference getPackageAccess_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.ParameterizedType <em>Parameterized Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parameterized Type</em>'.
	 * @see javaMM.ParameterizedType
	 * @generated
	 */
	EClass getParameterizedType();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ParameterizedType#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.ParameterizedType#getType()
	 * @see #getParameterizedType()
	 * @generated
	 */
	EReference getParameterizedType_Type();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.ParameterizedType#getTypeArguments <em>Type Arguments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Type Arguments</em>'.
	 * @see javaMM.ParameterizedType#getTypeArguments()
	 * @see #getParameterizedType()
	 * @generated
	 */
	EReference getParameterizedType_TypeArguments();

	/**
	 * Returns the meta object for class '{@link javaMM.ParenthesizedExpression <em>Parenthesized Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parenthesized Expression</em>'.
	 * @see javaMM.ParenthesizedExpression
	 * @generated
	 */
	EClass getParenthesizedExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ParenthesizedExpression#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ParenthesizedExpression#getExpression()
	 * @see #getParenthesizedExpression()
	 * @generated
	 */
	EReference getParenthesizedExpression_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.PostfixExpression <em>Postfix Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Postfix Expression</em>'.
	 * @see javaMM.PostfixExpression
	 * @generated
	 */
	EClass getPostfixExpression();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.PostfixExpression#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see javaMM.PostfixExpression#getOperator()
	 * @see #getPostfixExpression()
	 * @generated
	 */
	EAttribute getPostfixExpression_Operator();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.PostfixExpression#getOperand <em>Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Operand</em>'.
	 * @see javaMM.PostfixExpression#getOperand()
	 * @see #getPostfixExpression()
	 * @generated
	 */
	EReference getPostfixExpression_Operand();

	/**
	 * Returns the meta object for class '{@link javaMM.PrefixExpression <em>Prefix Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Prefix Expression</em>'.
	 * @see javaMM.PrefixExpression
	 * @generated
	 */
	EClass getPrefixExpression();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.PrefixExpression#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see javaMM.PrefixExpression#getOperator()
	 * @see #getPrefixExpression()
	 * @generated
	 */
	EAttribute getPrefixExpression_Operator();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.PrefixExpression#getOperand <em>Operand</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Operand</em>'.
	 * @see javaMM.PrefixExpression#getOperand()
	 * @see #getPrefixExpression()
	 * @generated
	 */
	EReference getPrefixExpression_Operand();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveType <em>Primitive Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type</em>'.
	 * @see javaMM.PrimitiveType
	 * @generated
	 */
	EClass getPrimitiveType();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeBoolean <em>Primitive Type Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Boolean</em>'.
	 * @see javaMM.PrimitiveTypeBoolean
	 * @generated
	 */
	EClass getPrimitiveTypeBoolean();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeByte <em>Primitive Type Byte</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Byte</em>'.
	 * @see javaMM.PrimitiveTypeByte
	 * @generated
	 */
	EClass getPrimitiveTypeByte();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeChar <em>Primitive Type Char</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Char</em>'.
	 * @see javaMM.PrimitiveTypeChar
	 * @generated
	 */
	EClass getPrimitiveTypeChar();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeDouble <em>Primitive Type Double</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Double</em>'.
	 * @see javaMM.PrimitiveTypeDouble
	 * @generated
	 */
	EClass getPrimitiveTypeDouble();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeShort <em>Primitive Type Short</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Short</em>'.
	 * @see javaMM.PrimitiveTypeShort
	 * @generated
	 */
	EClass getPrimitiveTypeShort();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeFloat <em>Primitive Type Float</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Float</em>'.
	 * @see javaMM.PrimitiveTypeFloat
	 * @generated
	 */
	EClass getPrimitiveTypeFloat();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeInt <em>Primitive Type Int</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Int</em>'.
	 * @see javaMM.PrimitiveTypeInt
	 * @generated
	 */
	EClass getPrimitiveTypeInt();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeLong <em>Primitive Type Long</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Long</em>'.
	 * @see javaMM.PrimitiveTypeLong
	 * @generated
	 */
	EClass getPrimitiveTypeLong();

	/**
	 * Returns the meta object for class '{@link javaMM.PrimitiveTypeVoid <em>Primitive Type Void</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Primitive Type Void</em>'.
	 * @see javaMM.PrimitiveTypeVoid
	 * @generated
	 */
	EClass getPrimitiveTypeVoid();

	/**
	 * Returns the meta object for class '{@link javaMM.ReturnStatement <em>Return Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Return Statement</em>'.
	 * @see javaMM.ReturnStatement
	 * @generated
	 */
	EClass getReturnStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ReturnStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ReturnStatement#getExpression()
	 * @see #getReturnStatement()
	 * @generated
	 */
	EReference getReturnStatement_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.SingleVariableAccess <em>Single Variable Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Single Variable Access</em>'.
	 * @see javaMM.SingleVariableAccess
	 * @generated
	 */
	EClass getSingleVariableAccess();

	/**
	 * Returns the meta object for the reference '{@link javaMM.SingleVariableAccess#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see javaMM.SingleVariableAccess#getVariable()
	 * @see #getSingleVariableAccess()
	 * @generated
	 */
	EReference getSingleVariableAccess_Variable();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SingleVariableAccess#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.SingleVariableAccess#getQualifier()
	 * @see #getSingleVariableAccess()
	 * @generated
	 */
	EReference getSingleVariableAccess_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.SingleVariableDeclaration <em>Single Variable Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Single Variable Declaration</em>'.
	 * @see javaMM.SingleVariableDeclaration
	 * @generated
	 */
	EClass getSingleVariableDeclaration();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SingleVariableDeclaration#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Modifier</em>'.
	 * @see javaMM.SingleVariableDeclaration#getModifier()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_Modifier();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.SingleVariableDeclaration#getVarargs <em>Varargs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Varargs</em>'.
	 * @see javaMM.SingleVariableDeclaration#getVarargs()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EAttribute getSingleVariableDeclaration_Varargs();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SingleVariableDeclaration#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.SingleVariableDeclaration#getType()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_Type();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.SingleVariableDeclaration#getAnnotations <em>Annotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Annotations</em>'.
	 * @see javaMM.SingleVariableDeclaration#getAnnotations()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_Annotations();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.SingleVariableDeclaration#getMethodDeclaration <em>Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Method Declaration</em>'.
	 * @see javaMM.SingleVariableDeclaration#getMethodDeclaration()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_MethodDeclaration();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.SingleVariableDeclaration#getCatchClause <em>Catch Clause</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Catch Clause</em>'.
	 * @see javaMM.SingleVariableDeclaration#getCatchClause()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_CatchClause();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.SingleVariableDeclaration#getEnhancedForStatement <em>Enhanced For Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Enhanced For Statement</em>'.
	 * @see javaMM.SingleVariableDeclaration#getEnhancedForStatement()
	 * @see #getSingleVariableDeclaration()
	 * @generated
	 */
	EReference getSingleVariableDeclaration_EnhancedForStatement();

	/**
	 * Returns the meta object for class '{@link javaMM.Statement <em>Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Statement</em>'.
	 * @see javaMM.Statement
	 * @generated
	 */
	EClass getStatement();

	/**
	 * Returns the meta object for class '{@link javaMM.StringLiteral <em>String Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>String Literal</em>'.
	 * @see javaMM.StringLiteral
	 * @generated
	 */
	EClass getStringLiteral();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.StringLiteral#getEscapedValue <em>Escaped Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Escaped Value</em>'.
	 * @see javaMM.StringLiteral#getEscapedValue()
	 * @see #getStringLiteral()
	 * @generated
	 */
	EAttribute getStringLiteral_EscapedValue();

	/**
	 * Returns the meta object for class '{@link javaMM.SuperConstructorInvocation <em>Super Constructor Invocation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Super Constructor Invocation</em>'.
	 * @see javaMM.SuperConstructorInvocation
	 * @generated
	 */
	EClass getSuperConstructorInvocation();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SuperConstructorInvocation#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.SuperConstructorInvocation#getExpression()
	 * @see #getSuperConstructorInvocation()
	 * @generated
	 */
	EReference getSuperConstructorInvocation_Expression();

	/**
	 * Returns the meta object for the '{@link javaMM.SuperConstructorInvocation#noRedundantSuperCall(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>No Redundant Super Call</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>No Redundant Super Call</em>' operation.
	 * @see javaMM.SuperConstructorInvocation#noRedundantSuperCall(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getSuperConstructorInvocation__NoRedundantSuperCall__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.SuperFieldAccess <em>Super Field Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Super Field Access</em>'.
	 * @see javaMM.SuperFieldAccess
	 * @generated
	 */
	EClass getSuperFieldAccess();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SuperFieldAccess#getField <em>Field</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Field</em>'.
	 * @see javaMM.SuperFieldAccess#getField()
	 * @see #getSuperFieldAccess()
	 * @generated
	 */
	EReference getSuperFieldAccess_Field();

	/**
	 * Returns the meta object for class '{@link javaMM.SuperMethodInvocation <em>Super Method Invocation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Super Method Invocation</em>'.
	 * @see javaMM.SuperMethodInvocation
	 * @generated
	 */
	EClass getSuperMethodInvocation();

	/**
	 * Returns the meta object for class '{@link javaMM.SwitchCase <em>Switch Case</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Switch Case</em>'.
	 * @see javaMM.SwitchCase
	 * @generated
	 */
	EClass getSwitchCase();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.SwitchCase#getDefault <em>Default</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Default</em>'.
	 * @see javaMM.SwitchCase#getDefault()
	 * @see #getSwitchCase()
	 * @generated
	 */
	EAttribute getSwitchCase_Default();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SwitchCase#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.SwitchCase#getExpression()
	 * @see #getSwitchCase()
	 * @generated
	 */
	EReference getSwitchCase_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.SwitchStatement <em>Switch Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Switch Statement</em>'.
	 * @see javaMM.SwitchStatement
	 * @generated
	 */
	EClass getSwitchStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SwitchStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.SwitchStatement#getExpression()
	 * @see #getSwitchStatement()
	 * @generated
	 */
	EReference getSwitchStatement_Expression();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.SwitchStatement#getStatements <em>Statements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Statements</em>'.
	 * @see javaMM.SwitchStatement#getStatements()
	 * @see #getSwitchStatement()
	 * @generated
	 */
	EReference getSwitchStatement_Statements();

	/**
	 * Returns the meta object for the '{@link javaMM.SwitchStatement#moreThan3Cases(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>More Than3 Cases</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>More Than3 Cases</em>' operation.
	 * @see javaMM.SwitchStatement#moreThan3Cases(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getSwitchStatement__MoreThan3Cases__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.SynchronizedStatement <em>Synchronized Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Synchronized Statement</em>'.
	 * @see javaMM.SynchronizedStatement
	 * @generated
	 */
	EClass getSynchronizedStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SynchronizedStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.SynchronizedStatement#getBody()
	 * @see #getSynchronizedStatement()
	 * @generated
	 */
	EReference getSynchronizedStatement_Body();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.SynchronizedStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.SynchronizedStatement#getExpression()
	 * @see #getSynchronizedStatement()
	 * @generated
	 */
	EReference getSynchronizedStatement_Expression();

	/**
	 * Returns the meta object for the '{@link javaMM.SynchronizedStatement#hasStatements(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Has Statements</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Has Statements</em>' operation.
	 * @see javaMM.SynchronizedStatement#hasStatements(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getSynchronizedStatement__HasStatements__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.TagElement <em>Tag Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag Element</em>'.
	 * @see javaMM.TagElement
	 * @generated
	 */
	EClass getTagElement();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.TagElement#getTagName <em>Tag Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Tag Name</em>'.
	 * @see javaMM.TagElement#getTagName()
	 * @see #getTagElement()
	 * @generated
	 */
	EAttribute getTagElement_TagName();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.TagElement#getFragments <em>Fragments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Fragments</em>'.
	 * @see javaMM.TagElement#getFragments()
	 * @see #getTagElement()
	 * @generated
	 */
	EReference getTagElement_Fragments();

	/**
	 * Returns the meta object for class '{@link javaMM.TextElement <em>Text Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Text Element</em>'.
	 * @see javaMM.TextElement
	 * @generated
	 */
	EClass getTextElement();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.TextElement#getText <em>Text</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Text</em>'.
	 * @see javaMM.TextElement#getText()
	 * @see #getTextElement()
	 * @generated
	 */
	EAttribute getTextElement_Text();

	/**
	 * Returns the meta object for class '{@link javaMM.ThisExpression <em>This Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>This Expression</em>'.
	 * @see javaMM.ThisExpression
	 * @generated
	 */
	EClass getThisExpression();

	/**
	 * Returns the meta object for class '{@link javaMM.ThrowStatement <em>Throw Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Throw Statement</em>'.
	 * @see javaMM.ThrowStatement
	 * @generated
	 */
	EClass getThrowStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.ThrowStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.ThrowStatement#getExpression()
	 * @see #getThrowStatement()
	 * @generated
	 */
	EReference getThrowStatement_Expression();

	/**
	 * Returns the meta object for class '{@link javaMM.TryStatement <em>Try Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Try Statement</em>'.
	 * @see javaMM.TryStatement
	 * @generated
	 */
	EClass getTryStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.TryStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.TryStatement#getBody()
	 * @see #getTryStatement()
	 * @generated
	 */
	EReference getTryStatement_Body();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.TryStatement#getFinally <em>Finally</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Finally</em>'.
	 * @see javaMM.TryStatement#getFinally()
	 * @see #getTryStatement()
	 * @generated
	 */
	EReference getTryStatement_Finally();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.TryStatement#getCatchClauses <em>Catch Clauses</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Catch Clauses</em>'.
	 * @see javaMM.TryStatement#getCatchClauses()
	 * @see #getTryStatement()
	 * @generated
	 */
	EReference getTryStatement_CatchClauses();

	/**
	 * Returns the meta object for class '{@link javaMM.Type <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type</em>'.
	 * @see javaMM.Type
	 * @generated
	 */
	EClass getType();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.Type#getUsagesInTypeAccess <em>Usages In Type Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usages In Type Access</em>'.
	 * @see javaMM.Type#getUsagesInTypeAccess()
	 * @see #getType()
	 * @generated
	 */
	EReference getType_UsagesInTypeAccess();

	/**
	 * Returns the meta object for class '{@link javaMM.TypeAccess <em>Type Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type Access</em>'.
	 * @see javaMM.TypeAccess
	 * @generated
	 */
	EClass getTypeAccess();

	/**
	 * Returns the meta object for the reference '{@link javaMM.TypeAccess#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Type</em>'.
	 * @see javaMM.TypeAccess#getType()
	 * @see #getTypeAccess()
	 * @generated
	 */
	EReference getTypeAccess_Type();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.TypeAccess#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.TypeAccess#getQualifier()
	 * @see #getTypeAccess()
	 * @generated
	 */
	EReference getTypeAccess_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.TypeDeclaration <em>Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type Declaration</em>'.
	 * @see javaMM.TypeDeclaration
	 * @generated
	 */
	EClass getTypeDeclaration();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.TypeDeclaration#getTypeParameters <em>Type Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Type Parameters</em>'.
	 * @see javaMM.TypeDeclaration#getTypeParameters()
	 * @see #getTypeDeclaration()
	 * @generated
	 */
	EReference getTypeDeclaration_TypeParameters();

	/**
	 * Returns the meta object for class '{@link javaMM.TypeDeclarationStatement <em>Type Declaration Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type Declaration Statement</em>'.
	 * @see javaMM.TypeDeclarationStatement
	 * @generated
	 */
	EClass getTypeDeclarationStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.TypeDeclarationStatement#getDeclaration <em>Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Declaration</em>'.
	 * @see javaMM.TypeDeclarationStatement#getDeclaration()
	 * @see #getTypeDeclarationStatement()
	 * @generated
	 */
	EReference getTypeDeclarationStatement_Declaration();

	/**
	 * Returns the meta object for class '{@link javaMM.TypeLiteral <em>Type Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type Literal</em>'.
	 * @see javaMM.TypeLiteral
	 * @generated
	 */
	EClass getTypeLiteral();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.TypeLiteral#getType <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Type</em>'.
	 * @see javaMM.TypeLiteral#getType()
	 * @see #getTypeLiteral()
	 * @generated
	 */
	EReference getTypeLiteral_Type();

	/**
	 * Returns the meta object for class '{@link javaMM.TypeParameter <em>Type Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Type Parameter</em>'.
	 * @see javaMM.TypeParameter
	 * @generated
	 */
	EClass getTypeParameter();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.TypeParameter#getBounds <em>Bounds</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Bounds</em>'.
	 * @see javaMM.TypeParameter#getBounds()
	 * @see #getTypeParameter()
	 * @generated
	 */
	EReference getTypeParameter_Bounds();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedItem <em>Unresolved Item</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Item</em>'.
	 * @see javaMM.UnresolvedItem
	 * @generated
	 */
	EClass getUnresolvedItem();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedItemAccess <em>Unresolved Item Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Item Access</em>'.
	 * @see javaMM.UnresolvedItemAccess
	 * @generated
	 */
	EClass getUnresolvedItemAccess();

	/**
	 * Returns the meta object for the reference '{@link javaMM.UnresolvedItemAccess#getElement <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Element</em>'.
	 * @see javaMM.UnresolvedItemAccess#getElement()
	 * @see #getUnresolvedItemAccess()
	 * @generated
	 */
	EReference getUnresolvedItemAccess_Element();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.UnresolvedItemAccess#getQualifier <em>Qualifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Qualifier</em>'.
	 * @see javaMM.UnresolvedItemAccess#getQualifier()
	 * @see #getUnresolvedItemAccess()
	 * @generated
	 */
	EReference getUnresolvedItemAccess_Qualifier();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedAnnotationDeclaration <em>Unresolved Annotation Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Annotation Declaration</em>'.
	 * @see javaMM.UnresolvedAnnotationDeclaration
	 * @generated
	 */
	EClass getUnresolvedAnnotationDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedAnnotationTypeMemberDeclaration <em>Unresolved Annotation Type Member Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Annotation Type Member Declaration</em>'.
	 * @see javaMM.UnresolvedAnnotationTypeMemberDeclaration
	 * @generated
	 */
	EClass getUnresolvedAnnotationTypeMemberDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedClassDeclaration <em>Unresolved Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Class Declaration</em>'.
	 * @see javaMM.UnresolvedClassDeclaration
	 * @generated
	 */
	EClass getUnresolvedClassDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedEnumDeclaration <em>Unresolved Enum Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Enum Declaration</em>'.
	 * @see javaMM.UnresolvedEnumDeclaration
	 * @generated
	 */
	EClass getUnresolvedEnumDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedInterfaceDeclaration <em>Unresolved Interface Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Interface Declaration</em>'.
	 * @see javaMM.UnresolvedInterfaceDeclaration
	 * @generated
	 */
	EClass getUnresolvedInterfaceDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedLabeledStatement <em>Unresolved Labeled Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Labeled Statement</em>'.
	 * @see javaMM.UnresolvedLabeledStatement
	 * @generated
	 */
	EClass getUnresolvedLabeledStatement();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedMethodDeclaration <em>Unresolved Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Method Declaration</em>'.
	 * @see javaMM.UnresolvedMethodDeclaration
	 * @generated
	 */
	EClass getUnresolvedMethodDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedSingleVariableDeclaration <em>Unresolved Single Variable Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Single Variable Declaration</em>'.
	 * @see javaMM.UnresolvedSingleVariableDeclaration
	 * @generated
	 */
	EClass getUnresolvedSingleVariableDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedType <em>Unresolved Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Type</em>'.
	 * @see javaMM.UnresolvedType
	 * @generated
	 */
	EClass getUnresolvedType();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedTypeDeclaration <em>Unresolved Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Type Declaration</em>'.
	 * @see javaMM.UnresolvedTypeDeclaration
	 * @generated
	 */
	EClass getUnresolvedTypeDeclaration();

	/**
	 * Returns the meta object for class '{@link javaMM.UnresolvedVariableDeclarationFragment <em>Unresolved Variable Declaration Fragment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unresolved Variable Declaration Fragment</em>'.
	 * @see javaMM.UnresolvedVariableDeclarationFragment
	 * @generated
	 */
	EClass getUnresolvedVariableDeclarationFragment();

	/**
	 * Returns the meta object for class '{@link javaMM.VariableDeclaration <em>Variable Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Declaration</em>'.
	 * @see javaMM.VariableDeclaration
	 * @generated
	 */
	EClass getVariableDeclaration();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.VariableDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Extra Array Dimensions</em>'.
	 * @see javaMM.VariableDeclaration#getExtraArrayDimensions()
	 * @see #getVariableDeclaration()
	 * @generated
	 */
	EAttribute getVariableDeclaration_ExtraArrayDimensions();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.VariableDeclaration#getInitializer <em>Initializer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Initializer</em>'.
	 * @see javaMM.VariableDeclaration#getInitializer()
	 * @see #getVariableDeclaration()
	 * @generated
	 */
	EReference getVariableDeclaration_Initializer();

	/**
	 * Returns the meta object for the reference list '{@link javaMM.VariableDeclaration#getUsageInVariableAccess <em>Usage In Variable Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Usage In Variable Access</em>'.
	 * @see javaMM.VariableDeclaration#getUsageInVariableAccess()
	 * @see #getVariableDeclaration()
	 * @generated
	 */
	EReference getVariableDeclaration_UsageInVariableAccess();

	/**
	 * Returns the meta object for the '{@link javaMM.VariableDeclaration#variableIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Variable Is Used</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Variable Is Used</em>' operation.
	 * @see javaMM.VariableDeclaration#variableIsUsed(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getVariableDeclaration__VariableIsUsed__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.VariableDeclarationExpression <em>Variable Declaration Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Declaration Expression</em>'.
	 * @see javaMM.VariableDeclarationExpression
	 * @generated
	 */
	EClass getVariableDeclarationExpression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.VariableDeclarationExpression#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Modifier</em>'.
	 * @see javaMM.VariableDeclarationExpression#getModifier()
	 * @see #getVariableDeclarationExpression()
	 * @generated
	 */
	EReference getVariableDeclarationExpression_Modifier();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.VariableDeclarationExpression#getAnnotations <em>Annotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Annotations</em>'.
	 * @see javaMM.VariableDeclarationExpression#getAnnotations()
	 * @see #getVariableDeclarationExpression()
	 * @generated
	 */
	EReference getVariableDeclarationExpression_Annotations();

	/**
	 * Returns the meta object for the '{@link javaMM.VariableDeclarationExpression#publicVariableIsFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Public Variable Is Final</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Public Variable Is Final</em>' operation.
	 * @see javaMM.VariableDeclarationExpression#publicVariableIsFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getVariableDeclarationExpression__PublicVariableIsFinal__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.VariableDeclarationFragment <em>Variable Declaration Fragment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Declaration Fragment</em>'.
	 * @see javaMM.VariableDeclarationFragment
	 * @generated
	 */
	EClass getVariableDeclarationFragment();

	/**
	 * Returns the meta object for the container reference '{@link javaMM.VariableDeclarationFragment#getVariablesContainer <em>Variables Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the container reference '<em>Variables Container</em>'.
	 * @see javaMM.VariableDeclarationFragment#getVariablesContainer()
	 * @see #getVariableDeclarationFragment()
	 * @generated
	 */
	EReference getVariableDeclarationFragment_VariablesContainer();

	/**
	 * Returns the meta object for class '{@link javaMM.VariableDeclarationStatement <em>Variable Declaration Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Declaration Statement</em>'.
	 * @see javaMM.VariableDeclarationStatement
	 * @generated
	 */
	EClass getVariableDeclarationStatement();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.VariableDeclarationStatement#getExtraArrayDimensions <em>Extra Array Dimensions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Extra Array Dimensions</em>'.
	 * @see javaMM.VariableDeclarationStatement#getExtraArrayDimensions()
	 * @see #getVariableDeclarationStatement()
	 * @generated
	 */
	EAttribute getVariableDeclarationStatement_ExtraArrayDimensions();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.VariableDeclarationStatement#getModifier <em>Modifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Modifier</em>'.
	 * @see javaMM.VariableDeclarationStatement#getModifier()
	 * @see #getVariableDeclarationStatement()
	 * @generated
	 */
	EReference getVariableDeclarationStatement_Modifier();

	/**
	 * Returns the meta object for the containment reference list '{@link javaMM.VariableDeclarationStatement#getAnnotations <em>Annotations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Annotations</em>'.
	 * @see javaMM.VariableDeclarationStatement#getAnnotations()
	 * @see #getVariableDeclarationStatement()
	 * @generated
	 */
	EReference getVariableDeclarationStatement_Annotations();

	/**
	 * Returns the meta object for the '{@link javaMM.VariableDeclarationStatement#publicVariableIsFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map) <em>Public Variable Is Final</em>}' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the '<em>Public Variable Is Final</em>' operation.
	 * @see javaMM.VariableDeclarationStatement#publicVariableIsFinal(org.eclipse.emf.common.util.DiagnosticChain, java.util.Map)
	 * @generated
	 */
	EOperation getVariableDeclarationStatement__PublicVariableIsFinal__DiagnosticChain_Map();

	/**
	 * Returns the meta object for class '{@link javaMM.WildCardType <em>Wild Card Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Wild Card Type</em>'.
	 * @see javaMM.WildCardType
	 * @generated
	 */
	EClass getWildCardType();

	/**
	 * Returns the meta object for the attribute '{@link javaMM.WildCardType#getUpperBound <em>Upper Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Upper Bound</em>'.
	 * @see javaMM.WildCardType#getUpperBound()
	 * @see #getWildCardType()
	 * @generated
	 */
	EAttribute getWildCardType_UpperBound();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.WildCardType#getBound <em>Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Bound</em>'.
	 * @see javaMM.WildCardType#getBound()
	 * @see #getWildCardType()
	 * @generated
	 */
	EReference getWildCardType_Bound();

	/**
	 * Returns the meta object for class '{@link javaMM.WhileStatement <em>While Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>While Statement</em>'.
	 * @see javaMM.WhileStatement
	 * @generated
	 */
	EClass getWhileStatement();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.WhileStatement#getExpression <em>Expression</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expression</em>'.
	 * @see javaMM.WhileStatement#getExpression()
	 * @see #getWhileStatement()
	 * @generated
	 */
	EReference getWhileStatement_Expression();

	/**
	 * Returns the meta object for the containment reference '{@link javaMM.WhileStatement#getBody <em>Body</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Body</em>'.
	 * @see javaMM.WhileStatement#getBody()
	 * @see #getWhileStatement()
	 * @generated
	 */
	EReference getWhileStatement_Body();

	/**
	 * Returns the meta object for enum '{@link javaMM.AssignmentKind <em>Assignment Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Assignment Kind</em>'.
	 * @see javaMM.AssignmentKind
	 * @generated
	 */
	EEnum getAssignmentKind();

	/**
	 * Returns the meta object for enum '{@link javaMM.InfixExpressionKind <em>Infix Expression Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Infix Expression Kind</em>'.
	 * @see javaMM.InfixExpressionKind
	 * @generated
	 */
	EEnum getInfixExpressionKind();

	/**
	 * Returns the meta object for enum '{@link javaMM.InheritanceKind <em>Inheritance Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Inheritance Kind</em>'.
	 * @see javaMM.InheritanceKind
	 * @generated
	 */
	EEnum getInheritanceKind();

	/**
	 * Returns the meta object for enum '{@link javaMM.PostfixExpressionKind <em>Postfix Expression Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Postfix Expression Kind</em>'.
	 * @see javaMM.PostfixExpressionKind
	 * @generated
	 */
	EEnum getPostfixExpressionKind();

	/**
	 * Returns the meta object for enum '{@link javaMM.PrefixExpressionKind <em>Prefix Expression Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Prefix Expression Kind</em>'.
	 * @see javaMM.PrefixExpressionKind
	 * @generated
	 */
	EEnum getPrefixExpressionKind();

	/**
	 * Returns the meta object for enum '{@link javaMM.VisibilityKind <em>Visibility Kind</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Visibility Kind</em>'.
	 * @see javaMM.VisibilityKind
	 * @generated
	 */
	EEnum getVisibilityKind();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	JavaMMFactory getJavaMMFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals {
		/**
		 * The meta object literal for the '{@link javaMM.impl.AbstractMethodDeclarationImpl <em>Abstract Method Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AbstractMethodDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAbstractMethodDeclaration()
		 * @generated
		 */
		EClass ABSTRACT_METHOD_DECLARATION = eINSTANCE.getAbstractMethodDeclaration();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__BODY = eINSTANCE.getAbstractMethodDeclaration_Body();

		/**
		 * The meta object literal for the '<em><b>Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__PARAMETERS = eINSTANCE.getAbstractMethodDeclaration_Parameters();

		/**
		 * The meta object literal for the '<em><b>Thrown Exceptions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS = eINSTANCE.getAbstractMethodDeclaration_ThrownExceptions();

		/**
		 * The meta object literal for the '<em><b>Type Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS = eINSTANCE.getAbstractMethodDeclaration_TypeParameters();

		/**
		 * The meta object literal for the '<em><b>Usages In Doc Comments</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS = eINSTANCE.getAbstractMethodDeclaration_UsagesInDocComments();

		/**
		 * The meta object literal for the '<em><b>Usages</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_DECLARATION__USAGES = eINSTANCE.getAbstractMethodDeclaration_Usages();

		/**
		 * The meta object literal for the '<em><b>Parameters Effectively Final</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation ABSTRACT_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP = eINSTANCE.getAbstractMethodDeclaration__ParametersEffectivelyFinal__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Local Method Is Used</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation ABSTRACT_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP = eINSTANCE.getAbstractMethodDeclaration__LocalMethodIsUsed__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AbstractMethodInvocationImpl <em>Abstract Method Invocation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AbstractMethodInvocationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAbstractMethodInvocation()
		 * @generated
		 */
		EClass ABSTRACT_METHOD_INVOCATION = eINSTANCE.getAbstractMethodInvocation();

		/**
		 * The meta object literal for the '<em><b>Method</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_INVOCATION__METHOD = eINSTANCE.getAbstractMethodInvocation_Method();

		/**
		 * The meta object literal for the '<em><b>Arguments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_INVOCATION__ARGUMENTS = eINSTANCE.getAbstractMethodInvocation_Arguments();

		/**
		 * The meta object literal for the '<em><b>Type Arguments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_METHOD_INVOCATION__TYPE_ARGUMENTS = eINSTANCE.getAbstractMethodInvocation_TypeArguments();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AbstractTypeDeclarationImpl <em>Abstract Type Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AbstractTypeDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAbstractTypeDeclaration()
		 * @generated
		 */
		EClass ABSTRACT_TYPE_DECLARATION = eINSTANCE.getAbstractTypeDeclaration();

		/**
		 * The meta object literal for the '<em><b>Body Declarations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS = eINSTANCE.getAbstractTypeDeclaration_BodyDeclarations();

		/**
		 * The meta object literal for the '<em><b>Comments Before Body</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY = eINSTANCE.getAbstractTypeDeclaration_CommentsBeforeBody();

		/**
		 * The meta object literal for the '<em><b>Comments After Body</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY = eINSTANCE.getAbstractTypeDeclaration_CommentsAfterBody();

		/**
		 * The meta object literal for the '<em><b>Package</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_DECLARATION__PACKAGE = eINSTANCE.getAbstractTypeDeclaration_Package();

		/**
		 * The meta object literal for the '<em><b>Super Interfaces</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES = eINSTANCE.getAbstractTypeDeclaration_SuperInterfaces();

		/**
		 * The meta object literal for the '<em><b>Implements </b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation ABSTRACT_TYPE_DECLARATION___IMPLEMENTS____STRING = eINSTANCE.getAbstractTypeDeclaration__Implements___String();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AbstractTypeQualifiedExpressionImpl <em>Abstract Type Qualified Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AbstractTypeQualifiedExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAbstractTypeQualifiedExpression()
		 * @generated
		 */
		EClass ABSTRACT_TYPE_QUALIFIED_EXPRESSION = eINSTANCE.getAbstractTypeQualifiedExpression();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_TYPE_QUALIFIED_EXPRESSION__QUALIFIER = eINSTANCE.getAbstractTypeQualifiedExpression_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AbstractVariablesContainerImpl <em>Abstract Variables Container</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AbstractVariablesContainerImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAbstractVariablesContainer()
		 * @generated
		 */
		EClass ABSTRACT_VARIABLES_CONTAINER = eINSTANCE.getAbstractVariablesContainer();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_VARIABLES_CONTAINER__TYPE = eINSTANCE.getAbstractVariablesContainer_Type();

		/**
		 * The meta object literal for the '<em><b>Fragments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ABSTRACT_VARIABLES_CONTAINER__FRAGMENTS = eINSTANCE.getAbstractVariablesContainer_Fragments();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AnnotationImpl <em>Annotation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AnnotationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAnnotation()
		 * @generated
		 */
		EClass ANNOTATION = eINSTANCE.getAnnotation();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION__TYPE = eINSTANCE.getAnnotation_Type();

		/**
		 * The meta object literal for the '<em><b>Values</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION__VALUES = eINSTANCE.getAnnotation_Values();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArchiveImpl <em>Archive</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArchiveImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArchive()
		 * @generated
		 */
		EClass ARCHIVE = eINSTANCE.getArchive();

		/**
		 * The meta object literal for the '<em><b>Original File Path</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ARCHIVE__ORIGINAL_FILE_PATH = eINSTANCE.getArchive_OriginalFilePath();

		/**
		 * The meta object literal for the '<em><b>Class Files</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARCHIVE__CLASS_FILES = eINSTANCE.getArchive_ClassFiles();

		/**
		 * The meta object literal for the '<em><b>Manifest</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARCHIVE__MANIFEST = eINSTANCE.getArchive_Manifest();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AssertStatementImpl <em>Assert Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AssertStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAssertStatement()
		 * @generated
		 */
		EClass ASSERT_STATEMENT = eINSTANCE.getAssertStatement();

		/**
		 * The meta object literal for the '<em><b>Message</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ASSERT_STATEMENT__MESSAGE = eINSTANCE.getAssertStatement_Message();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ASSERT_STATEMENT__EXPRESSION = eINSTANCE.getAssertStatement_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ASTNodeImpl <em>AST Node</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ASTNodeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getASTNode()
		 * @generated
		 */
		EClass AST_NODE = eINSTANCE.getASTNode();

		/**
		 * The meta object literal for the '<em><b>Comments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AST_NODE__COMMENTS = eINSTANCE.getASTNode_Comments();

		/**
		 * The meta object literal for the '<em><b>Original Compilation Unit</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AST_NODE__ORIGINAL_COMPILATION_UNIT = eINSTANCE.getASTNode_OriginalCompilationUnit();

		/**
		 * The meta object literal for the '<em><b>Original Class File</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AST_NODE__ORIGINAL_CLASS_FILE = eINSTANCE.getASTNode_OriginalClassFile();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AnnotationMemberValuePairImpl <em>Annotation Member Value Pair</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AnnotationMemberValuePairImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationMemberValuePair()
		 * @generated
		 */
		EClass ANNOTATION_MEMBER_VALUE_PAIR = eINSTANCE.getAnnotationMemberValuePair();

		/**
		 * The meta object literal for the '<em><b>Member</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION_MEMBER_VALUE_PAIR__MEMBER = eINSTANCE.getAnnotationMemberValuePair_Member();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION_MEMBER_VALUE_PAIR__VALUE = eINSTANCE.getAnnotationMemberValuePair_Value();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AnnotationTypeDeclarationImpl <em>Annotation Type Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AnnotationTypeDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationTypeDeclaration()
		 * @generated
		 */
		EClass ANNOTATION_TYPE_DECLARATION = eINSTANCE.getAnnotationTypeDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AnnotationTypeMemberDeclarationImpl <em>Annotation Type Member Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AnnotationTypeMemberDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAnnotationTypeMemberDeclaration()
		 * @generated
		 */
		EClass ANNOTATION_TYPE_MEMBER_DECLARATION = eINSTANCE.getAnnotationTypeMemberDeclaration();

		/**
		 * The meta object literal for the '<em><b>Default</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION_TYPE_MEMBER_DECLARATION__DEFAULT = eINSTANCE.getAnnotationTypeMemberDeclaration_Default();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION_TYPE_MEMBER_DECLARATION__TYPE = eINSTANCE.getAnnotationTypeMemberDeclaration_Type();

		/**
		 * The meta object literal for the '<em><b>Usages</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANNOTATION_TYPE_MEMBER_DECLARATION__USAGES = eINSTANCE.getAnnotationTypeMemberDeclaration_Usages();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AnonymousClassDeclarationImpl <em>Anonymous Class Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AnonymousClassDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAnonymousClassDeclaration()
		 * @generated
		 */
		EClass ANONYMOUS_CLASS_DECLARATION = eINSTANCE.getAnonymousClassDeclaration();

		/**
		 * The meta object literal for the '<em><b>Body Declarations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS = eINSTANCE.getAnonymousClassDeclaration_BodyDeclarations();

		/**
		 * The meta object literal for the '<em><b>Class Instance Creation</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION = eINSTANCE.getAnonymousClassDeclaration_ClassInstanceCreation();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArrayAccessImpl <em>Array Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArrayAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArrayAccess()
		 * @generated
		 */
		EClass ARRAY_ACCESS = eINSTANCE.getArrayAccess();

		/**
		 * The meta object literal for the '<em><b>Array</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_ACCESS__ARRAY = eINSTANCE.getArrayAccess_Array();

		/**
		 * The meta object literal for the '<em><b>Index</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_ACCESS__INDEX = eINSTANCE.getArrayAccess_Index();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArrayCreationImpl <em>Array Creation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArrayCreationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArrayCreation()
		 * @generated
		 */
		EClass ARRAY_CREATION = eINSTANCE.getArrayCreation();

		/**
		 * The meta object literal for the '<em><b>Dimensions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_CREATION__DIMENSIONS = eINSTANCE.getArrayCreation_Dimensions();

		/**
		 * The meta object literal for the '<em><b>Initializer</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_CREATION__INITIALIZER = eINSTANCE.getArrayCreation_Initializer();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_CREATION__TYPE = eINSTANCE.getArrayCreation_Type();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArrayInitializerImpl <em>Array Initializer</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArrayInitializerImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArrayInitializer()
		 * @generated
		 */
		EClass ARRAY_INITIALIZER = eINSTANCE.getArrayInitializer();

		/**
		 * The meta object literal for the '<em><b>Expressions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_INITIALIZER__EXPRESSIONS = eINSTANCE.getArrayInitializer_Expressions();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArrayLengthAccessImpl <em>Array Length Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArrayLengthAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArrayLengthAccess()
		 * @generated
		 */
		EClass ARRAY_LENGTH_ACCESS = eINSTANCE.getArrayLengthAccess();

		/**
		 * The meta object literal for the '<em><b>Array</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_LENGTH_ACCESS__ARRAY = eINSTANCE.getArrayLengthAccess_Array();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ArrayTypeImpl <em>Array Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ArrayTypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getArrayType()
		 * @generated
		 */
		EClass ARRAY_TYPE = eINSTANCE.getArrayType();

		/**
		 * The meta object literal for the '<em><b>Dimensions</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ARRAY_TYPE__DIMENSIONS = eINSTANCE.getArrayType_Dimensions();

		/**
		 * The meta object literal for the '<em><b>Element Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_TYPE__ELEMENT_TYPE = eINSTANCE.getArrayType_ElementType();

		/**
		 * The meta object literal for the '{@link javaMM.impl.AssignmentImpl <em>Assignment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.AssignmentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getAssignment()
		 * @generated
		 */
		EClass ASSIGNMENT = eINSTANCE.getAssignment();

		/**
		 * The meta object literal for the '<em><b>Left Hand Side</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ASSIGNMENT__LEFT_HAND_SIDE = eINSTANCE.getAssignment_LeftHandSide();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ASSIGNMENT__OPERATOR = eINSTANCE.getAssignment_Operator();

		/**
		 * The meta object literal for the '<em><b>Right Hand Side</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ASSIGNMENT__RIGHT_HAND_SIDE = eINSTANCE.getAssignment_RightHandSide();

		/**
		 * The meta object literal for the '<em><b>No Redundant Assignment</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation ASSIGNMENT___NO_REDUNDANT_ASSIGNMENT__DIAGNOSTICCHAIN_MAP = eINSTANCE.getAssignment__NoRedundantAssignment__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.BodyDeclarationImpl <em>Body Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.BodyDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getBodyDeclaration()
		 * @generated
		 */
		EClass BODY_DECLARATION = eINSTANCE.getBodyDeclaration();

		/**
		 * The meta object literal for the '<em><b>Abstract Type Declaration</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION = eINSTANCE.getBodyDeclaration_AbstractTypeDeclaration();

		/**
		 * The meta object literal for the '<em><b>Annotations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BODY_DECLARATION__ANNOTATIONS = eINSTANCE.getBodyDeclaration_Annotations();

		/**
		 * The meta object literal for the '<em><b>Anonymous Class Declaration Owner</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER = eINSTANCE.getBodyDeclaration_AnonymousClassDeclarationOwner();

		/**
		 * The meta object literal for the '<em><b>Modifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BODY_DECLARATION__MODIFIER = eINSTANCE.getBodyDeclaration_Modifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.BooleanLiteralImpl <em>Boolean Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.BooleanLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getBooleanLiteral()
		 * @generated
		 */
		EClass BOOLEAN_LITERAL = eINSTANCE.getBooleanLiteral();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BOOLEAN_LITERAL__VALUE = eINSTANCE.getBooleanLiteral_Value();

		/**
		 * The meta object literal for the '{@link javaMM.impl.BlockCommentImpl <em>Block Comment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.BlockCommentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getBlockComment()
		 * @generated
		 */
		EClass BLOCK_COMMENT = eINSTANCE.getBlockComment();

		/**
		 * The meta object literal for the '{@link javaMM.impl.BlockImpl <em>Block</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.BlockImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getBlock()
		 * @generated
		 */
		EClass BLOCK = eINSTANCE.getBlock();

		/**
		 * The meta object literal for the '<em><b>Statements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BLOCK__STATEMENTS = eINSTANCE.getBlock_Statements();

		/**
		 * The meta object literal for the '<em><b>Empty Block Is Documented</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation BLOCK___EMPTY_BLOCK_IS_DOCUMENTED__DIAGNOSTICCHAIN_MAP = eINSTANCE.getBlock__EmptyBlockIsDocumented__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.BreakStatementImpl <em>Break Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.BreakStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getBreakStatement()
		 * @generated
		 */
		EClass BREAK_STATEMENT = eINSTANCE.getBreakStatement();

		/**
		 * The meta object literal for the '<em><b>Label</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BREAK_STATEMENT__LABEL = eINSTANCE.getBreakStatement_Label();

		/**
		 * The meta object literal for the '{@link javaMM.impl.CastExpressionImpl <em>Cast Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.CastExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getCastExpression()
		 * @generated
		 */
		EClass CAST_EXPRESSION = eINSTANCE.getCastExpression();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CAST_EXPRESSION__EXPRESSION = eINSTANCE.getCastExpression_Expression();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CAST_EXPRESSION__TYPE = eINSTANCE.getCastExpression_Type();

		/**
		 * The meta object literal for the '{@link javaMM.impl.CatchClauseImpl <em>Catch Clause</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.CatchClauseImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getCatchClause()
		 * @generated
		 */
		EClass CATCH_CLAUSE = eINSTANCE.getCatchClause();

		/**
		 * The meta object literal for the '<em><b>Exception</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CATCH_CLAUSE__EXCEPTION = eINSTANCE.getCatchClause_Exception();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CATCH_CLAUSE__BODY = eINSTANCE.getCatchClause_Body();

		/**
		 * The meta object literal for the '<em><b>Does Not Catch Dubious Exceptions</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CATCH_CLAUSE___DOES_NOT_CATCH_DUBIOUS_EXCEPTIONS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getCatchClause__DoesNotCatchDubiousExceptions__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Exception Is Used</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CATCH_CLAUSE___EXCEPTION_IS_USED__DIAGNOSTICCHAIN_MAP = eINSTANCE.getCatchClause__ExceptionIsUsed__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.CharacterLiteralImpl <em>Character Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.CharacterLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getCharacterLiteral()
		 * @generated
		 */
		EClass CHARACTER_LITERAL = eINSTANCE.getCharacterLiteral();

		/**
		 * The meta object literal for the '<em><b>Escaped Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CHARACTER_LITERAL__ESCAPED_VALUE = eINSTANCE.getCharacterLiteral_EscapedValue();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ClassFileImpl <em>Class File</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ClassFileImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getClassFile()
		 * @generated
		 */
		EClass CLASS_FILE = eINSTANCE.getClassFile();

		/**
		 * The meta object literal for the '<em><b>Original File Path</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CLASS_FILE__ORIGINAL_FILE_PATH = eINSTANCE.getClassFile_OriginalFilePath();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_FILE__TYPE = eINSTANCE.getClassFile_Type();

		/**
		 * The meta object literal for the '<em><b>Attached Source</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_FILE__ATTACHED_SOURCE = eINSTANCE.getClassFile_AttachedSource();

		/**
		 * The meta object literal for the '<em><b>Package</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_FILE__PACKAGE = eINSTANCE.getClassFile_Package();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ClassInstanceCreationImpl <em>Class Instance Creation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ClassInstanceCreationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getClassInstanceCreation()
		 * @generated
		 */
		EClass CLASS_INSTANCE_CREATION = eINSTANCE.getClassInstanceCreation();

		/**
		 * The meta object literal for the '<em><b>Anonymous Class Declaration</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_INSTANCE_CREATION__ANONYMOUS_CLASS_DECLARATION = eINSTANCE.getClassInstanceCreation_AnonymousClassDeclaration();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_INSTANCE_CREATION__EXPRESSION = eINSTANCE.getClassInstanceCreation_Expression();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_INSTANCE_CREATION__TYPE = eINSTANCE.getClassInstanceCreation_Type();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ConstructorDeclarationImpl <em>Constructor Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ConstructorDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getConstructorDeclaration()
		 * @generated
		 */
		EClass CONSTRUCTOR_DECLARATION = eINSTANCE.getConstructorDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ConditionalExpressionImpl <em>Conditional Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ConditionalExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getConditionalExpression()
		 * @generated
		 */
		EClass CONDITIONAL_EXPRESSION = eINSTANCE.getConditionalExpression();

		/**
		 * The meta object literal for the '<em><b>Else Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CONDITIONAL_EXPRESSION__ELSE_EXPRESSION = eINSTANCE.getConditionalExpression_ElseExpression();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CONDITIONAL_EXPRESSION__EXPRESSION = eINSTANCE.getConditionalExpression_Expression();

		/**
		 * The meta object literal for the '<em><b>Then Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CONDITIONAL_EXPRESSION__THEN_EXPRESSION = eINSTANCE.getConditionalExpression_ThenExpression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ConstructorInvocationImpl <em>Constructor Invocation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ConstructorInvocationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getConstructorInvocation()
		 * @generated
		 */
		EClass CONSTRUCTOR_INVOCATION = eINSTANCE.getConstructorInvocation();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ClassDeclarationImpl <em>Class Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ClassDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getClassDeclaration()
		 * @generated
		 */
		EClass CLASS_DECLARATION = eINSTANCE.getClassDeclaration();

		/**
		 * The meta object literal for the '<em><b>Super Class</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CLASS_DECLARATION__SUPER_CLASS = eINSTANCE.getClassDeclaration_SuperClass();

		/**
		 * The meta object literal for the '<em><b>Get Methods</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___GET_METHODS = eINSTANCE.getClassDeclaration__GetMethods();

		/**
		 * The meta object literal for the '<em><b>Has Equals</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___HAS_EQUALS = eINSTANCE.getClassDeclaration__HasEquals();

		/**
		 * The meta object literal for the '<em><b>Has Hashcode</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___HAS_HASHCODE = eINSTANCE.getClassDeclaration__HasHashcode();

		/**
		 * The meta object literal for the '<em><b>Has Compare To</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___HAS_COMPARE_TO = eINSTANCE.getClassDeclaration__HasCompareTo();

		/**
		 * The meta object literal for the '<em><b>No Covariant Compare To</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___NO_COVARIANT_COMPARE_TO__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__NoCovariantCompareTo__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Hash Code And Equals</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___HASH_CODE_AND_EQUALS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__HashCodeAndEquals__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Serial UID In Serializable Class</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___SERIAL_UID_IN_SERIALIZABLE_CLASS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__SerialUIDInSerializableClass__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Equals And Compare To</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___EQUALS_AND_COMPARE_TO__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__EqualsAndCompareTo__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>No Redundant Interface Impl</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___NO_REDUNDANT_INTERFACE_IMPL__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__NoRedundantInterfaceImpl__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Clone In Cloneable</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___CLONE_IN_CLONEABLE__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__CloneInCloneable__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>No Covariant Equals</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___NO_COVARIANT_EQUALS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__NoCovariantEquals__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>No Obscured Variables</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___NO_OBSCURED_VARIABLES__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__NoObscuredVariables__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Comparator Implements Serializable</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation CLASS_DECLARATION___COMPARATOR_IMPLEMENTS_SERIALIZABLE__DIAGNOSTICCHAIN_MAP = eINSTANCE.getClassDeclaration__ComparatorImplementsSerializable__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.CommentImpl <em>Comment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.CommentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getComment()
		 * @generated
		 */
		EClass COMMENT = eINSTANCE.getComment();

		/**
		 * The meta object literal for the '<em><b>Content</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMMENT__CONTENT = eINSTANCE.getComment_Content();

		/**
		 * The meta object literal for the '<em><b>Enclosed By Parent</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMMENT__ENCLOSED_BY_PARENT = eINSTANCE.getComment_EnclosedByParent();

		/**
		 * The meta object literal for the '<em><b>Prefix Of Parent</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMMENT__PREFIX_OF_PARENT = eINSTANCE.getComment_PrefixOfParent();

		/**
		 * The meta object literal for the '{@link javaMM.impl.CompilationUnitImpl <em>Compilation Unit</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.CompilationUnitImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getCompilationUnit()
		 * @generated
		 */
		EClass COMPILATION_UNIT = eINSTANCE.getCompilationUnit();

		/**
		 * The meta object literal for the '<em><b>Original File Path</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COMPILATION_UNIT__ORIGINAL_FILE_PATH = eINSTANCE.getCompilationUnit_OriginalFilePath();

		/**
		 * The meta object literal for the '<em><b>Comment List</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPILATION_UNIT__COMMENT_LIST = eINSTANCE.getCompilationUnit_CommentList();

		/**
		 * The meta object literal for the '<em><b>Imports</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPILATION_UNIT__IMPORTS = eINSTANCE.getCompilationUnit_Imports();

		/**
		 * The meta object literal for the '<em><b>Package</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPILATION_UNIT__PACKAGE = eINSTANCE.getCompilationUnit_Package();

		/**
		 * The meta object literal for the '<em><b>Types</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPILATION_UNIT__TYPES = eINSTANCE.getCompilationUnit_Types();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ContinueStatementImpl <em>Continue Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ContinueStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getContinueStatement()
		 * @generated
		 */
		EClass CONTINUE_STATEMENT = eINSTANCE.getContinueStatement();

		/**
		 * The meta object literal for the '<em><b>Label</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CONTINUE_STATEMENT__LABEL = eINSTANCE.getContinueStatement_Label();

		/**
		 * The meta object literal for the '{@link javaMM.impl.DoStatementImpl <em>Do Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.DoStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getDoStatement()
		 * @generated
		 */
		EClass DO_STATEMENT = eINSTANCE.getDoStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DO_STATEMENT__EXPRESSION = eINSTANCE.getDoStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DO_STATEMENT__BODY = eINSTANCE.getDoStatement_Body();

		/**
		 * The meta object literal for the '{@link javaMM.impl.EmptyStatementImpl <em>Empty Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.EmptyStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getEmptyStatement()
		 * @generated
		 */
		EClass EMPTY_STATEMENT = eINSTANCE.getEmptyStatement();

		/**
		 * The meta object literal for the '{@link javaMM.impl.EnhancedForStatementImpl <em>Enhanced For Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.EnhancedForStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getEnhancedForStatement()
		 * @generated
		 */
		EClass ENHANCED_FOR_STATEMENT = eINSTANCE.getEnhancedForStatement();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENHANCED_FOR_STATEMENT__BODY = eINSTANCE.getEnhancedForStatement_Body();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENHANCED_FOR_STATEMENT__EXPRESSION = eINSTANCE.getEnhancedForStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Parameter</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENHANCED_FOR_STATEMENT__PARAMETER = eINSTANCE.getEnhancedForStatement_Parameter();

		/**
		 * The meta object literal for the '{@link javaMM.impl.EnumConstantDeclarationImpl <em>Enum Constant Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.EnumConstantDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getEnumConstantDeclaration()
		 * @generated
		 */
		EClass ENUM_CONSTANT_DECLARATION = eINSTANCE.getEnumConstantDeclaration();

		/**
		 * The meta object literal for the '<em><b>Anonymous Class Declaration</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENUM_CONSTANT_DECLARATION__ANONYMOUS_CLASS_DECLARATION = eINSTANCE.getEnumConstantDeclaration_AnonymousClassDeclaration();

		/**
		 * The meta object literal for the '<em><b>Arguments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENUM_CONSTANT_DECLARATION__ARGUMENTS = eINSTANCE.getEnumConstantDeclaration_Arguments();

		/**
		 * The meta object literal for the '{@link javaMM.impl.EnumDeclarationImpl <em>Enum Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.EnumDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getEnumDeclaration()
		 * @generated
		 */
		EClass ENUM_DECLARATION = eINSTANCE.getEnumDeclaration();

		/**
		 * The meta object literal for the '<em><b>Enum Constants</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENUM_DECLARATION__ENUM_CONSTANTS = eINSTANCE.getEnumDeclaration_EnumConstants();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ExpressionImpl <em>Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getExpression()
		 * @generated
		 */
		EClass EXPRESSION = eINSTANCE.getExpression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ExpressionStatementImpl <em>Expression Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ExpressionStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getExpressionStatement()
		 * @generated
		 */
		EClass EXPRESSION_STATEMENT = eINSTANCE.getExpressionStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPRESSION_STATEMENT__EXPRESSION = eINSTANCE.getExpressionStatement_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.FieldAccessImpl <em>Field Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.FieldAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getFieldAccess()
		 * @generated
		 */
		EClass FIELD_ACCESS = eINSTANCE.getFieldAccess();

		/**
		 * The meta object literal for the '<em><b>Field</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FIELD_ACCESS__FIELD = eINSTANCE.getFieldAccess_Field();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FIELD_ACCESS__EXPRESSION = eINSTANCE.getFieldAccess_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.FieldDeclarationImpl <em>Field Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.FieldDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getFieldDeclaration()
		 * @generated
		 */
		EClass FIELD_DECLARATION = eINSTANCE.getFieldDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ForStatementImpl <em>For Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ForStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getForStatement()
		 * @generated
		 */
		EClass FOR_STATEMENT = eINSTANCE.getForStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FOR_STATEMENT__EXPRESSION = eINSTANCE.getForStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Updaters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FOR_STATEMENT__UPDATERS = eINSTANCE.getForStatement_Updaters();

		/**
		 * The meta object literal for the '<em><b>Initializers</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FOR_STATEMENT__INITIALIZERS = eINSTANCE.getForStatement_Initializers();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FOR_STATEMENT__BODY = eINSTANCE.getForStatement_Body();

		/**
		 * The meta object literal for the '{@link javaMM.impl.IfStatementImpl <em>If Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.IfStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getIfStatement()
		 * @generated
		 */
		EClass IF_STATEMENT = eINSTANCE.getIfStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IF_STATEMENT__EXPRESSION = eINSTANCE.getIfStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Then Statement</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IF_STATEMENT__THEN_STATEMENT = eINSTANCE.getIfStatement_ThenStatement();

		/**
		 * The meta object literal for the '<em><b>Else Statement</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IF_STATEMENT__ELSE_STATEMENT = eINSTANCE.getIfStatement_ElseStatement();

		/**
		 * The meta object literal for the '<em><b>No Dead Code</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation IF_STATEMENT___NO_DEAD_CODE__DIAGNOSTICCHAIN_MAP = eINSTANCE.getIfStatement__NoDeadCode__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>No Useless Control Flow</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation IF_STATEMENT___NO_USELESS_CONTROL_FLOW__DIAGNOSTICCHAIN_MAP = eINSTANCE.getIfStatement__NoUselessControlFlow__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ImportDeclarationImpl <em>Import Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ImportDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getImportDeclaration()
		 * @generated
		 */
		EClass IMPORT_DECLARATION = eINSTANCE.getImportDeclaration();

		/**
		 * The meta object literal for the '<em><b>Static</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute IMPORT_DECLARATION__STATIC = eINSTANCE.getImportDeclaration_Static();

		/**
		 * The meta object literal for the '<em><b>Imported Element</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference IMPORT_DECLARATION__IMPORTED_ELEMENT = eINSTANCE.getImportDeclaration_ImportedElement();

		/**
		 * The meta object literal for the '{@link javaMM.impl.InfixExpressionImpl <em>Infix Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.InfixExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getInfixExpression()
		 * @generated
		 */
		EClass INFIX_EXPRESSION = eINSTANCE.getInfixExpression();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute INFIX_EXPRESSION__OPERATOR = eINSTANCE.getInfixExpression_Operator();

		/**
		 * The meta object literal for the '<em><b>Right Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INFIX_EXPRESSION__RIGHT_OPERAND = eINSTANCE.getInfixExpression_RightOperand();

		/**
		 * The meta object literal for the '<em><b>Left Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INFIX_EXPRESSION__LEFT_OPERAND = eINSTANCE.getInfixExpression_LeftOperand();

		/**
		 * The meta object literal for the '<em><b>Extended Operands</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INFIX_EXPRESSION__EXTENDED_OPERANDS = eINSTANCE.getInfixExpression_ExtendedOperands();

		/**
		 * The meta object literal for the '<em><b>Operator Is Equality</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation INFIX_EXPRESSION___OPERATOR_IS_EQUALITY = eINSTANCE.getInfixExpression__OperatorIsEquality();

		/**
		 * The meta object literal for the '<em><b>Equals Not On Literals</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation INFIX_EXPRESSION___EQUALS_NOT_ON_LITERALS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getInfixExpression__EqualsNotOnLiterals__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>No Redundant Comparison</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation INFIX_EXPRESSION___NO_REDUNDANT_COMPARISON__DIAGNOSTICCHAIN_MAP = eINSTANCE.getInfixExpression__NoRedundantComparison__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Equals Not On Strings</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation INFIX_EXPRESSION___EQUALS_NOT_ON_STRINGS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getInfixExpression__EqualsNotOnStrings__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.InitializerImpl <em>Initializer</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.InitializerImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getInitializer()
		 * @generated
		 */
		EClass INITIALIZER = eINSTANCE.getInitializer();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INITIALIZER__BODY = eINSTANCE.getInitializer_Body();

		/**
		 * The meta object literal for the '{@link javaMM.impl.InstanceofExpressionImpl <em>Instanceof Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.InstanceofExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getInstanceofExpression()
		 * @generated
		 */
		EClass INSTANCEOF_EXPRESSION = eINSTANCE.getInstanceofExpression();

		/**
		 * The meta object literal for the '<em><b>Right Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INSTANCEOF_EXPRESSION__RIGHT_OPERAND = eINSTANCE.getInstanceofExpression_RightOperand();

		/**
		 * The meta object literal for the '<em><b>Left Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INSTANCEOF_EXPRESSION__LEFT_OPERAND = eINSTANCE.getInstanceofExpression_LeftOperand();

		/**
		 * The meta object literal for the '{@link javaMM.impl.InterfaceDeclarationImpl <em>Interface Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.InterfaceDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getInterfaceDeclaration()
		 * @generated
		 */
		EClass INTERFACE_DECLARATION = eINSTANCE.getInterfaceDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.JavadocImpl <em>Javadoc</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.JavadocImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getJavadoc()
		 * @generated
		 */
		EClass JAVADOC = eINSTANCE.getJavadoc();

		/**
		 * The meta object literal for the '<em><b>Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference JAVADOC__TAGS = eINSTANCE.getJavadoc_Tags();

		/**
		 * The meta object literal for the '{@link javaMM.impl.LabeledStatementImpl <em>Labeled Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.LabeledStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getLabeledStatement()
		 * @generated
		 */
		EClass LABELED_STATEMENT = eINSTANCE.getLabeledStatement();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LABELED_STATEMENT__BODY = eINSTANCE.getLabeledStatement_Body();

		/**
		 * The meta object literal for the '<em><b>Usages In Break Statements</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LABELED_STATEMENT__USAGES_IN_BREAK_STATEMENTS = eINSTANCE.getLabeledStatement_UsagesInBreakStatements();

		/**
		 * The meta object literal for the '<em><b>Usages In Continue Statements</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LABELED_STATEMENT__USAGES_IN_CONTINUE_STATEMENTS = eINSTANCE.getLabeledStatement_UsagesInContinueStatements();

		/**
		 * The meta object literal for the '{@link javaMM.impl.LineCommentImpl <em>Line Comment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.LineCommentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getLineComment()
		 * @generated
		 */
		EClass LINE_COMMENT = eINSTANCE.getLineComment();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ManifestImpl <em>Manifest</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ManifestImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getManifest()
		 * @generated
		 */
		EClass MANIFEST = eINSTANCE.getManifest();

		/**
		 * The meta object literal for the '<em><b>Main Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MANIFEST__MAIN_ATTRIBUTES = eINSTANCE.getManifest_MainAttributes();

		/**
		 * The meta object literal for the '<em><b>Entry Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MANIFEST__ENTRY_ATTRIBUTES = eINSTANCE.getManifest_EntryAttributes();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ManifestAttributeImpl <em>Manifest Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ManifestAttributeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getManifestAttribute()
		 * @generated
		 */
		EClass MANIFEST_ATTRIBUTE = eINSTANCE.getManifestAttribute();

		/**
		 * The meta object literal for the '<em><b>Key</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MANIFEST_ATTRIBUTE__KEY = eINSTANCE.getManifestAttribute_Key();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MANIFEST_ATTRIBUTE__VALUE = eINSTANCE.getManifestAttribute_Value();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ManifestEntryImpl <em>Manifest Entry</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ManifestEntryImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getManifestEntry()
		 * @generated
		 */
		EClass MANIFEST_ENTRY = eINSTANCE.getManifestEntry();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MANIFEST_ENTRY__NAME = eINSTANCE.getManifestEntry_Name();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MANIFEST_ENTRY__ATTRIBUTES = eINSTANCE.getManifestEntry_Attributes();

		/**
		 * The meta object literal for the '{@link javaMM.impl.MemberRefImpl <em>Member Ref</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.MemberRefImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getMemberRef()
		 * @generated
		 */
		EClass MEMBER_REF = eINSTANCE.getMemberRef();

		/**
		 * The meta object literal for the '<em><b>Member</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_REF__MEMBER = eINSTANCE.getMemberRef_Member();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_REF__QUALIFIER = eINSTANCE.getMemberRef_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.MethodDeclarationImpl <em>Method Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.MethodDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getMethodDeclaration()
		 * @generated
		 */
		EClass METHOD_DECLARATION = eINSTANCE.getMethodDeclaration();

		/**
		 * The meta object literal for the '<em><b>Extra Array Dimensions</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS = eINSTANCE.getMethodDeclaration_ExtraArrayDimensions();

		/**
		 * The meta object literal for the '<em><b>Return Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_DECLARATION__RETURN_TYPE = eINSTANCE.getMethodDeclaration_ReturnType();

		/**
		 * The meta object literal for the '<em><b>Redefined Method Declaration</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION = eINSTANCE.getMethodDeclaration_RedefinedMethodDeclaration();

		/**
		 * The meta object literal for the '<em><b>Redefinitions</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_DECLARATION__REDEFINITIONS = eINSTANCE.getMethodDeclaration_Redefinitions();

		/**
		 * The meta object literal for the '<em><b>Should Start With Lower Case</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation METHOD_DECLARATION___SHOULD_START_WITH_LOWER_CASE__DIAGNOSTICCHAIN_MAP = eINSTANCE.getMethodDeclaration__ShouldStartWithLowerCase__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.MethodInvocationImpl <em>Method Invocation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.MethodInvocationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getMethodInvocation()
		 * @generated
		 */
		EClass METHOD_INVOCATION = eINSTANCE.getMethodInvocation();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_INVOCATION__EXPRESSION = eINSTANCE.getMethodInvocation_Expression();

		/**
		 * The meta object literal for the '<em><b>Does Not Call Run Finalizers</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation METHOD_INVOCATION___DOES_NOT_CALL_RUN_FINALIZERS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getMethodInvocation__DoesNotCallRunFinalizers__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Does Not Call Finalize</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation METHOD_INVOCATION___DOES_NOT_CALL_FINALIZE__DIAGNOSTICCHAIN_MAP = eINSTANCE.getMethodInvocation__DoesNotCallFinalize__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '<em><b>Does Not Call Exit</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation METHOD_INVOCATION___DOES_NOT_CALL_EXIT__DIAGNOSTICCHAIN_MAP = eINSTANCE.getMethodInvocation__DoesNotCallExit__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.MethodRefImpl <em>Method Ref</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.MethodRefImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getMethodRef()
		 * @generated
		 */
		EClass METHOD_REF = eINSTANCE.getMethodRef();

		/**
		 * The meta object literal for the '<em><b>Method</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_REF__METHOD = eINSTANCE.getMethodRef_Method();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_REF__QUALIFIER = eINSTANCE.getMethodRef_Qualifier();

		/**
		 * The meta object literal for the '<em><b>Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_REF__PARAMETERS = eINSTANCE.getMethodRef_Parameters();

		/**
		 * The meta object literal for the '{@link javaMM.impl.MethodRefParameterImpl <em>Method Ref Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.MethodRefParameterImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getMethodRefParameter()
		 * @generated
		 */
		EClass METHOD_REF_PARAMETER = eINSTANCE.getMethodRefParameter();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute METHOD_REF_PARAMETER__NAME = eINSTANCE.getMethodRefParameter_Name();

		/**
		 * The meta object literal for the '<em><b>Varargs</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute METHOD_REF_PARAMETER__VARARGS = eINSTANCE.getMethodRefParameter_Varargs();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference METHOD_REF_PARAMETER__TYPE = eINSTANCE.getMethodRefParameter_Type();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ModelImpl <em>Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ModelImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getModel()
		 * @generated
		 */
		EClass MODEL = eINSTANCE.getModel();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODEL__NAME = eINSTANCE.getModel_Name();

		/**
		 * The meta object literal for the '<em><b>Owned Elements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__OWNED_ELEMENTS = eINSTANCE.getModel_OwnedElements();

		/**
		 * The meta object literal for the '<em><b>Orphan Types</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__ORPHAN_TYPES = eINSTANCE.getModel_OrphanTypes();

		/**
		 * The meta object literal for the '<em><b>Unresolved Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__UNRESOLVED_ITEMS = eINSTANCE.getModel_UnresolvedItems();

		/**
		 * The meta object literal for the '<em><b>Compilation Units</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__COMPILATION_UNITS = eINSTANCE.getModel_CompilationUnits();

		/**
		 * The meta object literal for the '<em><b>Class Files</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__CLASS_FILES = eINSTANCE.getModel_ClassFiles();

		/**
		 * The meta object literal for the '<em><b>Archives</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODEL__ARCHIVES = eINSTANCE.getModel_Archives();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ModifierImpl <em>Modifier</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ModifierImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getModifier()
		 * @generated
		 */
		EClass MODIFIER = eINSTANCE.getModifier();

		/**
		 * The meta object literal for the '<em><b>Visibility</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__VISIBILITY = eINSTANCE.getModifier_Visibility();

		/**
		 * The meta object literal for the '<em><b>Inheritance</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__INHERITANCE = eINSTANCE.getModifier_Inheritance();

		/**
		 * The meta object literal for the '<em><b>Static</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__STATIC = eINSTANCE.getModifier_Static();

		/**
		 * The meta object literal for the '<em><b>Transient</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__TRANSIENT = eINSTANCE.getModifier_Transient();

		/**
		 * The meta object literal for the '<em><b>Volatile</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__VOLATILE = eINSTANCE.getModifier_Volatile();

		/**
		 * The meta object literal for the '<em><b>Native</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__NATIVE = eINSTANCE.getModifier_Native();

		/**
		 * The meta object literal for the '<em><b>Strictfp</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__STRICTFP = eINSTANCE.getModifier_Strictfp();

		/**
		 * The meta object literal for the '<em><b>Synchronized</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODIFIER__SYNCHRONIZED = eINSTANCE.getModifier_Synchronized();

		/**
		 * The meta object literal for the '<em><b>Body Declaration</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODIFIER__BODY_DECLARATION = eINSTANCE.getModifier_BodyDeclaration();

		/**
		 * The meta object literal for the '<em><b>Single Variable Declaration</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODIFIER__SINGLE_VARIABLE_DECLARATION = eINSTANCE.getModifier_SingleVariableDeclaration();

		/**
		 * The meta object literal for the '<em><b>Variable Declaration Statement</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODIFIER__VARIABLE_DECLARATION_STATEMENT = eINSTANCE.getModifier_VariableDeclarationStatement();

		/**
		 * The meta object literal for the '<em><b>Variable Declaration Expression</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODIFIER__VARIABLE_DECLARATION_EXPRESSION = eINSTANCE.getModifier_VariableDeclarationExpression();

		/**
		 * The meta object literal for the '<em><b>Is Local</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation MODIFIER___IS_LOCAL = eINSTANCE.getModifier__IsLocal();

		/**
		 * The meta object literal for the '{@link javaMM.impl.NamedElementImpl <em>Named Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.NamedElementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getNamedElement()
		 * @generated
		 */
		EClass NAMED_ELEMENT = eINSTANCE.getNamedElement();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NAMED_ELEMENT__NAME = eINSTANCE.getNamedElement_Name();

		/**
		 * The meta object literal for the '<em><b>Proxy</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NAMED_ELEMENT__PROXY = eINSTANCE.getNamedElement_Proxy();

		/**
		 * The meta object literal for the '<em><b>Usages In Imports</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference NAMED_ELEMENT__USAGES_IN_IMPORTS = eINSTANCE.getNamedElement_UsagesInImports();

		/**
		 * The meta object literal for the '{@link javaMM.impl.NamespaceAccessImpl <em>Namespace Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.NamespaceAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getNamespaceAccess()
		 * @generated
		 */
		EClass NAMESPACE_ACCESS = eINSTANCE.getNamespaceAccess();

		/**
		 * The meta object literal for the '{@link javaMM.impl.NumberLiteralImpl <em>Number Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.NumberLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getNumberLiteral()
		 * @generated
		 */
		EClass NUMBER_LITERAL = eINSTANCE.getNumberLiteral();

		/**
		 * The meta object literal for the '<em><b>Token Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NUMBER_LITERAL__TOKEN_VALUE = eINSTANCE.getNumberLiteral_TokenValue();

		/**
		 * The meta object literal for the '{@link javaMM.impl.NullLiteralImpl <em>Null Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.NullLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getNullLiteral()
		 * @generated
		 */
		EClass NULL_LITERAL = eINSTANCE.getNullLiteral();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PackageImpl <em>Package</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PackageImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPackage()
		 * @generated
		 */
		EClass PACKAGE = eINSTANCE.getPackage();

		/**
		 * The meta object literal for the '<em><b>Owned Elements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE__OWNED_ELEMENTS = eINSTANCE.getPackage_OwnedElements();

		/**
		 * The meta object literal for the '<em><b>Model</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE__MODEL = eINSTANCE.getPackage_Model();

		/**
		 * The meta object literal for the '<em><b>Owned Packages</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE__OWNED_PACKAGES = eINSTANCE.getPackage_OwnedPackages();

		/**
		 * The meta object literal for the '<em><b>Package</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE__PACKAGE = eINSTANCE.getPackage_Package();

		/**
		 * The meta object literal for the '<em><b>Usages In Package Access</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE__USAGES_IN_PACKAGE_ACCESS = eINSTANCE.getPackage_UsagesInPackageAccess();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PackageAccessImpl <em>Package Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PackageAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPackageAccess()
		 * @generated
		 */
		EClass PACKAGE_ACCESS = eINSTANCE.getPackageAccess();

		/**
		 * The meta object literal for the '<em><b>Package</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE_ACCESS__PACKAGE = eINSTANCE.getPackageAccess_Package();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PACKAGE_ACCESS__QUALIFIER = eINSTANCE.getPackageAccess_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ParameterizedTypeImpl <em>Parameterized Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ParameterizedTypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getParameterizedType()
		 * @generated
		 */
		EClass PARAMETERIZED_TYPE = eINSTANCE.getParameterizedType();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PARAMETERIZED_TYPE__TYPE = eINSTANCE.getParameterizedType_Type();

		/**
		 * The meta object literal for the '<em><b>Type Arguments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PARAMETERIZED_TYPE__TYPE_ARGUMENTS = eINSTANCE.getParameterizedType_TypeArguments();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ParenthesizedExpressionImpl <em>Parenthesized Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ParenthesizedExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getParenthesizedExpression()
		 * @generated
		 */
		EClass PARENTHESIZED_EXPRESSION = eINSTANCE.getParenthesizedExpression();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PARENTHESIZED_EXPRESSION__EXPRESSION = eINSTANCE.getParenthesizedExpression_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PostfixExpressionImpl <em>Postfix Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PostfixExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPostfixExpression()
		 * @generated
		 */
		EClass POSTFIX_EXPRESSION = eINSTANCE.getPostfixExpression();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute POSTFIX_EXPRESSION__OPERATOR = eINSTANCE.getPostfixExpression_Operator();

		/**
		 * The meta object literal for the '<em><b>Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference POSTFIX_EXPRESSION__OPERAND = eINSTANCE.getPostfixExpression_Operand();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrefixExpressionImpl <em>Prefix Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrefixExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrefixExpression()
		 * @generated
		 */
		EClass PREFIX_EXPRESSION = eINSTANCE.getPrefixExpression();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PREFIX_EXPRESSION__OPERATOR = eINSTANCE.getPrefixExpression_Operator();

		/**
		 * The meta object literal for the '<em><b>Operand</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PREFIX_EXPRESSION__OPERAND = eINSTANCE.getPrefixExpression_Operand();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeImpl <em>Primitive Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveType()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE = eINSTANCE.getPrimitiveType();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeBooleanImpl <em>Primitive Type Boolean</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeBooleanImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeBoolean()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_BOOLEAN = eINSTANCE.getPrimitiveTypeBoolean();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeByteImpl <em>Primitive Type Byte</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeByteImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeByte()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_BYTE = eINSTANCE.getPrimitiveTypeByte();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeCharImpl <em>Primitive Type Char</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeCharImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeChar()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_CHAR = eINSTANCE.getPrimitiveTypeChar();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeDoubleImpl <em>Primitive Type Double</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeDoubleImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeDouble()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_DOUBLE = eINSTANCE.getPrimitiveTypeDouble();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeShortImpl <em>Primitive Type Short</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeShortImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeShort()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_SHORT = eINSTANCE.getPrimitiveTypeShort();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeFloatImpl <em>Primitive Type Float</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeFloatImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeFloat()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_FLOAT = eINSTANCE.getPrimitiveTypeFloat();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeIntImpl <em>Primitive Type Int</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeIntImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeInt()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_INT = eINSTANCE.getPrimitiveTypeInt();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeLongImpl <em>Primitive Type Long</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeLongImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeLong()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_LONG = eINSTANCE.getPrimitiveTypeLong();

		/**
		 * The meta object literal for the '{@link javaMM.impl.PrimitiveTypeVoidImpl <em>Primitive Type Void</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.PrimitiveTypeVoidImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getPrimitiveTypeVoid()
		 * @generated
		 */
		EClass PRIMITIVE_TYPE_VOID = eINSTANCE.getPrimitiveTypeVoid();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ReturnStatementImpl <em>Return Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ReturnStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getReturnStatement()
		 * @generated
		 */
		EClass RETURN_STATEMENT = eINSTANCE.getReturnStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RETURN_STATEMENT__EXPRESSION = eINSTANCE.getReturnStatement_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SingleVariableAccessImpl <em>Single Variable Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SingleVariableAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSingleVariableAccess()
		 * @generated
		 */
		EClass SINGLE_VARIABLE_ACCESS = eINSTANCE.getSingleVariableAccess();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_ACCESS__VARIABLE = eINSTANCE.getSingleVariableAccess_Variable();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_ACCESS__QUALIFIER = eINSTANCE.getSingleVariableAccess_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SingleVariableDeclarationImpl <em>Single Variable Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SingleVariableDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSingleVariableDeclaration()
		 * @generated
		 */
		EClass SINGLE_VARIABLE_DECLARATION = eINSTANCE.getSingleVariableDeclaration();

		/**
		 * The meta object literal for the '<em><b>Modifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__MODIFIER = eINSTANCE.getSingleVariableDeclaration_Modifier();

		/**
		 * The meta object literal for the '<em><b>Varargs</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SINGLE_VARIABLE_DECLARATION__VARARGS = eINSTANCE.getSingleVariableDeclaration_Varargs();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__TYPE = eINSTANCE.getSingleVariableDeclaration_Type();

		/**
		 * The meta object literal for the '<em><b>Annotations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__ANNOTATIONS = eINSTANCE.getSingleVariableDeclaration_Annotations();

		/**
		 * The meta object literal for the '<em><b>Method Declaration</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION = eINSTANCE.getSingleVariableDeclaration_MethodDeclaration();

		/**
		 * The meta object literal for the '<em><b>Catch Clause</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE = eINSTANCE.getSingleVariableDeclaration_CatchClause();

		/**
		 * The meta object literal for the '<em><b>Enhanced For Statement</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT = eINSTANCE.getSingleVariableDeclaration_EnhancedForStatement();

		/**
		 * The meta object literal for the '{@link javaMM.impl.StatementImpl <em>Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.StatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getStatement()
		 * @generated
		 */
		EClass STATEMENT = eINSTANCE.getStatement();

		/**
		 * The meta object literal for the '{@link javaMM.impl.StringLiteralImpl <em>String Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.StringLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getStringLiteral()
		 * @generated
		 */
		EClass STRING_LITERAL = eINSTANCE.getStringLiteral();

		/**
		 * The meta object literal for the '<em><b>Escaped Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute STRING_LITERAL__ESCAPED_VALUE = eINSTANCE.getStringLiteral_EscapedValue();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SuperConstructorInvocationImpl <em>Super Constructor Invocation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SuperConstructorInvocationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSuperConstructorInvocation()
		 * @generated
		 */
		EClass SUPER_CONSTRUCTOR_INVOCATION = eINSTANCE.getSuperConstructorInvocation();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUPER_CONSTRUCTOR_INVOCATION__EXPRESSION = eINSTANCE.getSuperConstructorInvocation_Expression();

		/**
		 * The meta object literal for the '<em><b>No Redundant Super Call</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation SUPER_CONSTRUCTOR_INVOCATION___NO_REDUNDANT_SUPER_CALL__DIAGNOSTICCHAIN_MAP = eINSTANCE.getSuperConstructorInvocation__NoRedundantSuperCall__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SuperFieldAccessImpl <em>Super Field Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SuperFieldAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSuperFieldAccess()
		 * @generated
		 */
		EClass SUPER_FIELD_ACCESS = eINSTANCE.getSuperFieldAccess();

		/**
		 * The meta object literal for the '<em><b>Field</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUPER_FIELD_ACCESS__FIELD = eINSTANCE.getSuperFieldAccess_Field();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SuperMethodInvocationImpl <em>Super Method Invocation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SuperMethodInvocationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSuperMethodInvocation()
		 * @generated
		 */
		EClass SUPER_METHOD_INVOCATION = eINSTANCE.getSuperMethodInvocation();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SwitchCaseImpl <em>Switch Case</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SwitchCaseImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSwitchCase()
		 * @generated
		 */
		EClass SWITCH_CASE = eINSTANCE.getSwitchCase();

		/**
		 * The meta object literal for the '<em><b>Default</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SWITCH_CASE__DEFAULT = eINSTANCE.getSwitchCase_Default();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SWITCH_CASE__EXPRESSION = eINSTANCE.getSwitchCase_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SwitchStatementImpl <em>Switch Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SwitchStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSwitchStatement()
		 * @generated
		 */
		EClass SWITCH_STATEMENT = eINSTANCE.getSwitchStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SWITCH_STATEMENT__EXPRESSION = eINSTANCE.getSwitchStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Statements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SWITCH_STATEMENT__STATEMENTS = eINSTANCE.getSwitchStatement_Statements();

		/**
		 * The meta object literal for the '<em><b>More Than3 Cases</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation SWITCH_STATEMENT___MORE_THAN3_CASES__DIAGNOSTICCHAIN_MAP = eINSTANCE.getSwitchStatement__MoreThan3Cases__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.SynchronizedStatementImpl <em>Synchronized Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.SynchronizedStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getSynchronizedStatement()
		 * @generated
		 */
		EClass SYNCHRONIZED_STATEMENT = eINSTANCE.getSynchronizedStatement();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SYNCHRONIZED_STATEMENT__BODY = eINSTANCE.getSynchronizedStatement_Body();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SYNCHRONIZED_STATEMENT__EXPRESSION = eINSTANCE.getSynchronizedStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Has Statements</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation SYNCHRONIZED_STATEMENT___HAS_STATEMENTS__DIAGNOSTICCHAIN_MAP = eINSTANCE.getSynchronizedStatement__HasStatements__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TagElementImpl <em>Tag Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TagElementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTagElement()
		 * @generated
		 */
		EClass TAG_ELEMENT = eINSTANCE.getTagElement();

		/**
		 * The meta object literal for the '<em><b>Tag Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TAG_ELEMENT__TAG_NAME = eINSTANCE.getTagElement_TagName();

		/**
		 * The meta object literal for the '<em><b>Fragments</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TAG_ELEMENT__FRAGMENTS = eINSTANCE.getTagElement_Fragments();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TextElementImpl <em>Text Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TextElementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTextElement()
		 * @generated
		 */
		EClass TEXT_ELEMENT = eINSTANCE.getTextElement();

		/**
		 * The meta object literal for the '<em><b>Text</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEXT_ELEMENT__TEXT = eINSTANCE.getTextElement_Text();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ThisExpressionImpl <em>This Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ThisExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getThisExpression()
		 * @generated
		 */
		EClass THIS_EXPRESSION = eINSTANCE.getThisExpression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.ThrowStatementImpl <em>Throw Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.ThrowStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getThrowStatement()
		 * @generated
		 */
		EClass THROW_STATEMENT = eINSTANCE.getThrowStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference THROW_STATEMENT__EXPRESSION = eINSTANCE.getThrowStatement_Expression();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TryStatementImpl <em>Try Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TryStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTryStatement()
		 * @generated
		 */
		EClass TRY_STATEMENT = eINSTANCE.getTryStatement();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TRY_STATEMENT__BODY = eINSTANCE.getTryStatement_Body();

		/**
		 * The meta object literal for the '<em><b>Finally</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TRY_STATEMENT__FINALLY = eINSTANCE.getTryStatement_Finally();

		/**
		 * The meta object literal for the '<em><b>Catch Clauses</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TRY_STATEMENT__CATCH_CLAUSES = eINSTANCE.getTryStatement_CatchClauses();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeImpl <em>Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getType()
		 * @generated
		 */
		EClass TYPE = eINSTANCE.getType();

		/**
		 * The meta object literal for the '<em><b>Usages In Type Access</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE__USAGES_IN_TYPE_ACCESS = eINSTANCE.getType_UsagesInTypeAccess();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeAccessImpl <em>Type Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTypeAccess()
		 * @generated
		 */
		EClass TYPE_ACCESS = eINSTANCE.getTypeAccess();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_ACCESS__TYPE = eINSTANCE.getTypeAccess_Type();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_ACCESS__QUALIFIER = eINSTANCE.getTypeAccess_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeDeclarationImpl <em>Type Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTypeDeclaration()
		 * @generated
		 */
		EClass TYPE_DECLARATION = eINSTANCE.getTypeDeclaration();

		/**
		 * The meta object literal for the '<em><b>Type Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_DECLARATION__TYPE_PARAMETERS = eINSTANCE.getTypeDeclaration_TypeParameters();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeDeclarationStatementImpl <em>Type Declaration Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeDeclarationStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTypeDeclarationStatement()
		 * @generated
		 */
		EClass TYPE_DECLARATION_STATEMENT = eINSTANCE.getTypeDeclarationStatement();

		/**
		 * The meta object literal for the '<em><b>Declaration</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_DECLARATION_STATEMENT__DECLARATION = eINSTANCE.getTypeDeclarationStatement_Declaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeLiteralImpl <em>Type Literal</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeLiteralImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTypeLiteral()
		 * @generated
		 */
		EClass TYPE_LITERAL = eINSTANCE.getTypeLiteral();

		/**
		 * The meta object literal for the '<em><b>Type</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_LITERAL__TYPE = eINSTANCE.getTypeLiteral_Type();

		/**
		 * The meta object literal for the '{@link javaMM.impl.TypeParameterImpl <em>Type Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.TypeParameterImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getTypeParameter()
		 * @generated
		 */
		EClass TYPE_PARAMETER = eINSTANCE.getTypeParameter();

		/**
		 * The meta object literal for the '<em><b>Bounds</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPE_PARAMETER__BOUNDS = eINSTANCE.getTypeParameter_Bounds();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedItemImpl <em>Unresolved Item</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedItemImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedItem()
		 * @generated
		 */
		EClass UNRESOLVED_ITEM = eINSTANCE.getUnresolvedItem();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedItemAccessImpl <em>Unresolved Item Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedItemAccessImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedItemAccess()
		 * @generated
		 */
		EClass UNRESOLVED_ITEM_ACCESS = eINSTANCE.getUnresolvedItemAccess();

		/**
		 * The meta object literal for the '<em><b>Element</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference UNRESOLVED_ITEM_ACCESS__ELEMENT = eINSTANCE.getUnresolvedItemAccess_Element();

		/**
		 * The meta object literal for the '<em><b>Qualifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference UNRESOLVED_ITEM_ACCESS__QUALIFIER = eINSTANCE.getUnresolvedItemAccess_Qualifier();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedAnnotationDeclarationImpl <em>Unresolved Annotation Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedAnnotationDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedAnnotationDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_ANNOTATION_DECLARATION = eINSTANCE.getUnresolvedAnnotationDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedAnnotationTypeMemberDeclarationImpl <em>Unresolved Annotation Type Member Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedAnnotationTypeMemberDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedAnnotationTypeMemberDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION = eINSTANCE.getUnresolvedAnnotationTypeMemberDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedClassDeclarationImpl <em>Unresolved Class Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedClassDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedClassDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_CLASS_DECLARATION = eINSTANCE.getUnresolvedClassDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedEnumDeclarationImpl <em>Unresolved Enum Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedEnumDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedEnumDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_ENUM_DECLARATION = eINSTANCE.getUnresolvedEnumDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedInterfaceDeclarationImpl <em>Unresolved Interface Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedInterfaceDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedInterfaceDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_INTERFACE_DECLARATION = eINSTANCE.getUnresolvedInterfaceDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedLabeledStatementImpl <em>Unresolved Labeled Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedLabeledStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedLabeledStatement()
		 * @generated
		 */
		EClass UNRESOLVED_LABELED_STATEMENT = eINSTANCE.getUnresolvedLabeledStatement();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedMethodDeclarationImpl <em>Unresolved Method Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedMethodDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedMethodDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_METHOD_DECLARATION = eINSTANCE.getUnresolvedMethodDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedSingleVariableDeclarationImpl <em>Unresolved Single Variable Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedSingleVariableDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedSingleVariableDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_SINGLE_VARIABLE_DECLARATION = eINSTANCE.getUnresolvedSingleVariableDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedTypeImpl <em>Unresolved Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedTypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedType()
		 * @generated
		 */
		EClass UNRESOLVED_TYPE = eINSTANCE.getUnresolvedType();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedTypeDeclarationImpl <em>Unresolved Type Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedTypeDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedTypeDeclaration()
		 * @generated
		 */
		EClass UNRESOLVED_TYPE_DECLARATION = eINSTANCE.getUnresolvedTypeDeclaration();

		/**
		 * The meta object literal for the '{@link javaMM.impl.UnresolvedVariableDeclarationFragmentImpl <em>Unresolved Variable Declaration Fragment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.UnresolvedVariableDeclarationFragmentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getUnresolvedVariableDeclarationFragment()
		 * @generated
		 */
		EClass UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT = eINSTANCE.getUnresolvedVariableDeclarationFragment();

		/**
		 * The meta object literal for the '{@link javaMM.impl.VariableDeclarationImpl <em>Variable Declaration</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.VariableDeclarationImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclaration()
		 * @generated
		 */
		EClass VARIABLE_DECLARATION = eINSTANCE.getVariableDeclaration();

		/**
		 * The meta object literal for the '<em><b>Extra Array Dimensions</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_DECLARATION__EXTRA_ARRAY_DIMENSIONS = eINSTANCE.getVariableDeclaration_ExtraArrayDimensions();

		/**
		 * The meta object literal for the '<em><b>Initializer</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION__INITIALIZER = eINSTANCE.getVariableDeclaration_Initializer();

		/**
		 * The meta object literal for the '<em><b>Usage In Variable Access</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION__USAGE_IN_VARIABLE_ACCESS = eINSTANCE.getVariableDeclaration_UsageInVariableAccess();

		/**
		 * The meta object literal for the '<em><b>Variable Is Used</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation VARIABLE_DECLARATION___VARIABLE_IS_USED__DIAGNOSTICCHAIN_MAP = eINSTANCE.getVariableDeclaration__VariableIsUsed__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.VariableDeclarationExpressionImpl <em>Variable Declaration Expression</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.VariableDeclarationExpressionImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationExpression()
		 * @generated
		 */
		EClass VARIABLE_DECLARATION_EXPRESSION = eINSTANCE.getVariableDeclarationExpression();

		/**
		 * The meta object literal for the '<em><b>Modifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION_EXPRESSION__MODIFIER = eINSTANCE.getVariableDeclarationExpression_Modifier();

		/**
		 * The meta object literal for the '<em><b>Annotations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS = eINSTANCE.getVariableDeclarationExpression_Annotations();

		/**
		 * The meta object literal for the '<em><b>Public Variable Is Final</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation VARIABLE_DECLARATION_EXPRESSION___PUBLIC_VARIABLE_IS_FINAL__DIAGNOSTICCHAIN_MAP = eINSTANCE.getVariableDeclarationExpression__PublicVariableIsFinal__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.VariableDeclarationFragmentImpl <em>Variable Declaration Fragment</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.VariableDeclarationFragmentImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationFragment()
		 * @generated
		 */
		EClass VARIABLE_DECLARATION_FRAGMENT = eINSTANCE.getVariableDeclarationFragment();

		/**
		 * The meta object literal for the '<em><b>Variables Container</b></em>' container reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION_FRAGMENT__VARIABLES_CONTAINER = eINSTANCE.getVariableDeclarationFragment_VariablesContainer();

		/**
		 * The meta object literal for the '{@link javaMM.impl.VariableDeclarationStatementImpl <em>Variable Declaration Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.VariableDeclarationStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getVariableDeclarationStatement()
		 * @generated
		 */
		EClass VARIABLE_DECLARATION_STATEMENT = eINSTANCE.getVariableDeclarationStatement();

		/**
		 * The meta object literal for the '<em><b>Extra Array Dimensions</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_DECLARATION_STATEMENT__EXTRA_ARRAY_DIMENSIONS = eINSTANCE.getVariableDeclarationStatement_ExtraArrayDimensions();

		/**
		 * The meta object literal for the '<em><b>Modifier</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION_STATEMENT__MODIFIER = eINSTANCE.getVariableDeclarationStatement_Modifier();

		/**
		 * The meta object literal for the '<em><b>Annotations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_DECLARATION_STATEMENT__ANNOTATIONS = eINSTANCE.getVariableDeclarationStatement_Annotations();

		/**
		 * The meta object literal for the '<em><b>Public Variable Is Final</b></em>' operation.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EOperation VARIABLE_DECLARATION_STATEMENT___PUBLIC_VARIABLE_IS_FINAL__DIAGNOSTICCHAIN_MAP = eINSTANCE.getVariableDeclarationStatement__PublicVariableIsFinal__DiagnosticChain_Map();

		/**
		 * The meta object literal for the '{@link javaMM.impl.WildCardTypeImpl <em>Wild Card Type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.WildCardTypeImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getWildCardType()
		 * @generated
		 */
		EClass WILD_CARD_TYPE = eINSTANCE.getWildCardType();

		/**
		 * The meta object literal for the '<em><b>Upper Bound</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute WILD_CARD_TYPE__UPPER_BOUND = eINSTANCE.getWildCardType_UpperBound();

		/**
		 * The meta object literal for the '<em><b>Bound</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference WILD_CARD_TYPE__BOUND = eINSTANCE.getWildCardType_Bound();

		/**
		 * The meta object literal for the '{@link javaMM.impl.WhileStatementImpl <em>While Statement</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.impl.WhileStatementImpl
		 * @see javaMM.impl.JavaMMPackageImpl#getWhileStatement()
		 * @generated
		 */
		EClass WHILE_STATEMENT = eINSTANCE.getWhileStatement();

		/**
		 * The meta object literal for the '<em><b>Expression</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference WHILE_STATEMENT__EXPRESSION = eINSTANCE.getWhileStatement_Expression();

		/**
		 * The meta object literal for the '<em><b>Body</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference WHILE_STATEMENT__BODY = eINSTANCE.getWhileStatement_Body();

		/**
		 * The meta object literal for the '{@link javaMM.AssignmentKind <em>Assignment Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.AssignmentKind
		 * @see javaMM.impl.JavaMMPackageImpl#getAssignmentKind()
		 * @generated
		 */
		EEnum ASSIGNMENT_KIND = eINSTANCE.getAssignmentKind();

		/**
		 * The meta object literal for the '{@link javaMM.InfixExpressionKind <em>Infix Expression Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.InfixExpressionKind
		 * @see javaMM.impl.JavaMMPackageImpl#getInfixExpressionKind()
		 * @generated
		 */
		EEnum INFIX_EXPRESSION_KIND = eINSTANCE.getInfixExpressionKind();

		/**
		 * The meta object literal for the '{@link javaMM.InheritanceKind <em>Inheritance Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.InheritanceKind
		 * @see javaMM.impl.JavaMMPackageImpl#getInheritanceKind()
		 * @generated
		 */
		EEnum INHERITANCE_KIND = eINSTANCE.getInheritanceKind();

		/**
		 * The meta object literal for the '{@link javaMM.PostfixExpressionKind <em>Postfix Expression Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.PostfixExpressionKind
		 * @see javaMM.impl.JavaMMPackageImpl#getPostfixExpressionKind()
		 * @generated
		 */
		EEnum POSTFIX_EXPRESSION_KIND = eINSTANCE.getPostfixExpressionKind();

		/**
		 * The meta object literal for the '{@link javaMM.PrefixExpressionKind <em>Prefix Expression Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.PrefixExpressionKind
		 * @see javaMM.impl.JavaMMPackageImpl#getPrefixExpressionKind()
		 * @generated
		 */
		EEnum PREFIX_EXPRESSION_KIND = eINSTANCE.getPrefixExpressionKind();

		/**
		 * The meta object literal for the '{@link javaMM.VisibilityKind <em>Visibility Kind</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see javaMM.VisibilityKind
		 * @see javaMM.impl.JavaMMPackageImpl#getVisibilityKind()
		 * @generated
		 */
		EEnum VISIBILITY_KIND = eINSTANCE.getVisibilityKind();

	}

} //JavaMMPackage
