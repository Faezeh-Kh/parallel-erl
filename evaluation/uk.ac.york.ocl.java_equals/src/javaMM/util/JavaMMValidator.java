/**
 */
package javaMM.util;

import java.util.Map;

import javaMM.ASTNode;
import javaMM.AbstractMethodDeclaration;
import javaMM.AbstractMethodInvocation;
import javaMM.AbstractTypeDeclaration;
import javaMM.AbstractTypeQualifiedExpression;
import javaMM.AbstractVariablesContainer;
import javaMM.Annotation;
import javaMM.AnnotationMemberValuePair;
import javaMM.AnnotationTypeDeclaration;
import javaMM.AnnotationTypeMemberDeclaration;
import javaMM.AnonymousClassDeclaration;
import javaMM.Archive;
import javaMM.ArrayAccess;
import javaMM.ArrayCreation;
import javaMM.ArrayInitializer;
import javaMM.ArrayLengthAccess;
import javaMM.ArrayType;
import javaMM.AssertStatement;
import javaMM.Assignment;
import javaMM.AssignmentKind;
import javaMM.Block;
import javaMM.BlockComment;
import javaMM.BodyDeclaration;
import javaMM.BooleanLiteral;
import javaMM.BreakStatement;
import javaMM.CastExpression;
import javaMM.CatchClause;
import javaMM.CharacterLiteral;
import javaMM.ClassDeclaration;
import javaMM.ClassFile;
import javaMM.ClassInstanceCreation;
import javaMM.Comment;
import javaMM.CompilationUnit;
import javaMM.ConditionalExpression;
import javaMM.ConstructorDeclaration;
import javaMM.ConstructorInvocation;
import javaMM.ContinueStatement;
import javaMM.DoStatement;
import javaMM.EmptyStatement;
import javaMM.EnhancedForStatement;
import javaMM.EnumConstantDeclaration;
import javaMM.EnumDeclaration;
import javaMM.Expression;
import javaMM.ExpressionStatement;
import javaMM.FieldAccess;
import javaMM.FieldDeclaration;
import javaMM.ForStatement;
import javaMM.IfStatement;
import javaMM.ImportDeclaration;
import javaMM.InfixExpression;
import javaMM.InfixExpressionKind;
import javaMM.InheritanceKind;
import javaMM.Initializer;
import javaMM.InstanceofExpression;
import javaMM.InterfaceDeclaration;
import javaMM.JavaMMPackage;
import javaMM.Javadoc;
import javaMM.LabeledStatement;
import javaMM.LineComment;
import javaMM.Manifest;
import javaMM.ManifestAttribute;
import javaMM.ManifestEntry;
import javaMM.MemberRef;
import javaMM.MethodDeclaration;
import javaMM.MethodInvocation;
import javaMM.MethodRef;
import javaMM.MethodRefParameter;
import javaMM.Model;
import javaMM.Modifier;
import javaMM.NamedElement;
import javaMM.NamespaceAccess;
import javaMM.NullLiteral;
import javaMM.NumberLiteral;
import javaMM.PackageAccess;
import javaMM.ParameterizedType;
import javaMM.ParenthesizedExpression;
import javaMM.PostfixExpression;
import javaMM.PostfixExpressionKind;
import javaMM.PrefixExpression;
import javaMM.PrefixExpressionKind;
import javaMM.PrimitiveType;
import javaMM.PrimitiveTypeBoolean;
import javaMM.PrimitiveTypeByte;
import javaMM.PrimitiveTypeChar;
import javaMM.PrimitiveTypeDouble;
import javaMM.PrimitiveTypeFloat;
import javaMM.PrimitiveTypeInt;
import javaMM.PrimitiveTypeLong;
import javaMM.PrimitiveTypeShort;
import javaMM.PrimitiveTypeVoid;
import javaMM.ReturnStatement;
import javaMM.SingleVariableAccess;
import javaMM.SingleVariableDeclaration;
import javaMM.Statement;
import javaMM.StringLiteral;
import javaMM.SuperConstructorInvocation;
import javaMM.SuperFieldAccess;
import javaMM.SuperMethodInvocation;
import javaMM.SwitchCase;
import javaMM.SwitchStatement;
import javaMM.SynchronizedStatement;
import javaMM.TagElement;
import javaMM.TextElement;
import javaMM.ThisExpression;
import javaMM.ThrowStatement;
import javaMM.TryStatement;
import javaMM.Type;
import javaMM.TypeAccess;
import javaMM.TypeDeclaration;
import javaMM.TypeDeclarationStatement;
import javaMM.TypeLiteral;
import javaMM.TypeParameter;
import javaMM.UnresolvedAnnotationDeclaration;
import javaMM.UnresolvedAnnotationTypeMemberDeclaration;
import javaMM.UnresolvedClassDeclaration;
import javaMM.UnresolvedEnumDeclaration;
import javaMM.UnresolvedInterfaceDeclaration;
import javaMM.UnresolvedItem;
import javaMM.UnresolvedItemAccess;
import javaMM.UnresolvedLabeledStatement;
import javaMM.UnresolvedMethodDeclaration;
import javaMM.UnresolvedSingleVariableDeclaration;
import javaMM.UnresolvedType;
import javaMM.UnresolvedTypeDeclaration;
import javaMM.UnresolvedVariableDeclarationFragment;
import javaMM.VariableDeclaration;
import javaMM.VariableDeclarationExpression;
import javaMM.VariableDeclarationFragment;
import javaMM.VariableDeclarationStatement;
import javaMM.VisibilityKind;
import javaMM.WhileStatement;
import javaMM.WildCardType;

import org.eclipse.emf.common.util.Diagnostic;
import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.EObjectValidator;

/**
 * <!-- begin-user-doc -->
 * The <b>Validator</b> for the model.
 * <!-- end-user-doc -->
 * @see javaMM.JavaMMPackage
 * @generated
 */
public class JavaMMValidator extends EObjectValidator {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static final JavaMMValidator INSTANCE = new JavaMMValidator();

	/**
	 * A constant for the {@link org.eclipse.emf.common.util.Diagnostic#getSource() source} of diagnostic {@link org.eclipse.emf.common.util.Diagnostic#getCode() codes} from this package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.common.util.Diagnostic#getSource()
	 * @see org.eclipse.emf.common.util.Diagnostic#getCode()
	 * @generated
	 */
	public static final String DIAGNOSTIC_SOURCE = "javaMM";

	/**
	 * A constant with a fixed name that can be used as the base value for additional hand written constants.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static final int GENERATED_DIAGNOSTIC_CODE_COUNT = 0;

	/**
	 * A constant with a fixed name that can be used as the base value for additional hand written constants in a derived class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static final int DIAGNOSTIC_CODE_COUNT = GENERATED_DIAGNOSTIC_CODE_COUNT;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public JavaMMValidator() {
		super();
	}

	/**
	 * Returns the package of this validator switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EPackage getEPackage() {
	  return JavaMMPackage.eINSTANCE;
	}

	/**
	 * Calls <code>validateXXX</code> for the corresponding classifier of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected boolean validate(int classifierID, Object value, DiagnosticChain diagnostics, Map<Object, Object> context) {
		switch (classifierID) {
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION:
				return validateAbstractMethodDeclaration((AbstractMethodDeclaration)value, diagnostics, context);
			case JavaMMPackage.ABSTRACT_METHOD_INVOCATION:
				return validateAbstractMethodInvocation((AbstractMethodInvocation)value, diagnostics, context);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION:
				return validateAbstractTypeDeclaration((AbstractTypeDeclaration)value, diagnostics, context);
			case JavaMMPackage.ABSTRACT_TYPE_QUALIFIED_EXPRESSION:
				return validateAbstractTypeQualifiedExpression((AbstractTypeQualifiedExpression)value, diagnostics, context);
			case JavaMMPackage.ABSTRACT_VARIABLES_CONTAINER:
				return validateAbstractVariablesContainer((AbstractVariablesContainer)value, diagnostics, context);
			case JavaMMPackage.ANNOTATION:
				return validateAnnotation((Annotation)value, diagnostics, context);
			case JavaMMPackage.ARCHIVE:
				return validateArchive((Archive)value, diagnostics, context);
			case JavaMMPackage.ASSERT_STATEMENT:
				return validateAssertStatement((AssertStatement)value, diagnostics, context);
			case JavaMMPackage.AST_NODE:
				return validateASTNode((ASTNode)value, diagnostics, context);
			case JavaMMPackage.ANNOTATION_MEMBER_VALUE_PAIR:
				return validateAnnotationMemberValuePair((AnnotationMemberValuePair)value, diagnostics, context);
			case JavaMMPackage.ANNOTATION_TYPE_DECLARATION:
				return validateAnnotationTypeDeclaration((AnnotationTypeDeclaration)value, diagnostics, context);
			case JavaMMPackage.ANNOTATION_TYPE_MEMBER_DECLARATION:
				return validateAnnotationTypeMemberDeclaration((AnnotationTypeMemberDeclaration)value, diagnostics, context);
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION:
				return validateAnonymousClassDeclaration((AnonymousClassDeclaration)value, diagnostics, context);
			case JavaMMPackage.ARRAY_ACCESS:
				return validateArrayAccess((ArrayAccess)value, diagnostics, context);
			case JavaMMPackage.ARRAY_CREATION:
				return validateArrayCreation((ArrayCreation)value, diagnostics, context);
			case JavaMMPackage.ARRAY_INITIALIZER:
				return validateArrayInitializer((ArrayInitializer)value, diagnostics, context);
			case JavaMMPackage.ARRAY_LENGTH_ACCESS:
				return validateArrayLengthAccess((ArrayLengthAccess)value, diagnostics, context);
			case JavaMMPackage.ARRAY_TYPE:
				return validateArrayType((ArrayType)value, diagnostics, context);
			case JavaMMPackage.ASSIGNMENT:
				return validateAssignment((Assignment)value, diagnostics, context);
			case JavaMMPackage.BODY_DECLARATION:
				return validateBodyDeclaration((BodyDeclaration)value, diagnostics, context);
			case JavaMMPackage.BOOLEAN_LITERAL:
				return validateBooleanLiteral((BooleanLiteral)value, diagnostics, context);
			case JavaMMPackage.BLOCK_COMMENT:
				return validateBlockComment((BlockComment)value, diagnostics, context);
			case JavaMMPackage.BLOCK:
				return validateBlock((Block)value, diagnostics, context);
			case JavaMMPackage.BREAK_STATEMENT:
				return validateBreakStatement((BreakStatement)value, diagnostics, context);
			case JavaMMPackage.CAST_EXPRESSION:
				return validateCastExpression((CastExpression)value, diagnostics, context);
			case JavaMMPackage.CATCH_CLAUSE:
				return validateCatchClause((CatchClause)value, diagnostics, context);
			case JavaMMPackage.CHARACTER_LITERAL:
				return validateCharacterLiteral((CharacterLiteral)value, diagnostics, context);
			case JavaMMPackage.CLASS_FILE:
				return validateClassFile((ClassFile)value, diagnostics, context);
			case JavaMMPackage.CLASS_INSTANCE_CREATION:
				return validateClassInstanceCreation((ClassInstanceCreation)value, diagnostics, context);
			case JavaMMPackage.CONSTRUCTOR_DECLARATION:
				return validateConstructorDeclaration((ConstructorDeclaration)value, diagnostics, context);
			case JavaMMPackage.CONDITIONAL_EXPRESSION:
				return validateConditionalExpression((ConditionalExpression)value, diagnostics, context);
			case JavaMMPackage.CONSTRUCTOR_INVOCATION:
				return validateConstructorInvocation((ConstructorInvocation)value, diagnostics, context);
			case JavaMMPackage.CLASS_DECLARATION:
				return validateClassDeclaration((ClassDeclaration)value, diagnostics, context);
			case JavaMMPackage.COMMENT:
				return validateComment((Comment)value, diagnostics, context);
			case JavaMMPackage.COMPILATION_UNIT:
				return validateCompilationUnit((CompilationUnit)value, diagnostics, context);
			case JavaMMPackage.CONTINUE_STATEMENT:
				return validateContinueStatement((ContinueStatement)value, diagnostics, context);
			case JavaMMPackage.DO_STATEMENT:
				return validateDoStatement((DoStatement)value, diagnostics, context);
			case JavaMMPackage.EMPTY_STATEMENT:
				return validateEmptyStatement((EmptyStatement)value, diagnostics, context);
			case JavaMMPackage.ENHANCED_FOR_STATEMENT:
				return validateEnhancedForStatement((EnhancedForStatement)value, diagnostics, context);
			case JavaMMPackage.ENUM_CONSTANT_DECLARATION:
				return validateEnumConstantDeclaration((EnumConstantDeclaration)value, diagnostics, context);
			case JavaMMPackage.ENUM_DECLARATION:
				return validateEnumDeclaration((EnumDeclaration)value, diagnostics, context);
			case JavaMMPackage.EXPRESSION:
				return validateExpression((Expression)value, diagnostics, context);
			case JavaMMPackage.EXPRESSION_STATEMENT:
				return validateExpressionStatement((ExpressionStatement)value, diagnostics, context);
			case JavaMMPackage.FIELD_ACCESS:
				return validateFieldAccess((FieldAccess)value, diagnostics, context);
			case JavaMMPackage.FIELD_DECLARATION:
				return validateFieldDeclaration((FieldDeclaration)value, diagnostics, context);
			case JavaMMPackage.FOR_STATEMENT:
				return validateForStatement((ForStatement)value, diagnostics, context);
			case JavaMMPackage.IF_STATEMENT:
				return validateIfStatement((IfStatement)value, diagnostics, context);
			case JavaMMPackage.IMPORT_DECLARATION:
				return validateImportDeclaration((ImportDeclaration)value, diagnostics, context);
			case JavaMMPackage.INFIX_EXPRESSION:
				return validateInfixExpression((InfixExpression)value, diagnostics, context);
			case JavaMMPackage.INITIALIZER:
				return validateInitializer((Initializer)value, diagnostics, context);
			case JavaMMPackage.INSTANCEOF_EXPRESSION:
				return validateInstanceofExpression((InstanceofExpression)value, diagnostics, context);
			case JavaMMPackage.INTERFACE_DECLARATION:
				return validateInterfaceDeclaration((InterfaceDeclaration)value, diagnostics, context);
			case JavaMMPackage.JAVADOC:
				return validateJavadoc((Javadoc)value, diagnostics, context);
			case JavaMMPackage.LABELED_STATEMENT:
				return validateLabeledStatement((LabeledStatement)value, diagnostics, context);
			case JavaMMPackage.LINE_COMMENT:
				return validateLineComment((LineComment)value, diagnostics, context);
			case JavaMMPackage.MANIFEST:
				return validateManifest((Manifest)value, diagnostics, context);
			case JavaMMPackage.MANIFEST_ATTRIBUTE:
				return validateManifestAttribute((ManifestAttribute)value, diagnostics, context);
			case JavaMMPackage.MANIFEST_ENTRY:
				return validateManifestEntry((ManifestEntry)value, diagnostics, context);
			case JavaMMPackage.MEMBER_REF:
				return validateMemberRef((MemberRef)value, diagnostics, context);
			case JavaMMPackage.METHOD_DECLARATION:
				return validateMethodDeclaration((MethodDeclaration)value, diagnostics, context);
			case JavaMMPackage.METHOD_INVOCATION:
				return validateMethodInvocation((MethodInvocation)value, diagnostics, context);
			case JavaMMPackage.METHOD_REF:
				return validateMethodRef((MethodRef)value, diagnostics, context);
			case JavaMMPackage.METHOD_REF_PARAMETER:
				return validateMethodRefParameter((MethodRefParameter)value, diagnostics, context);
			case JavaMMPackage.MODEL:
				return validateModel((Model)value, diagnostics, context);
			case JavaMMPackage.MODIFIER:
				return validateModifier((Modifier)value, diagnostics, context);
			case JavaMMPackage.NAMED_ELEMENT:
				return validateNamedElement((NamedElement)value, diagnostics, context);
			case JavaMMPackage.NAMESPACE_ACCESS:
				return validateNamespaceAccess((NamespaceAccess)value, diagnostics, context);
			case JavaMMPackage.NUMBER_LITERAL:
				return validateNumberLiteral((NumberLiteral)value, diagnostics, context);
			case JavaMMPackage.NULL_LITERAL:
				return validateNullLiteral((NullLiteral)value, diagnostics, context);
			case JavaMMPackage.PACKAGE:
				return validatePackage((javaMM.Package)value, diagnostics, context);
			case JavaMMPackage.PACKAGE_ACCESS:
				return validatePackageAccess((PackageAccess)value, diagnostics, context);
			case JavaMMPackage.PARAMETERIZED_TYPE:
				return validateParameterizedType((ParameterizedType)value, diagnostics, context);
			case JavaMMPackage.PARENTHESIZED_EXPRESSION:
				return validateParenthesizedExpression((ParenthesizedExpression)value, diagnostics, context);
			case JavaMMPackage.POSTFIX_EXPRESSION:
				return validatePostfixExpression((PostfixExpression)value, diagnostics, context);
			case JavaMMPackage.PREFIX_EXPRESSION:
				return validatePrefixExpression((PrefixExpression)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE:
				return validatePrimitiveType((PrimitiveType)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_BOOLEAN:
				return validatePrimitiveTypeBoolean((PrimitiveTypeBoolean)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_BYTE:
				return validatePrimitiveTypeByte((PrimitiveTypeByte)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_CHAR:
				return validatePrimitiveTypeChar((PrimitiveTypeChar)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_DOUBLE:
				return validatePrimitiveTypeDouble((PrimitiveTypeDouble)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_SHORT:
				return validatePrimitiveTypeShort((PrimitiveTypeShort)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_FLOAT:
				return validatePrimitiveTypeFloat((PrimitiveTypeFloat)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_INT:
				return validatePrimitiveTypeInt((PrimitiveTypeInt)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_LONG:
				return validatePrimitiveTypeLong((PrimitiveTypeLong)value, diagnostics, context);
			case JavaMMPackage.PRIMITIVE_TYPE_VOID:
				return validatePrimitiveTypeVoid((PrimitiveTypeVoid)value, diagnostics, context);
			case JavaMMPackage.RETURN_STATEMENT:
				return validateReturnStatement((ReturnStatement)value, diagnostics, context);
			case JavaMMPackage.SINGLE_VARIABLE_ACCESS:
				return validateSingleVariableAccess((SingleVariableAccess)value, diagnostics, context);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION:
				return validateSingleVariableDeclaration((SingleVariableDeclaration)value, diagnostics, context);
			case JavaMMPackage.STATEMENT:
				return validateStatement((Statement)value, diagnostics, context);
			case JavaMMPackage.STRING_LITERAL:
				return validateStringLiteral((StringLiteral)value, diagnostics, context);
			case JavaMMPackage.SUPER_CONSTRUCTOR_INVOCATION:
				return validateSuperConstructorInvocation((SuperConstructorInvocation)value, diagnostics, context);
			case JavaMMPackage.SUPER_FIELD_ACCESS:
				return validateSuperFieldAccess((SuperFieldAccess)value, diagnostics, context);
			case JavaMMPackage.SUPER_METHOD_INVOCATION:
				return validateSuperMethodInvocation((SuperMethodInvocation)value, diagnostics, context);
			case JavaMMPackage.SWITCH_CASE:
				return validateSwitchCase((SwitchCase)value, diagnostics, context);
			case JavaMMPackage.SWITCH_STATEMENT:
				return validateSwitchStatement((SwitchStatement)value, diagnostics, context);
			case JavaMMPackage.SYNCHRONIZED_STATEMENT:
				return validateSynchronizedStatement((SynchronizedStatement)value, diagnostics, context);
			case JavaMMPackage.TAG_ELEMENT:
				return validateTagElement((TagElement)value, diagnostics, context);
			case JavaMMPackage.TEXT_ELEMENT:
				return validateTextElement((TextElement)value, diagnostics, context);
			case JavaMMPackage.THIS_EXPRESSION:
				return validateThisExpression((ThisExpression)value, diagnostics, context);
			case JavaMMPackage.THROW_STATEMENT:
				return validateThrowStatement((ThrowStatement)value, diagnostics, context);
			case JavaMMPackage.TRY_STATEMENT:
				return validateTryStatement((TryStatement)value, diagnostics, context);
			case JavaMMPackage.TYPE:
				return validateType((Type)value, diagnostics, context);
			case JavaMMPackage.TYPE_ACCESS:
				return validateTypeAccess((TypeAccess)value, diagnostics, context);
			case JavaMMPackage.TYPE_DECLARATION:
				return validateTypeDeclaration((TypeDeclaration)value, diagnostics, context);
			case JavaMMPackage.TYPE_DECLARATION_STATEMENT:
				return validateTypeDeclarationStatement((TypeDeclarationStatement)value, diagnostics, context);
			case JavaMMPackage.TYPE_LITERAL:
				return validateTypeLiteral((TypeLiteral)value, diagnostics, context);
			case JavaMMPackage.TYPE_PARAMETER:
				return validateTypeParameter((TypeParameter)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_ITEM:
				return validateUnresolvedItem((UnresolvedItem)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_ITEM_ACCESS:
				return validateUnresolvedItemAccess((UnresolvedItemAccess)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_ANNOTATION_DECLARATION:
				return validateUnresolvedAnnotationDeclaration((UnresolvedAnnotationDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION:
				return validateUnresolvedAnnotationTypeMemberDeclaration((UnresolvedAnnotationTypeMemberDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_CLASS_DECLARATION:
				return validateUnresolvedClassDeclaration((UnresolvedClassDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_ENUM_DECLARATION:
				return validateUnresolvedEnumDeclaration((UnresolvedEnumDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_INTERFACE_DECLARATION:
				return validateUnresolvedInterfaceDeclaration((UnresolvedInterfaceDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_LABELED_STATEMENT:
				return validateUnresolvedLabeledStatement((UnresolvedLabeledStatement)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_METHOD_DECLARATION:
				return validateUnresolvedMethodDeclaration((UnresolvedMethodDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_SINGLE_VARIABLE_DECLARATION:
				return validateUnresolvedSingleVariableDeclaration((UnresolvedSingleVariableDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_TYPE:
				return validateUnresolvedType((UnresolvedType)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_TYPE_DECLARATION:
				return validateUnresolvedTypeDeclaration((UnresolvedTypeDeclaration)value, diagnostics, context);
			case JavaMMPackage.UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT:
				return validateUnresolvedVariableDeclarationFragment((UnresolvedVariableDeclarationFragment)value, diagnostics, context);
			case JavaMMPackage.VARIABLE_DECLARATION:
				return validateVariableDeclaration((VariableDeclaration)value, diagnostics, context);
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION:
				return validateVariableDeclarationExpression((VariableDeclarationExpression)value, diagnostics, context);
			case JavaMMPackage.VARIABLE_DECLARATION_FRAGMENT:
				return validateVariableDeclarationFragment((VariableDeclarationFragment)value, diagnostics, context);
			case JavaMMPackage.VARIABLE_DECLARATION_STATEMENT:
				return validateVariableDeclarationStatement((VariableDeclarationStatement)value, diagnostics, context);
			case JavaMMPackage.WILD_CARD_TYPE:
				return validateWildCardType((WildCardType)value, diagnostics, context);
			case JavaMMPackage.WHILE_STATEMENT:
				return validateWhileStatement((WhileStatement)value, diagnostics, context);
			case JavaMMPackage.ASSIGNMENT_KIND:
				return validateAssignmentKind((AssignmentKind)value, diagnostics, context);
			case JavaMMPackage.INFIX_EXPRESSION_KIND:
				return validateInfixExpressionKind((InfixExpressionKind)value, diagnostics, context);
			case JavaMMPackage.INHERITANCE_KIND:
				return validateInheritanceKind((InheritanceKind)value, diagnostics, context);
			case JavaMMPackage.POSTFIX_EXPRESSION_KIND:
				return validatePostfixExpressionKind((PostfixExpressionKind)value, diagnostics, context);
			case JavaMMPackage.PREFIX_EXPRESSION_KIND:
				return validatePrefixExpressionKind((PrefixExpressionKind)value, diagnostics, context);
			case JavaMMPackage.VISIBILITY_KIND:
				return validateVisibilityKind((VisibilityKind)value, diagnostics, context);
			default:
				return true;
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAbstractMethodDeclaration(AbstractMethodDeclaration abstractMethodDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(abstractMethodDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAbstractMethodInvocation(AbstractMethodInvocation abstractMethodInvocation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(abstractMethodInvocation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAbstractTypeDeclaration(AbstractTypeDeclaration abstractTypeDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(abstractTypeDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAbstractTypeQualifiedExpression(AbstractTypeQualifiedExpression abstractTypeQualifiedExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(abstractTypeQualifiedExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAbstractVariablesContainer(AbstractVariablesContainer abstractVariablesContainer, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(abstractVariablesContainer, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAnnotation(Annotation annotation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(annotation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArchive(Archive archive, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(archive, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAssertStatement(AssertStatement assertStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(assertStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateASTNode(ASTNode astNode, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(astNode, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAnnotationMemberValuePair(AnnotationMemberValuePair annotationMemberValuePair, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(annotationMemberValuePair, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAnnotationTypeDeclaration(AnnotationTypeDeclaration annotationTypeDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(annotationTypeDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAnnotationTypeMemberDeclaration(AnnotationTypeMemberDeclaration annotationTypeMemberDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(annotationTypeMemberDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAnonymousClassDeclaration(AnonymousClassDeclaration anonymousClassDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(anonymousClassDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArrayAccess(ArrayAccess arrayAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(arrayAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArrayCreation(ArrayCreation arrayCreation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(arrayCreation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArrayInitializer(ArrayInitializer arrayInitializer, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(arrayInitializer, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArrayLengthAccess(ArrayLengthAccess arrayLengthAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(arrayLengthAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateArrayType(ArrayType arrayType, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(arrayType, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAssignment(Assignment assignment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(assignment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBodyDeclaration(BodyDeclaration bodyDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(bodyDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBooleanLiteral(BooleanLiteral booleanLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(booleanLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBlockComment(BlockComment blockComment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(blockComment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBlock(Block block, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(block, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateBreakStatement(BreakStatement breakStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(breakStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateCastExpression(CastExpression castExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(castExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateCatchClause(CatchClause catchClause, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(catchClause, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateCharacterLiteral(CharacterLiteral characterLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(characterLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassFile(ClassFile classFile, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(classFile, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassInstanceCreation(ClassInstanceCreation classInstanceCreation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(classInstanceCreation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateConstructorDeclaration(ConstructorDeclaration constructorDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(constructorDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateConditionalExpression(ConditionalExpression conditionalExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(conditionalExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateConstructorInvocation(ConstructorInvocation constructorInvocation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(constructorInvocation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassDeclaration(ClassDeclaration classDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		if (!validate_NoCircularContainment(classDeclaration, diagnostics, context)) return false;
		boolean result = validate_EveryMultiplicityConforms(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryDataValueConforms(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryReferenceIsContained(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryBidirectionalReferenceIsPaired(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryProxyResolves(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_UniqueID(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryKeyUnique(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryMapEntryUnique(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validateClassDeclaration_hasEquals(classDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validateClassDeclaration_hasHashCode(classDeclaration, diagnostics, context);
		return result;
	}

	/**
	 * The cached validation expression for the hasEquals constraint of '<em>Class Declaration</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static final String CLASS_DECLARATION__HAS_EQUALS__EEXPRESSION = "hasHashcode() implies\n" +
		"\t\t\tgetPublicMethods()->exists(method |\n" +
		"\t\t\t\tmethod.name = 'equals' and\n" +
		"\t\t\t\tmethod.parameters->size() = 1 and\n" +
		"\t\t\t\tmethod.parameters->first().type.type.name = 'Object' and\n" +
		"\t\t\t\tmethod.thrownExceptions->isEmpty() and\n" +
		"\t\t\t\tmethod.returnType.type.oclIsTypeOf(PrimitiveTypeBoolean)\n" +
		"\t\t\t)";

	/**
	 * Validates the hasEquals constraint of '<em>Class Declaration</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassDeclaration_hasEquals(ClassDeclaration classDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return
			validate
				(JavaMMPackage.Literals.CLASS_DECLARATION,
				 classDeclaration,
				 diagnostics,
				 context,
				 "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot",
				 "hasEquals",
				 CLASS_DECLARATION__HAS_EQUALS__EEXPRESSION,
				 Diagnostic.ERROR,
				 DIAGNOSTIC_SOURCE,
				 0);
	}

	/**
	 * The cached validation expression for the hasHashCode constraint of '<em>Class Declaration</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static final String CLASS_DECLARATION__HAS_HASH_CODE__EEXPRESSION = "hasHashcode()";

	/**
	 * Validates the hasHashCode constraint of '<em>Class Declaration</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateClassDeclaration_hasHashCode(ClassDeclaration classDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return
			validate
				(JavaMMPackage.Literals.CLASS_DECLARATION,
				 classDeclaration,
				 diagnostics,
				 context,
				 "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot",
				 "hasHashCode",
				 CLASS_DECLARATION__HAS_HASH_CODE__EEXPRESSION,
				 Diagnostic.ERROR,
				 DIAGNOSTIC_SOURCE,
				 0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateComment(Comment comment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(comment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateCompilationUnit(CompilationUnit compilationUnit, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(compilationUnit, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateContinueStatement(ContinueStatement continueStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(continueStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateDoStatement(DoStatement doStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(doStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEmptyStatement(EmptyStatement emptyStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(emptyStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEnhancedForStatement(EnhancedForStatement enhancedForStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(enhancedForStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEnumConstantDeclaration(EnumConstantDeclaration enumConstantDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(enumConstantDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateEnumDeclaration(EnumDeclaration enumDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(enumDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateExpression(Expression expression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(expression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateExpressionStatement(ExpressionStatement expressionStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(expressionStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateFieldAccess(FieldAccess fieldAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(fieldAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateFieldDeclaration(FieldDeclaration fieldDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(fieldDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateForStatement(ForStatement forStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(forStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateIfStatement(IfStatement ifStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(ifStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateImportDeclaration(ImportDeclaration importDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(importDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInfixExpression(InfixExpression infixExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(infixExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInitializer(Initializer initializer, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(initializer, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInstanceofExpression(InstanceofExpression instanceofExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(instanceofExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInterfaceDeclaration(InterfaceDeclaration interfaceDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(interfaceDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateJavadoc(Javadoc javadoc, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(javadoc, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateLabeledStatement(LabeledStatement labeledStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(labeledStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateLineComment(LineComment lineComment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(lineComment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateManifest(Manifest manifest, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(manifest, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateManifestAttribute(ManifestAttribute manifestAttribute, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(manifestAttribute, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateManifestEntry(ManifestEntry manifestEntry, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(manifestEntry, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateMemberRef(MemberRef memberRef, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(memberRef, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateMethodDeclaration(MethodDeclaration methodDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(methodDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateMethodInvocation(MethodInvocation methodInvocation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(methodInvocation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateMethodRef(MethodRef methodRef, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(methodRef, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateMethodRefParameter(MethodRefParameter methodRefParameter, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(methodRefParameter, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateModel(Model model, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(model, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateModifier(Modifier modifier, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(modifier, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateNamedElement(NamedElement namedElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(namedElement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateNamespaceAccess(NamespaceAccess namespaceAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(namespaceAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateNumberLiteral(NumberLiteral numberLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(numberLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateNullLiteral(NullLiteral nullLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(nullLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePackage(javaMM.Package package_, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(package_, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePackageAccess(PackageAccess packageAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(packageAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateParameterizedType(ParameterizedType parameterizedType, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(parameterizedType, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateParenthesizedExpression(ParenthesizedExpression parenthesizedExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(parenthesizedExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePostfixExpression(PostfixExpression postfixExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(postfixExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrefixExpression(PrefixExpression prefixExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(prefixExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveType(PrimitiveType primitiveType, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveType, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeBoolean(PrimitiveTypeBoolean primitiveTypeBoolean, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeBoolean, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeByte(PrimitiveTypeByte primitiveTypeByte, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeByte, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeChar(PrimitiveTypeChar primitiveTypeChar, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeChar, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeDouble(PrimitiveTypeDouble primitiveTypeDouble, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeDouble, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeShort(PrimitiveTypeShort primitiveTypeShort, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeShort, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeFloat(PrimitiveTypeFloat primitiveTypeFloat, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeFloat, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeInt(PrimitiveTypeInt primitiveTypeInt, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeInt, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeLong(PrimitiveTypeLong primitiveTypeLong, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeLong, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrimitiveTypeVoid(PrimitiveTypeVoid primitiveTypeVoid, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(primitiveTypeVoid, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateReturnStatement(ReturnStatement returnStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(returnStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSingleVariableAccess(SingleVariableAccess singleVariableAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(singleVariableAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSingleVariableDeclaration(SingleVariableDeclaration singleVariableDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(singleVariableDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateStatement(Statement statement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(statement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateStringLiteral(StringLiteral stringLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(stringLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSuperConstructorInvocation(SuperConstructorInvocation superConstructorInvocation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(superConstructorInvocation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSuperFieldAccess(SuperFieldAccess superFieldAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(superFieldAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSuperMethodInvocation(SuperMethodInvocation superMethodInvocation, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(superMethodInvocation, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSwitchCase(SwitchCase switchCase, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(switchCase, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSwitchStatement(SwitchStatement switchStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(switchStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateSynchronizedStatement(SynchronizedStatement synchronizedStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(synchronizedStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTagElement(TagElement tagElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(tagElement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTextElement(TextElement textElement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(textElement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateThisExpression(ThisExpression thisExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(thisExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateThrowStatement(ThrowStatement throwStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(throwStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTryStatement(TryStatement tryStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(tryStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateType(Type type, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(type, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypeAccess(TypeAccess typeAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typeAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypeDeclaration(TypeDeclaration typeDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typeDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypeDeclarationStatement(TypeDeclarationStatement typeDeclarationStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typeDeclarationStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypeLiteral(TypeLiteral typeLiteral, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typeLiteral, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateTypeParameter(TypeParameter typeParameter, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(typeParameter, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedItem(UnresolvedItem unresolvedItem, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedItem, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedItemAccess(UnresolvedItemAccess unresolvedItemAccess, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedItemAccess, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedAnnotationDeclaration(UnresolvedAnnotationDeclaration unresolvedAnnotationDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedAnnotationDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedAnnotationTypeMemberDeclaration(UnresolvedAnnotationTypeMemberDeclaration unresolvedAnnotationTypeMemberDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedAnnotationTypeMemberDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedClassDeclaration(UnresolvedClassDeclaration unresolvedClassDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		if (!validate_NoCircularContainment(unresolvedClassDeclaration, diagnostics, context)) return false;
		boolean result = validate_EveryMultiplicityConforms(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryDataValueConforms(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryReferenceIsContained(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryBidirectionalReferenceIsPaired(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryProxyResolves(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_UniqueID(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryKeyUnique(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validate_EveryMapEntryUnique(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validateClassDeclaration_hasEquals(unresolvedClassDeclaration, diagnostics, context);
		if (result || diagnostics != null) result &= validateClassDeclaration_hasHashCode(unresolvedClassDeclaration, diagnostics, context);
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedEnumDeclaration(UnresolvedEnumDeclaration unresolvedEnumDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedEnumDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedInterfaceDeclaration(UnresolvedInterfaceDeclaration unresolvedInterfaceDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedInterfaceDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedLabeledStatement(UnresolvedLabeledStatement unresolvedLabeledStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedLabeledStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedMethodDeclaration(UnresolvedMethodDeclaration unresolvedMethodDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedMethodDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedSingleVariableDeclaration(UnresolvedSingleVariableDeclaration unresolvedSingleVariableDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedSingleVariableDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedType(UnresolvedType unresolvedType, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedType, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedTypeDeclaration(UnresolvedTypeDeclaration unresolvedTypeDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedTypeDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateUnresolvedVariableDeclarationFragment(UnresolvedVariableDeclarationFragment unresolvedVariableDeclarationFragment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(unresolvedVariableDeclarationFragment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateVariableDeclaration(VariableDeclaration variableDeclaration, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(variableDeclaration, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateVariableDeclarationExpression(VariableDeclarationExpression variableDeclarationExpression, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(variableDeclarationExpression, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateVariableDeclarationFragment(VariableDeclarationFragment variableDeclarationFragment, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(variableDeclarationFragment, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateVariableDeclarationStatement(VariableDeclarationStatement variableDeclarationStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(variableDeclarationStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateWildCardType(WildCardType wildCardType, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(wildCardType, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateWhileStatement(WhileStatement whileStatement, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return validate_EveryDefaultConstraint(whileStatement, diagnostics, context);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateAssignmentKind(AssignmentKind assignmentKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInfixExpressionKind(InfixExpressionKind infixExpressionKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateInheritanceKind(InheritanceKind inheritanceKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePostfixExpressionKind(PostfixExpressionKind postfixExpressionKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validatePrefixExpressionKind(PrefixExpressionKind prefixExpressionKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean validateVisibilityKind(VisibilityKind visibilityKind, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return true;
	}

	/**
	 * Returns the resource locator that will be used to fetch messages for this validator's diagnostics.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		// TODO
		// Specialize this to return a resource locator for messages specific to this validator.
		// Ensure that you remove @generated or mark it @generated NOT
		return super.getResourceLocator();
	}

} //JavaMMValidator
