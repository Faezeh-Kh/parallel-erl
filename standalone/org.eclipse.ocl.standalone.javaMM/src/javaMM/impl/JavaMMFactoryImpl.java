/**
 */
package javaMM.impl;

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
import javaMM.BooleanLiteral;
import javaMM.BreakStatement;
import javaMM.CastExpression;
import javaMM.CatchClause;
import javaMM.CharacterLiteral;
import javaMM.ClassDeclaration;
import javaMM.ClassFile;
import javaMM.ClassInstanceCreation;
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
import javaMM.JavaMMFactory;
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
import javaMM.TypeAccess;
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
import javaMM.VariableDeclarationExpression;
import javaMM.VariableDeclarationFragment;
import javaMM.VariableDeclarationStatement;
import javaMM.VisibilityKind;
import javaMM.WhileStatement;
import javaMM.WildCardType;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class JavaMMFactoryImpl extends EFactoryImpl implements JavaMMFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static JavaMMFactory init() {
		try {
			JavaMMFactory theJavaMMFactory = (JavaMMFactory)EPackage.Registry.INSTANCE.getEFactory(JavaMMPackage.eNS_URI);
			if (theJavaMMFactory != null) {
				return theJavaMMFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new JavaMMFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public JavaMMFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case JavaMMPackage.ANNOTATION: return createAnnotation();
			case JavaMMPackage.ARCHIVE: return createArchive();
			case JavaMMPackage.ASSERT_STATEMENT: return createAssertStatement();
			case JavaMMPackage.ANNOTATION_MEMBER_VALUE_PAIR: return createAnnotationMemberValuePair();
			case JavaMMPackage.ANNOTATION_TYPE_DECLARATION: return createAnnotationTypeDeclaration();
			case JavaMMPackage.ANNOTATION_TYPE_MEMBER_DECLARATION: return createAnnotationTypeMemberDeclaration();
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION: return createAnonymousClassDeclaration();
			case JavaMMPackage.ARRAY_ACCESS: return createArrayAccess();
			case JavaMMPackage.ARRAY_CREATION: return createArrayCreation();
			case JavaMMPackage.ARRAY_INITIALIZER: return createArrayInitializer();
			case JavaMMPackage.ARRAY_LENGTH_ACCESS: return createArrayLengthAccess();
			case JavaMMPackage.ARRAY_TYPE: return createArrayType();
			case JavaMMPackage.ASSIGNMENT: return createAssignment();
			case JavaMMPackage.BOOLEAN_LITERAL: return createBooleanLiteral();
			case JavaMMPackage.BLOCK_COMMENT: return createBlockComment();
			case JavaMMPackage.BLOCK: return createBlock();
			case JavaMMPackage.BREAK_STATEMENT: return createBreakStatement();
			case JavaMMPackage.CAST_EXPRESSION: return createCastExpression();
			case JavaMMPackage.CATCH_CLAUSE: return createCatchClause();
			case JavaMMPackage.CHARACTER_LITERAL: return createCharacterLiteral();
			case JavaMMPackage.CLASS_FILE: return createClassFile();
			case JavaMMPackage.CLASS_INSTANCE_CREATION: return createClassInstanceCreation();
			case JavaMMPackage.CONSTRUCTOR_DECLARATION: return createConstructorDeclaration();
			case JavaMMPackage.CONDITIONAL_EXPRESSION: return createConditionalExpression();
			case JavaMMPackage.CONSTRUCTOR_INVOCATION: return createConstructorInvocation();
			case JavaMMPackage.CLASS_DECLARATION: return createClassDeclaration();
			case JavaMMPackage.COMPILATION_UNIT: return createCompilationUnit();
			case JavaMMPackage.CONTINUE_STATEMENT: return createContinueStatement();
			case JavaMMPackage.DO_STATEMENT: return createDoStatement();
			case JavaMMPackage.EMPTY_STATEMENT: return createEmptyStatement();
			case JavaMMPackage.ENHANCED_FOR_STATEMENT: return createEnhancedForStatement();
			case JavaMMPackage.ENUM_CONSTANT_DECLARATION: return createEnumConstantDeclaration();
			case JavaMMPackage.ENUM_DECLARATION: return createEnumDeclaration();
			case JavaMMPackage.EXPRESSION_STATEMENT: return createExpressionStatement();
			case JavaMMPackage.FIELD_ACCESS: return createFieldAccess();
			case JavaMMPackage.FIELD_DECLARATION: return createFieldDeclaration();
			case JavaMMPackage.FOR_STATEMENT: return createForStatement();
			case JavaMMPackage.IF_STATEMENT: return createIfStatement();
			case JavaMMPackage.IMPORT_DECLARATION: return createImportDeclaration();
			case JavaMMPackage.INFIX_EXPRESSION: return createInfixExpression();
			case JavaMMPackage.INITIALIZER: return createInitializer();
			case JavaMMPackage.INSTANCEOF_EXPRESSION: return createInstanceofExpression();
			case JavaMMPackage.INTERFACE_DECLARATION: return createInterfaceDeclaration();
			case JavaMMPackage.JAVADOC: return createJavadoc();
			case JavaMMPackage.LABELED_STATEMENT: return createLabeledStatement();
			case JavaMMPackage.LINE_COMMENT: return createLineComment();
			case JavaMMPackage.MANIFEST: return createManifest();
			case JavaMMPackage.MANIFEST_ATTRIBUTE: return createManifestAttribute();
			case JavaMMPackage.MANIFEST_ENTRY: return createManifestEntry();
			case JavaMMPackage.MEMBER_REF: return createMemberRef();
			case JavaMMPackage.METHOD_DECLARATION: return createMethodDeclaration();
			case JavaMMPackage.METHOD_INVOCATION: return createMethodInvocation();
			case JavaMMPackage.METHOD_REF: return createMethodRef();
			case JavaMMPackage.METHOD_REF_PARAMETER: return createMethodRefParameter();
			case JavaMMPackage.MODEL: return createModel();
			case JavaMMPackage.MODIFIER: return createModifier();
			case JavaMMPackage.NUMBER_LITERAL: return createNumberLiteral();
			case JavaMMPackage.NULL_LITERAL: return createNullLiteral();
			case JavaMMPackage.PACKAGE: return createPackage();
			case JavaMMPackage.PACKAGE_ACCESS: return createPackageAccess();
			case JavaMMPackage.PARAMETERIZED_TYPE: return createParameterizedType();
			case JavaMMPackage.PARENTHESIZED_EXPRESSION: return createParenthesizedExpression();
			case JavaMMPackage.POSTFIX_EXPRESSION: return createPostfixExpression();
			case JavaMMPackage.PREFIX_EXPRESSION: return createPrefixExpression();
			case JavaMMPackage.PRIMITIVE_TYPE: return createPrimitiveType();
			case JavaMMPackage.PRIMITIVE_TYPE_BOOLEAN: return createPrimitiveTypeBoolean();
			case JavaMMPackage.PRIMITIVE_TYPE_BYTE: return createPrimitiveTypeByte();
			case JavaMMPackage.PRIMITIVE_TYPE_CHAR: return createPrimitiveTypeChar();
			case JavaMMPackage.PRIMITIVE_TYPE_DOUBLE: return createPrimitiveTypeDouble();
			case JavaMMPackage.PRIMITIVE_TYPE_SHORT: return createPrimitiveTypeShort();
			case JavaMMPackage.PRIMITIVE_TYPE_FLOAT: return createPrimitiveTypeFloat();
			case JavaMMPackage.PRIMITIVE_TYPE_INT: return createPrimitiveTypeInt();
			case JavaMMPackage.PRIMITIVE_TYPE_LONG: return createPrimitiveTypeLong();
			case JavaMMPackage.PRIMITIVE_TYPE_VOID: return createPrimitiveTypeVoid();
			case JavaMMPackage.RETURN_STATEMENT: return createReturnStatement();
			case JavaMMPackage.SINGLE_VARIABLE_ACCESS: return createSingleVariableAccess();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION: return createSingleVariableDeclaration();
			case JavaMMPackage.STRING_LITERAL: return createStringLiteral();
			case JavaMMPackage.SUPER_CONSTRUCTOR_INVOCATION: return createSuperConstructorInvocation();
			case JavaMMPackage.SUPER_FIELD_ACCESS: return createSuperFieldAccess();
			case JavaMMPackage.SUPER_METHOD_INVOCATION: return createSuperMethodInvocation();
			case JavaMMPackage.SWITCH_CASE: return createSwitchCase();
			case JavaMMPackage.SWITCH_STATEMENT: return createSwitchStatement();
			case JavaMMPackage.SYNCHRONIZED_STATEMENT: return createSynchronizedStatement();
			case JavaMMPackage.TAG_ELEMENT: return createTagElement();
			case JavaMMPackage.TEXT_ELEMENT: return createTextElement();
			case JavaMMPackage.THIS_EXPRESSION: return createThisExpression();
			case JavaMMPackage.THROW_STATEMENT: return createThrowStatement();
			case JavaMMPackage.TRY_STATEMENT: return createTryStatement();
			case JavaMMPackage.TYPE_ACCESS: return createTypeAccess();
			case JavaMMPackage.TYPE_DECLARATION_STATEMENT: return createTypeDeclarationStatement();
			case JavaMMPackage.TYPE_LITERAL: return createTypeLiteral();
			case JavaMMPackage.TYPE_PARAMETER: return createTypeParameter();
			case JavaMMPackage.UNRESOLVED_ITEM: return createUnresolvedItem();
			case JavaMMPackage.UNRESOLVED_ITEM_ACCESS: return createUnresolvedItemAccess();
			case JavaMMPackage.UNRESOLVED_ANNOTATION_DECLARATION: return createUnresolvedAnnotationDeclaration();
			case JavaMMPackage.UNRESOLVED_ANNOTATION_TYPE_MEMBER_DECLARATION: return createUnresolvedAnnotationTypeMemberDeclaration();
			case JavaMMPackage.UNRESOLVED_CLASS_DECLARATION: return createUnresolvedClassDeclaration();
			case JavaMMPackage.UNRESOLVED_ENUM_DECLARATION: return createUnresolvedEnumDeclaration();
			case JavaMMPackage.UNRESOLVED_INTERFACE_DECLARATION: return createUnresolvedInterfaceDeclaration();
			case JavaMMPackage.UNRESOLVED_LABELED_STATEMENT: return createUnresolvedLabeledStatement();
			case JavaMMPackage.UNRESOLVED_METHOD_DECLARATION: return createUnresolvedMethodDeclaration();
			case JavaMMPackage.UNRESOLVED_SINGLE_VARIABLE_DECLARATION: return createUnresolvedSingleVariableDeclaration();
			case JavaMMPackage.UNRESOLVED_TYPE: return createUnresolvedType();
			case JavaMMPackage.UNRESOLVED_TYPE_DECLARATION: return createUnresolvedTypeDeclaration();
			case JavaMMPackage.UNRESOLVED_VARIABLE_DECLARATION_FRAGMENT: return createUnresolvedVariableDeclarationFragment();
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION: return createVariableDeclarationExpression();
			case JavaMMPackage.VARIABLE_DECLARATION_FRAGMENT: return createVariableDeclarationFragment();
			case JavaMMPackage.VARIABLE_DECLARATION_STATEMENT: return createVariableDeclarationStatement();
			case JavaMMPackage.WILD_CARD_TYPE: return createWildCardType();
			case JavaMMPackage.WHILE_STATEMENT: return createWhileStatement();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case JavaMMPackage.ASSIGNMENT_KIND:
				return createAssignmentKindFromString(eDataType, initialValue);
			case JavaMMPackage.INFIX_EXPRESSION_KIND:
				return createInfixExpressionKindFromString(eDataType, initialValue);
			case JavaMMPackage.INHERITANCE_KIND:
				return createInheritanceKindFromString(eDataType, initialValue);
			case JavaMMPackage.POSTFIX_EXPRESSION_KIND:
				return createPostfixExpressionKindFromString(eDataType, initialValue);
			case JavaMMPackage.PREFIX_EXPRESSION_KIND:
				return createPrefixExpressionKindFromString(eDataType, initialValue);
			case JavaMMPackage.VISIBILITY_KIND:
				return createVisibilityKindFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case JavaMMPackage.ASSIGNMENT_KIND:
				return convertAssignmentKindToString(eDataType, instanceValue);
			case JavaMMPackage.INFIX_EXPRESSION_KIND:
				return convertInfixExpressionKindToString(eDataType, instanceValue);
			case JavaMMPackage.INHERITANCE_KIND:
				return convertInheritanceKindToString(eDataType, instanceValue);
			case JavaMMPackage.POSTFIX_EXPRESSION_KIND:
				return convertPostfixExpressionKindToString(eDataType, instanceValue);
			case JavaMMPackage.PREFIX_EXPRESSION_KIND:
				return convertPrefixExpressionKindToString(eDataType, instanceValue);
			case JavaMMPackage.VISIBILITY_KIND:
				return convertVisibilityKindToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Annotation createAnnotation() {
		AnnotationImpl annotation = new AnnotationImpl();
		return annotation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Archive createArchive() {
		ArchiveImpl archive = new ArchiveImpl();
		return archive;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AssertStatement createAssertStatement() {
		AssertStatementImpl assertStatement = new AssertStatementImpl();
		return assertStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AnnotationMemberValuePair createAnnotationMemberValuePair() {
		AnnotationMemberValuePairImpl annotationMemberValuePair = new AnnotationMemberValuePairImpl();
		return annotationMemberValuePair;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AnnotationTypeDeclaration createAnnotationTypeDeclaration() {
		AnnotationTypeDeclarationImpl annotationTypeDeclaration = new AnnotationTypeDeclarationImpl();
		return annotationTypeDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AnnotationTypeMemberDeclaration createAnnotationTypeMemberDeclaration() {
		AnnotationTypeMemberDeclarationImpl annotationTypeMemberDeclaration = new AnnotationTypeMemberDeclarationImpl();
		return annotationTypeMemberDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AnonymousClassDeclaration createAnonymousClassDeclaration() {
		AnonymousClassDeclarationImpl anonymousClassDeclaration = new AnonymousClassDeclarationImpl();
		return anonymousClassDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayAccess createArrayAccess() {
		ArrayAccessImpl arrayAccess = new ArrayAccessImpl();
		return arrayAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayCreation createArrayCreation() {
		ArrayCreationImpl arrayCreation = new ArrayCreationImpl();
		return arrayCreation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayInitializer createArrayInitializer() {
		ArrayInitializerImpl arrayInitializer = new ArrayInitializerImpl();
		return arrayInitializer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayLengthAccess createArrayLengthAccess() {
		ArrayLengthAccessImpl arrayLengthAccess = new ArrayLengthAccessImpl();
		return arrayLengthAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ArrayType createArrayType() {
		ArrayTypeImpl arrayType = new ArrayTypeImpl();
		return arrayType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Assignment createAssignment() {
		AssignmentImpl assignment = new AssignmentImpl();
		return assignment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BooleanLiteral createBooleanLiteral() {
		BooleanLiteralImpl booleanLiteral = new BooleanLiteralImpl();
		return booleanLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BlockComment createBlockComment() {
		BlockCommentImpl blockComment = new BlockCommentImpl();
		return blockComment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Block createBlock() {
		BlockImpl block = new BlockImpl();
		return block;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BreakStatement createBreakStatement() {
		BreakStatementImpl breakStatement = new BreakStatementImpl();
		return breakStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CastExpression createCastExpression() {
		CastExpressionImpl castExpression = new CastExpressionImpl();
		return castExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CatchClause createCatchClause() {
		CatchClauseImpl catchClause = new CatchClauseImpl();
		return catchClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CharacterLiteral createCharacterLiteral() {
		CharacterLiteralImpl characterLiteral = new CharacterLiteralImpl();
		return characterLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClassFile createClassFile() {
		ClassFileImpl classFile = new ClassFileImpl();
		return classFile;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClassInstanceCreation createClassInstanceCreation() {
		ClassInstanceCreationImpl classInstanceCreation = new ClassInstanceCreationImpl();
		return classInstanceCreation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ConstructorDeclaration createConstructorDeclaration() {
		ConstructorDeclarationImpl constructorDeclaration = new ConstructorDeclarationImpl();
		return constructorDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ConditionalExpression createConditionalExpression() {
		ConditionalExpressionImpl conditionalExpression = new ConditionalExpressionImpl();
		return conditionalExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ConstructorInvocation createConstructorInvocation() {
		ConstructorInvocationImpl constructorInvocation = new ConstructorInvocationImpl();
		return constructorInvocation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClassDeclaration createClassDeclaration() {
		ClassDeclarationImpl classDeclaration = new ClassDeclarationImpl();
		return classDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CompilationUnit createCompilationUnit() {
		CompilationUnitImpl compilationUnit = new CompilationUnitImpl();
		return compilationUnit;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ContinueStatement createContinueStatement() {
		ContinueStatementImpl continueStatement = new ContinueStatementImpl();
		return continueStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DoStatement createDoStatement() {
		DoStatementImpl doStatement = new DoStatementImpl();
		return doStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EmptyStatement createEmptyStatement() {
		EmptyStatementImpl emptyStatement = new EmptyStatementImpl();
		return emptyStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnhancedForStatement createEnhancedForStatement() {
		EnhancedForStatementImpl enhancedForStatement = new EnhancedForStatementImpl();
		return enhancedForStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumConstantDeclaration createEnumConstantDeclaration() {
		EnumConstantDeclarationImpl enumConstantDeclaration = new EnumConstantDeclarationImpl();
		return enumConstantDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumDeclaration createEnumDeclaration() {
		EnumDeclarationImpl enumDeclaration = new EnumDeclarationImpl();
		return enumDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExpressionStatement createExpressionStatement() {
		ExpressionStatementImpl expressionStatement = new ExpressionStatementImpl();
		return expressionStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FieldAccess createFieldAccess() {
		FieldAccessImpl fieldAccess = new FieldAccessImpl();
		return fieldAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FieldDeclaration createFieldDeclaration() {
		FieldDeclarationImpl fieldDeclaration = new FieldDeclarationImpl();
		return fieldDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ForStatement createForStatement() {
		ForStatementImpl forStatement = new ForStatementImpl();
		return forStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public IfStatement createIfStatement() {
		IfStatementImpl ifStatement = new IfStatementImpl();
		return ifStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ImportDeclaration createImportDeclaration() {
		ImportDeclarationImpl importDeclaration = new ImportDeclarationImpl();
		return importDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InfixExpression createInfixExpression() {
		InfixExpressionImpl infixExpression = new InfixExpressionImpl();
		return infixExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Initializer createInitializer() {
		InitializerImpl initializer = new InitializerImpl();
		return initializer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InstanceofExpression createInstanceofExpression() {
		InstanceofExpressionImpl instanceofExpression = new InstanceofExpressionImpl();
		return instanceofExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InterfaceDeclaration createInterfaceDeclaration() {
		InterfaceDeclarationImpl interfaceDeclaration = new InterfaceDeclarationImpl();
		return interfaceDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Javadoc createJavadoc() {
		JavadocImpl javadoc = new JavadocImpl();
		return javadoc;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LabeledStatement createLabeledStatement() {
		LabeledStatementImpl labeledStatement = new LabeledStatementImpl();
		return labeledStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LineComment createLineComment() {
		LineCommentImpl lineComment = new LineCommentImpl();
		return lineComment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Manifest createManifest() {
		ManifestImpl manifest = new ManifestImpl();
		return manifest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ManifestAttribute createManifestAttribute() {
		ManifestAttributeImpl manifestAttribute = new ManifestAttributeImpl();
		return manifestAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ManifestEntry createManifestEntry() {
		ManifestEntryImpl manifestEntry = new ManifestEntryImpl();
		return manifestEntry;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberRef createMemberRef() {
		MemberRefImpl memberRef = new MemberRefImpl();
		return memberRef;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MethodDeclaration createMethodDeclaration() {
		MethodDeclarationImpl methodDeclaration = new MethodDeclarationImpl();
		return methodDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MethodInvocation createMethodInvocation() {
		MethodInvocationImpl methodInvocation = new MethodInvocationImpl();
		return methodInvocation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MethodRef createMethodRef() {
		MethodRefImpl methodRef = new MethodRefImpl();
		return methodRef;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MethodRefParameter createMethodRefParameter() {
		MethodRefParameterImpl methodRefParameter = new MethodRefParameterImpl();
		return methodRefParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Model createModel() {
		ModelImpl model = new ModelImpl();
		return model;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Modifier createModifier() {
		ModifierImpl modifier = new ModifierImpl();
		return modifier;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NumberLiteral createNumberLiteral() {
		NumberLiteralImpl numberLiteral = new NumberLiteralImpl();
		return numberLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NullLiteral createNullLiteral() {
		NullLiteralImpl nullLiteral = new NullLiteralImpl();
		return nullLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public javaMM.Package createPackage() {
		PackageImpl package_ = new PackageImpl();
		return package_;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PackageAccess createPackageAccess() {
		PackageAccessImpl packageAccess = new PackageAccessImpl();
		return packageAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ParameterizedType createParameterizedType() {
		ParameterizedTypeImpl parameterizedType = new ParameterizedTypeImpl();
		return parameterizedType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ParenthesizedExpression createParenthesizedExpression() {
		ParenthesizedExpressionImpl parenthesizedExpression = new ParenthesizedExpressionImpl();
		return parenthesizedExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PostfixExpression createPostfixExpression() {
		PostfixExpressionImpl postfixExpression = new PostfixExpressionImpl();
		return postfixExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrefixExpression createPrefixExpression() {
		PrefixExpressionImpl prefixExpression = new PrefixExpressionImpl();
		return prefixExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveType createPrimitiveType() {
		PrimitiveTypeImpl primitiveType = new PrimitiveTypeImpl();
		return primitiveType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeBoolean createPrimitiveTypeBoolean() {
		PrimitiveTypeBooleanImpl primitiveTypeBoolean = new PrimitiveTypeBooleanImpl();
		return primitiveTypeBoolean;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeByte createPrimitiveTypeByte() {
		PrimitiveTypeByteImpl primitiveTypeByte = new PrimitiveTypeByteImpl();
		return primitiveTypeByte;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeChar createPrimitiveTypeChar() {
		PrimitiveTypeCharImpl primitiveTypeChar = new PrimitiveTypeCharImpl();
		return primitiveTypeChar;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeDouble createPrimitiveTypeDouble() {
		PrimitiveTypeDoubleImpl primitiveTypeDouble = new PrimitiveTypeDoubleImpl();
		return primitiveTypeDouble;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeShort createPrimitiveTypeShort() {
		PrimitiveTypeShortImpl primitiveTypeShort = new PrimitiveTypeShortImpl();
		return primitiveTypeShort;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeFloat createPrimitiveTypeFloat() {
		PrimitiveTypeFloatImpl primitiveTypeFloat = new PrimitiveTypeFloatImpl();
		return primitiveTypeFloat;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeInt createPrimitiveTypeInt() {
		PrimitiveTypeIntImpl primitiveTypeInt = new PrimitiveTypeIntImpl();
		return primitiveTypeInt;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeLong createPrimitiveTypeLong() {
		PrimitiveTypeLongImpl primitiveTypeLong = new PrimitiveTypeLongImpl();
		return primitiveTypeLong;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrimitiveTypeVoid createPrimitiveTypeVoid() {
		PrimitiveTypeVoidImpl primitiveTypeVoid = new PrimitiveTypeVoidImpl();
		return primitiveTypeVoid;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReturnStatement createReturnStatement() {
		ReturnStatementImpl returnStatement = new ReturnStatementImpl();
		return returnStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleVariableAccess createSingleVariableAccess() {
		SingleVariableAccessImpl singleVariableAccess = new SingleVariableAccessImpl();
		return singleVariableAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleVariableDeclaration createSingleVariableDeclaration() {
		SingleVariableDeclarationImpl singleVariableDeclaration = new SingleVariableDeclarationImpl();
		return singleVariableDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public StringLiteral createStringLiteral() {
		StringLiteralImpl stringLiteral = new StringLiteralImpl();
		return stringLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SuperConstructorInvocation createSuperConstructorInvocation() {
		SuperConstructorInvocationImpl superConstructorInvocation = new SuperConstructorInvocationImpl();
		return superConstructorInvocation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SuperFieldAccess createSuperFieldAccess() {
		SuperFieldAccessImpl superFieldAccess = new SuperFieldAccessImpl();
		return superFieldAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SuperMethodInvocation createSuperMethodInvocation() {
		SuperMethodInvocationImpl superMethodInvocation = new SuperMethodInvocationImpl();
		return superMethodInvocation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SwitchCase createSwitchCase() {
		SwitchCaseImpl switchCase = new SwitchCaseImpl();
		return switchCase;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SwitchStatement createSwitchStatement() {
		SwitchStatementImpl switchStatement = new SwitchStatementImpl();
		return switchStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SynchronizedStatement createSynchronizedStatement() {
		SynchronizedStatementImpl synchronizedStatement = new SynchronizedStatementImpl();
		return synchronizedStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TagElement createTagElement() {
		TagElementImpl tagElement = new TagElementImpl();
		return tagElement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TextElement createTextElement() {
		TextElementImpl textElement = new TextElementImpl();
		return textElement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ThisExpression createThisExpression() {
		ThisExpressionImpl thisExpression = new ThisExpressionImpl();
		return thisExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ThrowStatement createThrowStatement() {
		ThrowStatementImpl throwStatement = new ThrowStatementImpl();
		return throwStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TryStatement createTryStatement() {
		TryStatementImpl tryStatement = new TryStatementImpl();
		return tryStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeAccess createTypeAccess() {
		TypeAccessImpl typeAccess = new TypeAccessImpl();
		return typeAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeDeclarationStatement createTypeDeclarationStatement() {
		TypeDeclarationStatementImpl typeDeclarationStatement = new TypeDeclarationStatementImpl();
		return typeDeclarationStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeLiteral createTypeLiteral() {
		TypeLiteralImpl typeLiteral = new TypeLiteralImpl();
		return typeLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeParameter createTypeParameter() {
		TypeParameterImpl typeParameter = new TypeParameterImpl();
		return typeParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedItem createUnresolvedItem() {
		UnresolvedItemImpl unresolvedItem = new UnresolvedItemImpl();
		return unresolvedItem;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedItemAccess createUnresolvedItemAccess() {
		UnresolvedItemAccessImpl unresolvedItemAccess = new UnresolvedItemAccessImpl();
		return unresolvedItemAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedAnnotationDeclaration createUnresolvedAnnotationDeclaration() {
		UnresolvedAnnotationDeclarationImpl unresolvedAnnotationDeclaration = new UnresolvedAnnotationDeclarationImpl();
		return unresolvedAnnotationDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedAnnotationTypeMemberDeclaration createUnresolvedAnnotationTypeMemberDeclaration() {
		UnresolvedAnnotationTypeMemberDeclarationImpl unresolvedAnnotationTypeMemberDeclaration = new UnresolvedAnnotationTypeMemberDeclarationImpl();
		return unresolvedAnnotationTypeMemberDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedClassDeclaration createUnresolvedClassDeclaration() {
		UnresolvedClassDeclarationImpl unresolvedClassDeclaration = new UnresolvedClassDeclarationImpl();
		return unresolvedClassDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedEnumDeclaration createUnresolvedEnumDeclaration() {
		UnresolvedEnumDeclarationImpl unresolvedEnumDeclaration = new UnresolvedEnumDeclarationImpl();
		return unresolvedEnumDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedInterfaceDeclaration createUnresolvedInterfaceDeclaration() {
		UnresolvedInterfaceDeclarationImpl unresolvedInterfaceDeclaration = new UnresolvedInterfaceDeclarationImpl();
		return unresolvedInterfaceDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedLabeledStatement createUnresolvedLabeledStatement() {
		UnresolvedLabeledStatementImpl unresolvedLabeledStatement = new UnresolvedLabeledStatementImpl();
		return unresolvedLabeledStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedMethodDeclaration createUnresolvedMethodDeclaration() {
		UnresolvedMethodDeclarationImpl unresolvedMethodDeclaration = new UnresolvedMethodDeclarationImpl();
		return unresolvedMethodDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedSingleVariableDeclaration createUnresolvedSingleVariableDeclaration() {
		UnresolvedSingleVariableDeclarationImpl unresolvedSingleVariableDeclaration = new UnresolvedSingleVariableDeclarationImpl();
		return unresolvedSingleVariableDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedType createUnresolvedType() {
		UnresolvedTypeImpl unresolvedType = new UnresolvedTypeImpl();
		return unresolvedType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedTypeDeclaration createUnresolvedTypeDeclaration() {
		UnresolvedTypeDeclarationImpl unresolvedTypeDeclaration = new UnresolvedTypeDeclarationImpl();
		return unresolvedTypeDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnresolvedVariableDeclarationFragment createUnresolvedVariableDeclarationFragment() {
		UnresolvedVariableDeclarationFragmentImpl unresolvedVariableDeclarationFragment = new UnresolvedVariableDeclarationFragmentImpl();
		return unresolvedVariableDeclarationFragment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableDeclarationExpression createVariableDeclarationExpression() {
		VariableDeclarationExpressionImpl variableDeclarationExpression = new VariableDeclarationExpressionImpl();
		return variableDeclarationExpression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableDeclarationFragment createVariableDeclarationFragment() {
		VariableDeclarationFragmentImpl variableDeclarationFragment = new VariableDeclarationFragmentImpl();
		return variableDeclarationFragment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableDeclarationStatement createVariableDeclarationStatement() {
		VariableDeclarationStatementImpl variableDeclarationStatement = new VariableDeclarationStatementImpl();
		return variableDeclarationStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public WildCardType createWildCardType() {
		WildCardTypeImpl wildCardType = new WildCardTypeImpl();
		return wildCardType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public WhileStatement createWhileStatement() {
		WhileStatementImpl whileStatement = new WhileStatementImpl();
		return whileStatement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AssignmentKind createAssignmentKindFromString(EDataType eDataType, String initialValue) {
		AssignmentKind result = AssignmentKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertAssignmentKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InfixExpressionKind createInfixExpressionKindFromString(EDataType eDataType, String initialValue) {
		InfixExpressionKind result = InfixExpressionKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertInfixExpressionKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InheritanceKind createInheritanceKindFromString(EDataType eDataType, String initialValue) {
		InheritanceKind result = InheritanceKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertInheritanceKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PostfixExpressionKind createPostfixExpressionKindFromString(EDataType eDataType, String initialValue) {
		PostfixExpressionKind result = PostfixExpressionKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertPostfixExpressionKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public PrefixExpressionKind createPrefixExpressionKindFromString(EDataType eDataType, String initialValue) {
		PrefixExpressionKind result = PrefixExpressionKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertPrefixExpressionKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VisibilityKind createVisibilityKindFromString(EDataType eDataType, String initialValue) {
		VisibilityKind result = VisibilityKind.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertVisibilityKindToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public JavaMMPackage getJavaMMPackage() {
		return (JavaMMPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static JavaMMPackage getPackage() {
		return JavaMMPackage.eINSTANCE;
	}

} //JavaMMFactoryImpl
