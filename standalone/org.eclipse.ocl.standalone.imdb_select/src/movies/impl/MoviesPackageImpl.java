/**
 */
package movies.impl;

import movies.Actor;
import movies.Actress;
import movies.Clique;
import movies.Couple;
import movies.Group;
import movies.Movie;
import movies.MovieType;
import movies.MoviesFactory;
import movies.MoviesPackage;
import movies.Person;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class MoviesPackageImpl extends EPackageImpl implements MoviesPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass personEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass actorEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass actressEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass coupleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass movieEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass groupEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cliqueEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum movieTypeEEnum = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see movies.MoviesPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private MoviesPackageImpl() {
		super(eNS_URI, MoviesFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link MoviesPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static MoviesPackage init() {
		if (isInited) return (MoviesPackage)EPackage.Registry.INSTANCE.getEPackage(MoviesPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredMoviesPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		MoviesPackageImpl theMoviesPackage = registeredMoviesPackage instanceof MoviesPackageImpl ? (MoviesPackageImpl)registeredMoviesPackage : new MoviesPackageImpl();

		isInited = true;

		// Create package meta-data objects
		theMoviesPackage.createPackageContents();

		// Initialize created meta-data
		theMoviesPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theMoviesPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(MoviesPackage.eNS_URI, theMoviesPackage);
		return theMoviesPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPerson() {
		return personEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getPerson_Movies() {
		return (EReference)personEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPerson_Name() {
		return (EAttribute)personEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EOperation getPerson__Coactors() {
		return personEClass.getEOperations().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EOperation getPerson__AreCouple__Person() {
		return personEClass.getEOperations().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EOperation getPerson__AreCoupleCoactors__Person() {
		return personEClass.getEOperations().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EOperation getPerson__QUERY() {
		return personEClass.getEOperations().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getActor() {
		return actorEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getActress() {
		return actressEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCouple() {
		return coupleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCouple_P1() {
		return (EReference)coupleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCouple_P2() {
		return (EReference)coupleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getMovie() {
		return movieEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getMovie_Persons() {
		return (EReference)movieEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMovie_Title() {
		return (EAttribute)movieEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMovie_Rating() {
		return (EAttribute)movieEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMovie_Year() {
		return (EAttribute)movieEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getMovie_Type() {
		return (EAttribute)movieEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGroup() {
		return groupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGroup_CommonMovies() {
		return (EReference)groupEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getGroup_AvgRating() {
		return (EAttribute)groupEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getClique() {
		return cliqueEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getClique_Persons() {
		return (EReference)cliqueEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EEnum getMovieType() {
		return movieTypeEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MoviesFactory getMoviesFactory() {
		return (MoviesFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		personEClass = createEClass(PERSON);
		createEReference(personEClass, PERSON__MOVIES);
		createEAttribute(personEClass, PERSON__NAME);
		createEOperation(personEClass, PERSON___COACTORS);
		createEOperation(personEClass, PERSON___ARE_COUPLE__PERSON);
		createEOperation(personEClass, PERSON___ARE_COUPLE_COACTORS__PERSON);
		createEOperation(personEClass, PERSON___QUERY);

		actorEClass = createEClass(ACTOR);

		actressEClass = createEClass(ACTRESS);

		coupleEClass = createEClass(COUPLE);
		createEReference(coupleEClass, COUPLE__P1);
		createEReference(coupleEClass, COUPLE__P2);

		movieEClass = createEClass(MOVIE);
		createEReference(movieEClass, MOVIE__PERSONS);
		createEAttribute(movieEClass, MOVIE__TITLE);
		createEAttribute(movieEClass, MOVIE__RATING);
		createEAttribute(movieEClass, MOVIE__YEAR);
		createEAttribute(movieEClass, MOVIE__TYPE);

		groupEClass = createEClass(GROUP);
		createEReference(groupEClass, GROUP__COMMON_MOVIES);
		createEAttribute(groupEClass, GROUP__AVG_RATING);

		cliqueEClass = createEClass(CLIQUE);
		createEReference(cliqueEClass, CLIQUE__PERSONS);

		// Create enums
		movieTypeEEnum = createEEnum(MOVIE_TYPE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		actorEClass.getESuperTypes().add(this.getPerson());
		actressEClass.getESuperTypes().add(this.getPerson());
		coupleEClass.getESuperTypes().add(this.getGroup());
		cliqueEClass.getESuperTypes().add(this.getGroup());

		// Initialize classes, features, and operations; add parameters
		initEClass(personEClass, Person.class, "Person", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getPerson_Movies(), this.getMovie(), this.getMovie_Persons(), "movies", null, 0, -1, Person.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPerson_Name(), ecorePackage.getEString(), "name", null, 0, 1, Person.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEOperation(getPerson__Coactors(), this.getPerson(), "coactors", 0, -1, IS_UNIQUE, !IS_ORDERED);

		EOperation op = initEOperation(getPerson__AreCouple__Person(), ecorePackage.getEBooleanObject(), "areCouple", 0, 1, IS_UNIQUE, IS_ORDERED);
		addEParameter(op, this.getPerson(), "p", 0, 1, IS_UNIQUE, IS_ORDERED);

		op = initEOperation(getPerson__AreCoupleCoactors__Person(), ecorePackage.getEBooleanObject(), "areCoupleCoactors", 0, 1, IS_UNIQUE, IS_ORDERED);
		addEParameter(op, this.getPerson(), "co", 0, 1, IS_UNIQUE, IS_ORDERED);

		initEOperation(getPerson__QUERY(), ecorePackage.getEBigInteger(), "QUERY", 0, 1, IS_UNIQUE, IS_ORDERED);

		initEClass(actorEClass, Actor.class, "Actor", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(actressEClass, Actress.class, "Actress", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(coupleEClass, Couple.class, "Couple", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCouple_P1(), this.getPerson(), null, "p1", null, 0, 1, Couple.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCouple_P2(), this.getPerson(), null, "p2", null, 0, 1, Couple.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(movieEClass, Movie.class, "Movie", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMovie_Persons(), this.getPerson(), this.getPerson_Movies(), "persons", null, 0, -1, Movie.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMovie_Title(), ecorePackage.getEString(), "title", null, 0, 1, Movie.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMovie_Rating(), ecorePackage.getEDouble(), "rating", null, 1, 1, Movie.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMovie_Year(), ecorePackage.getEInt(), "year", null, 1, 1, Movie.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getMovie_Type(), this.getMovieType(), "type", null, 0, 1, Movie.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(groupEClass, Group.class, "Group", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getGroup_CommonMovies(), this.getMovie(), null, "commonMovies", null, 0, -1, Group.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getGroup_AvgRating(), ecorePackage.getEDouble(), "avgRating", null, 1, 1, Group.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(cliqueEClass, Clique.class, "Clique", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getClique_Persons(), this.getPerson(), null, "persons", null, 0, -1, Clique.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Initialize enums and add enum literals
		initEEnum(movieTypeEEnum, MovieType.class, "MovieType");
		addEEnumLiteral(movieTypeEEnum, MovieType.MOVIE);
		addEEnumLiteral(movieTypeEEnum, MovieType.VIDEO);
		addEEnumLiteral(movieTypeEEnum, MovieType.TV);
		addEEnumLiteral(movieTypeEEnum, MovieType.VIDEOGAME);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http://www.eclipse.org/OCL/Import
		createImportAnnotations();
		// http://www.eclipse.org/emf/2002/Ecore
		createEcoreAnnotations();
		// http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot
		createPivotAnnotations();
	}

	/**
	 * Initializes the annotations for <b>http://www.eclipse.org/OCL/Import</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createImportAnnotations() {
		String source = "http://www.eclipse.org/OCL/Import";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "ecore", "http://www.eclipse.org/emf/2002/Ecore"
		   });
	}

	/**
	 * Initializes the annotations for <b>http://www.eclipse.org/emf/2002/Ecore</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createEcoreAnnotations() {
		String source = "http://www.eclipse.org/emf/2002/Ecore";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "invocationDelegates", "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot",
			   "settingDelegates", "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot",
			   "validationDelegates", "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot"
		   });
	}

	/**
	 * Initializes the annotations for <b>http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createPivotAnnotations() {
		String source = "http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot";
		addAnnotation
		  (getPerson__Coactors(),
		   source,
		   new String[] {
			   "body", "self.movies->collect(persons)->flatten()->asSet()"
		   });
		addAnnotation
		  (getPerson__AreCouple__Person(),
		   source,
		   new String[] {
			   "body", "self.movies->excludingAll(p.movies)->size() <= (self.movies->size() - 3)"
		   });
		addAnnotation
		  (getPerson__AreCoupleCoactors__Person(),
		   source,
		   new String[] {
			   "body", "self.name < co.name and co.movies->size() >= 3 and self.areCouple(co)"
		   });
		addAnnotation
		  (getPerson__QUERY(),
		   source,
		   new String[] {
			   "body", "Person.allInstances()->select(a | a.coactors()->exists(areCoupleCoactors(a)))->size()"
		   });
	}

} //MoviesPackageImpl
