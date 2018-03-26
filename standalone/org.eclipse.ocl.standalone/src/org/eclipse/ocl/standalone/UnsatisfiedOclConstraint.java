package org.eclipse.ocl.standalone;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.eclipse.emf.common.util.Diagnostic;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;

public class UnsatisfiedOclConstraint {
	private static final String preamble = "Diagnostic [A-Z]* source=[a-zA-Z0-9\\.]* code=[0-9]* The ";
	private static final Pattern pattern = Pattern.compile("The '[^\\s]+::[^\\s]+' constraint is violated for ");
	
	public final String constraintName;
	public final EObject element;
	
	public UnsatisfiedOclConstraint(String constraintName, EObject element) {
		this.constraintName = constraintName;
		this.element = element;
	}
	
	UnsatisfiedOclConstraint(Diagnostic diag) {
		Matcher matcher = pattern.matcher(diag.getMessage().replaceAll(preamble, ""));
		matcher.find();
		this.constraintName = matcher.group().split(" ")[1].split("::")[1].replace("'", "");
		this.element = (EObject) diag.getData().get(0);
	}
	
	@Override
	public String toString() {
		return "'"+constraintName+"': "+element;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(constraintName, EcoreUtil.getID(element));
	}
	
	@Override
	public boolean equals(Object other) {
		if (this == other) return true;
		if (!(other instanceof UnsatisfiedOclConstraint))
			return false;
		
		UnsatisfiedOclConstraint uoc = (UnsatisfiedOclConstraint) other;
		return
			Objects.equals(this.constraintName, uoc.constraintName) &&
			Objects.equals(EcoreUtil.getID(this.element), EcoreUtil.getID(uoc.element));
	}
	
	public static Collection<UnsatisfiedOclConstraint> getUnsatisfiedConstraintsFromDiagnostic(Diagnostic diagnostic) {
		return diagnostic.getChildren()
			.parallelStream()
			.filter(d -> d != null && d.getSeverity() == 2)
			.map(UnsatisfiedOclConstraint::new)
			.collect(Collectors.toList());
	}
	
	public static Map<String, Integer> sortUnsatisfiedConstraintsBySize(Collection<UnsatisfiedOclConstraint> unsatisfiedConstraints) {
	    return unsatisfiedConstraints
            .stream()
            .collect(Collectors.groupingByConcurrent(uc -> uc.constraintName, Collectors.summingInt(uc -> 1)))
            .entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
	}
}