package javaMM.launch;

import org.eclipse.ocl.standalone.*;
import javaMM.JavaMMPackage;
import javaMM.util.JavaMMValidator;

public class JavaMMLauncher {
	public static void main(String[] args) {
		StandaloneOCL.newCompiledInstance(JavaMMPackage.eINSTANCE, JavaMMValidator.INSTANCE, args).run();
	}
}
