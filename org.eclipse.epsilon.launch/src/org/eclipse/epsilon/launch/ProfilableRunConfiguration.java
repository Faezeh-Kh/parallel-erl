package org.eclipse.epsilon.launch;

import static java.lang.System.nanoTime;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.util.OsUtil;
import org.eclipse.epsilon.profiling.BenchmarkUtils;
import org.eclipse.epsilon.profiling.ProfileDiagnostic;
import org.eclipse.epsilon.profiling.ProfileDiagnostic.MemoryUnit;

public abstract class ProfilableRunConfiguration implements Runnable {
	
	protected String printMarker = "-----------------------------------------------------";
	protected int id;
	public final boolean showResults, profileExecution;
	public final Path script, outputFile;
	protected final Collection<ProfileDiagnostic> profiledStages;
	protected boolean hasRun = false;
	
	protected ProfilableRunConfiguration(
		Path scriptFile,
		Optional<Boolean> showResults,
		Optional<Boolean> profileExecution,
		Optional<Integer> configID,
		Optional<Path> scratchFile) {
			this.script = scriptFile;
			this.showResults = showResults.orElse(false);
			this.profileExecution = profileExecution.orElse(true);
			this.outputFile = scratchFile.orElse(null);
			this.profiledStages = new LinkedList<>();
			this.id = configID.orElseGet(() ->
				Objects.hash(
					Objects.toString(scriptFile)
				)
			);
	}
	
	protected ProfilableRunConfiguration(ProfilableRunConfiguration other) {
		this(
			other.script,
			Optional.of(other.showResults),
			Optional.of(other.profileExecution),
			Optional.of(other.id),
			Optional.ofNullable(other.outputFile)
		);
	}
	
	@Override
	public void run() {
		try {
			preExecute();
			
			if (profileExecution) {
				long endTime, endMemory,
				startMemory = BenchmarkUtils.getTotalMemoryUsage(),
				startTime = nanoTime();
				execute();
				endTime = nanoTime();
				endMemory = BenchmarkUtils.getTotalMemoryUsage();
				recordExecution(endTime-startTime, endMemory-startMemory);
			}
			else {
				execute();
			}
			
			postExecute();
		}
		catch (Exception ex) {
			handleException(ex);
		}
		hasRun = true;
	}
	
	protected void handleException(Exception ex) {
		ex.printStackTrace();
	}
	
	protected void preExecute() throws Exception {
		if (outputFile != null) {
			Path parent = outputFile.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
		}
		if (profileExecution) {
			writeOut(
				OsUtil.getCpuName(),
				"Logical processors: "+BenchmarkUtils.getNumberOfHardwareThreads(),
				"Xms: "+BenchmarkUtils.getAvailableMemory(MemoryUnit.MB),
				"Xmx: "+BenchmarkUtils.getMaxMemory(MemoryUnit.MB),
				"Starting execution at "+BenchmarkUtils.getTime(),
				printMarker
			);
		}
	}
	
	protected abstract void execute() throws Exception;
	
	protected void postExecute() throws Exception {
		if (profileExecution) {
			writeOut("",
				"Profiled processes:",
				BenchmarkUtils.formatExecutionStages(profiledStages),
				"Finished at "+BenchmarkUtils.getTime(),
				printMarker
			);
		}
	}
	
	protected void recordExecution(long nanos, long memory) {
		addProfileInfo("execute()", nanos, memory);
	}
	
	/*
	 * @param stage The identifier of executed code (must be unique)
	 * @param nanos The execution time in nanoseconds
	 * @param memory The memory consumption in bytes
	 */
	protected void addProfileInfo(String stage, long nanos, long memory) {
		BenchmarkUtils.addProfileInfo(profiledStages, stage, nanos, memory);
	}
	
	public Duration getExecutionTime() {
		if (!hasRun) {
			throw new IllegalStateException("Not yet run!");
		}
		return BenchmarkUtils.getTotalExecutionTimeFrom(profiledStages);
	}
	
	public int getId() {
		return id;
	}
	
	protected final void writeOut(Object... lines) {
		writeOut(Arrays.asList(lines));
	}
	
	protected void writeOut(Collection<?> lines) {
		if (outputFile != null) {
			try {
				Files.write(
					outputFile,
					lines.stream().map(Object::toString).collect(Collectors.toList()),
					StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE
				);
				return;
			}
			catch (IOException iox) {
				System.err.println("Couldn't write to file '"+outputFile+"': "+iox.getMessage());
			}
		}
		//Fall back to stdout if couldn't write to file
		lines.forEach(System.out::println);
	}
	
	@Override
	public String toString() {
		return
			getClass().getSimpleName()+
			": id="+id+
			", script='"+Objects.toString(script.getFileName())+"'";
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(id, script, showResults, profileExecution, outputFile);
	}
	
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ProfilableRunConfiguration))
			return false;
		
		ProfilableRunConfiguration prc = (ProfilableRunConfiguration) other;
		return
			Objects.equals(this.id, prc.id) &&
			Objects.equals(this.script, prc.script) &&
			Objects.equals(this.showResults, prc.showResults) &&
			Objects.equals(this.profileExecution, prc.profileExecution) &&
			Objects.equals(this.outputFile, prc.outputFile);
	}
}
