#!/usr/bin/python3
import os, argparse, re, csv, statistics, math, sys

parser = argparse.ArgumentParser()
parser.add_argument('mode', choices=['GENERATE', 'ANALYSE'], help='What do you want the program to do?')
parser.add_argument('--rootDir', help='The base directory. For all unspecified paths, everything will be assumed to be relative to this. Default value is current directory.')
parser.add_argument('--inDir', help='Input directory.')
parser.add_argument('--csvFile', help='Output file for the results.')
parser.add_argument('--texFile', help='Output file for the results in LaTeX tabluar format. Leave blank for default.')
parser.add_argument('--binDir', help='The directory where executable JAR files are stored (absolute path).')
parser.add_argument('--scriptDir', help='The scripts directory (absolute path).')
parser.add_argument('--modelDir', help='The models directory (absolute path).')
parser.add_argument('--metamodelDir', help='The metamodels directory (absolute path).')
parser.add_argument('--stdDir', help='The directory to send program output to (absolute path).')
parser.add_argument('--genDir', help='The directory to place all generated files (absolute path).')
parser.add_argument('--resultsDir', help='The directory to place analysis results.')
parser.add_argument('--basePath', help='The basePath argument for distributed modules.')
parser.add_argument('--jmc', help='Enable application profiling using Java Flight Recorder.', action='store_true')
parser.add_argument('--sge', help='Output for YARCC.', action='store_true')
parser.add_argument('--java8', help='Compatibility with Java 8 JVM', action='store_true')
parser.add_argument('--smt', help='Whether the system uses Hyper-Threading technology.', action='store_true')
parser.add_argument('--vmargs', help='Additional arguments to the JVM.')
parser.add_argument('--broker', help='Broker URL to use for distributed modules.')
parser.add_argument('--workers', help='Expected number of workers (parallelism) for distributed modules.')
parser.add_argument('--batch', help='Batch factor for batch-based distributed modules.')
parser.add_argument('--repeats', help='Number of runs in same JVM for each experiment.')
parser.add_argument('--duplicates', help='Number of times to write each experiment to aggregate benchmark script.')
args = parser.parse_args()

def defaultPath(parsedArg, defaultValue):
    thePath = os.path.normpath(parsedArg if parsedArg else defaultValue)
    if thePath.startswith('..'):
        thePath = os.path.abspath(os.path.join(os.getcwd(), thePath))
    return os.path.normpath(thePath).replace('\\', '/')+'/'

def makePathIfNotExists(path):
    if not os.path.exists(path):
        os.makedirs(path)

isGenerate = args.mode == 'GENERATE'
nL = '\n'
intermediateExt = '.txt'
resultsExt = '.csv'
rootDir = defaultPath(args.rootDir, os.getcwd())

if isGenerate:
    binDir = defaultPath(args.binDir, rootDir+'bin')
    scriptDir = defaultPath(args.scriptDir, rootDir+'scripts')
    modelDir = defaultPath(args.modelDir, rootDir+'models')
    metamodelDir = defaultPath(args.metamodelDir, rootDir+'metamodels')
    stdDir = defaultPath(args.stdDir, rootDir+'output')
    genDir = defaultPath(args.genDir, rootDir+'generated')
    makePathIfNotExists(genDir)
if not isGenerate:
    inDir = defaultPath(args.inDir, rootDir+'output')
    resultsDir = defaultPath(args.resultsDir, rootDir+'results')
    makePathIfNotExists(resultsDir)
    resultsFileName = resultsDir + (args.csvFile if args.csvFile else 'results.csv')
    resultsFileName += resultsExt if not resultsFileName.endswith(resultsExt) else ''
    texFileName = defaultPath(args.texFile, resultsDir+'results.tex')[:-1] if args.texFile else None

java8 = args.java8
sge = args.sge
jmc = args.jmc
smt = args.smt
repeats = args.repeats
numBenchDuplicates = int(args.duplicates) if args.duplicates else 5
basePath = args.basePath if args.basePath else rootDir
broker = args.broker if args.broker else 'tcp://localhost:61616'
maxWorkers = int(args.workers) if args.workers else 0
distributedArgs = '-basePath "'+basePath+'" -host '+broker+' -session 746'
logicalCores = 24 if sge else os.cpu_count()
batchFactor = args.batch if args.batch else str(logicalCores)
fileExt = '.cmd' if (os.name == 'nt' and not sge) else '.sh' 
fileNameRegex = r'(.*)_(.*_.*)_(.*)(\.txt)' # Script name must be preceded by metamodel!
resultsFile = open(resultsFileName, 'w') if not isGenerate else None
writer = csv.writer(resultsFile, lineterminator='\n') if not isGenerate else None
rows = []
columns = ['MODULE', 'THREADS', 'SCRIPT', 'MODEL', 'EXEC_TIME', 'EXEC_TIME_STDEV', 'SPEEDUP', 'EFFICIENCY', 'EXEC_MEMORY', 'EXEC_MEMORY_STDEV', 'MODEL_TIME', 'MODEL_TIME_STDEV', 'MODEL_MEMORY', 'MODEL_MEMORY_STDEV']

sgeDirectives = '''export MALLOC_ARENA_MAX='''+str(round(logicalCores/4))+'''
#$ -cwd
#$ -o ../../stdout
#$ -e ../../stderr
#$ -l exclusive=TRUE
#$ -l ram-128=TRUE
#$ -l E5-2650v4=TRUE
#$ -binding linear:'''+str(logicalCores)+'''
#$ -pe smp '''+str(logicalCores)+'''
#$ -l h_vmem='''+str(60/logicalCores)+'''G
#$ -l h_rt=7:59:59
'''
jvmFlags = 'java -ea -XX:MaxRAM'
jvmFlags += 'Fraction=1' if sge or java8 else 'Percentage=92'
jvmFlags += ' -XX:InitialRAM'
jvmFlags += 'Fraction=4' if sge or java8 else 'Percentage=25'
if args.vmargs:
    jvmFlags += ' '
    if not args.vmargs.startswith('-'):
        jvmFlags += '-'
    jvmFlags += args.vmargs.replace('"', '').replace("'", '')
if jmc:
    jvmFlags += ' -XX:+FlightRecorder -XX:StartFlightRecording=dumponexit=true,filename='

subCmdPrefix = 'qsub ' if sge else 'call ' if os.name == 'nt' else './'
subCmdSuffix = nL
epsilonJar = 'epsilon-engine'

# Author: A.Polino
def is_power2(num):
    return num != 0 and ((num & (num - 1)) == 0)

# https://stackoverflow.com/a/14267557/5870336
def next_power_of_2(x):  
    return 1 if x == 0 else 2**(x - 1).bit_length()

# https://in.mathworks.com/matlabcentral/answers/368590-how-to-find-next-number-divisible-by-n
def next_6(x):
    return round(6*math.ceil(x/6))

threads = [1]
if logicalCores >= 2:
    threads.append(2)
if logicalCores == 3:
    threads.append(3)
if logicalCores >= 4:
    threads.append(4)
    isNormal = is_power2(logicalCores)
    isWeird = not isNormal and logicalCores % 6 == 0
    if isWeird:
        threads.append(3)
    for i in range(5, logicalCores+1, 2):
        threadCounter = next_power_of_2(i) if isNormal else next_6(i)
        if threadCounter >= logicalCores:
            threads.append(logicalCores)
            break
        elif not threadCounter in threads:
            threads.append(threadCounter)
maxCores = round(threads[-1]/2) if smt else threads[-1]
maxCoresStr = str(maxCores)
programs = []

def write_table(colHeadings, tabRows, tabCaption, tabNum = 0, longtable = False):
    colFormat = 'c'
    for i in range(len(colHeadings)):
        if longtable:
            colFormat += ' |'
        colFormat += ' c'
    
    texFile.write(
(r'\begin{longtable}{|'+colFormat+r'''|} \toprule
''' if longtable else
r'''\begin{table} \toprule
\noindent\makebox[\textwidth]{
\begin{tabular}{||'''+colFormat+r'''||}
''')+format_tex_row(colHeadings, r'[0.5ex] \hline'+nL))

    for tabRow in tabRows:
        texFile.write(format_tex_row(tabRow))

    if not longtable:
        texFile.write(r'\end{tabular}}')
    
    texFile.write(
r'''\caption{'''+tabCaption+r'''}
\label{table:'''+str(tabNum)+r'''}
\end{'''+(r'longtable' if longtable else r'table')+r'''}

''')
#END write_table

def format_tex_row(entries, modifier = ''):
    result = ''
    for entry in entries[:-1]:
        result += str(entry)+r' & '
    result += str(entries[-1])+r' \\ '+modifier+r'\hline'+nL
    return result.replace('_', '\\_').replace('%', '\\%').replace('None', '---')

def compute_descriptive_stats(data, roundToInt = True):
    dmean = statistics.mean(data) if data else None
    dstdev = statistics.stdev(data, dmean) if (dmean and len(data) >= 2) else None
    stats = [dmean, dstdev]
    if roundToInt:
        stats = [(round(ds) if ds else None) for ds in stats]
    return tuple(stats)

def get_scenario_name(moduleConfig, script, model):
    module = moduleConfig if isinstance(moduleConfig, str) else moduleConfig[0]
    return module+'_'+script+'_'+model

def write_generated_file(filename, lines):
    with open(genDir + filename + fileExt, 'w') as qsbFile:
        qsbFile.writelines(lines)

def write_benchmark_scenarios(name, scenariosArgs, duplicates = numBenchDuplicates):
    lines = [subCmdPrefix+get_scenario_name(module, script, model)+fileExt+subCmdSuffix for (module, script, model) in scenariosArgs]
    write_generated_file(name+'_benchmarks', lines*duplicates)

def normalize_foop(script):
        normalFOOP = script.replace('_parallel', '_')
        if len(normalFOOP) == len(script):
            return script
        metamodelScriptIndex = script.index('_')+1
        normalFOOP = normalFOOP[0:metamodelScriptIndex] + normalFOOP[metamodelScriptIndex].lower() + normalFOOP[metamodelScriptIndex+1:] if normalFOOP else script
        return normalFOOP

def calculate_master_proportion(numWorkers):
    return 1 / (1 + numWorkers)

# (Meta)Models
# Java models can be obtained from http://atenea.lcc.uma.es/index.php/Main_Page/Resources/LinTra#Java_Refactoring
javaMM = 'java.ecore'
imdbMM = 'movies.ecore'
dblpMM = 'dblp.ecore'
simulinkMM = 'simulink.ecore'
eclipsePrefix = 'eclipseModel-'
imdbPrefix = 'imdb-'
imdbRanges = ['all', '0.1', '0.2'] + [str(round(i/10, 1)) for i in range(5, 35, 5)]
eclipseRanges = imdbRanges + ['3.5', '4.0']
imdbModels = [imdbPrefix + imdbR + '.xmi' for imdbR in imdbRanges]
javaModels = [eclipsePrefix + eclipseR + '.xmi' for eclipseR in eclipseRanges]

# Validation (EVL, OCL)
javaValidationScripts = [
    'java_findbugs',
    'java_simple',
    'java_1Constraint',
    'java_equals',
    'java_manyConstraint1Context',
    'java_manyContext1Constraint'
]

simulinkModels = [
    'darkd0', 'darksc2', 'darkd3', 'darkd4', 'darkd6', 'darktriad3'
]

evlParallelModules = [
    'EvlModuleParallelAnnotation',
    'EvlModuleParallelContextAtoms',
    'EvlModuleParallelConstraintAtoms',
    'EvlModuleParallelElements'
]
evlDistributedModules = [
    'EvlModuleJmsMasterBatch',
    'EvlModuleJmsMasterBatchLocal',
    'EvlModuleJmsMasterAtomic'
]
evlModules = ['EvlModule'] + evlParallelModules + evlDistributedModules
evlModulesDefault = evlModules[0:1] + [module + maxCoresStr for module in evlParallelModules] + [evlDistributedModules[1]+'1']
evlParallelModulesAllThreads = [module + str(numThread) for module in evlParallelModules for numThread in threads]

evlScenarios = [
    (javaMM, [s+'.evl' for s in javaValidationScripts], javaModels),
    (imdbMM, ['imdb_validator.evl'], imdbModels),
    (dblpMM, ['dblp_isbn.evl'], ['dblp-all.xmi']),
    (simulinkMM, ['simulink_live.evl'], [model + '.slx' for model in simulinkModels]),
    (simulinkMM, ['simulink_offline.evl'], [model + '.simulink' for model in simulinkModels])
]
evlModulesAndArgs = [[evlModulesDefault[0], '-module evl.'+evlModules[0]]]
for evlModule in evlParallelModules:
    modulePkg = '-module evl.concurrent.'
    if evlModule.endswith('Atoms'):
        modulePkg += 'atomic.'
    for numThread in threads:
        threadStr = str(numThread)
        evlModulesAndArgs.append([evlModule + threadStr, modulePkg+evlModule+' -parallelism '+threadStr])
programs.append(['EVL', epsilonJar, '', evlScenarios, evlModulesAndArgs, ''])

evlJmsJar = 'EVL-JMS'
evlJmsMaster = 'org.eclipse.epsilon.evl.distributed.jms.launch.JmsEvlMasterConfigParser'
for evlModule in evlDistributedModules:
    isLocal = 'Local' in evlModule
    isBatch = 'Batch' in evlModule
    if isLocal:
        for parallelism in threads:
            numWorker = int(threads[-1] / parallelism)
            workerStr = str(numWorker)
            threadStr = str(parallelism)
            evlDistArgs = distributedArgs +\
                ' -local -parallelism '+threadStr+' -workers '+workerStr +\
                ' -masterProportion '+ str(calculate_master_proportion(numWorker))
            programs.append([evlJmsJar, evlJmsJar, evlJmsMaster, evlScenarios[:3], [[evlModule + workerStr]], evlDistArgs])
    else:
        for numWorker in range(0, maxWorkers):
            workerStr = str(numWorker)
            evlDistArgs = distributedArgs+ \
                ' -workers '+workerStr+' -masterProportion '+ \
                str(calculate_master_proportion(numWorker))
            if isLocal:
                evlDistArgs += ' -local'
            if isBatch:
                evlDistArgs += ' -batches '+batchFactor
            programs.append([evlJmsJar, evlJmsJar, evlJmsMaster, evlScenarios[:3], [[evlModule + workerStr]], evlDistArgs])
            programs.append([evlJmsJar, evlJmsJar, evlJmsMaster, evlScenarios[3:], [[evlModule + workerStr]], evlDistArgs + ' -parallelism 1'])

oclModules = ['EOCL-interpreted', 'EOCL-compiled']
programs.append(['OCL', 'OCL', '', [(javaMM, [s+'.ocl' for s in javaValidationScripts], javaModels)], [[oclModules[0]]], ''])
programs.append(['OCL_'+javaValidationScripts[1], 'OCL_'+javaValidationScripts[1], '', [(javaMM, [javaValidationScripts[1]+'.ocl'], javaModels)], [[oclModules[1]]], ''])
validationModulesDefault = evlModulesDefault + oclModules
validationModulesScalabilityDefault = [evlModules[0], oclModules[0]]+evlParallelModulesAllThreads+oclModules[0:1]+[evlDistributedModules[1]+'1', evlDistributedModules[1]+'2']

# First-Order Operations (EOL, OCL, Java)
imdbEOLFOOPScripts = ['imdb_select', 'imdb_count', 'imdb_atLeastN', 'imdb_filter']
imdbOCLFOOPScripts = ['imdb_select']
imdbJavaFOOPScripts = ['imdb_filter', 'imdb_atLeastN', 'imdb_count']
eolModule = 'EolModule'
javaJar = 'JavaQuery'
javaModule = javaJar
javaModuleParallel = javaModule+'Parallel'
standardJavaModulesAndArgs = [[javaModule]]
parallelJavaModulesAndArgs = [[javaModuleParallel, '-parallel']]
eolModuleParallel = eolModule+'Parallel'
eolModulesDefault = [eolModule] + [eolModuleParallel+str(numThread) for numThread in threads[1:]]
queryModulesDefault = [oclModules[0], eolModulesDefault[0], eolModulesDefault[-1]]
eolModulesAndArgs = [[eolModule, '-module eol.'+eolModule]]
for numThread in threads:
    threadStr = str(numThread)
    eolModulesAndArgs.append([eolModuleParallel+threadStr, ' -parallelism '+threadStr])

programs.append(['EOL', epsilonJar, '', [(imdbMM, [s+'.eol' for s in imdbEOLFOOPScripts], imdbModels)], eolModulesAndArgs, ''])
for p in imdbJavaFOOPScripts:
    programs.append([javaModule, javaJar, '', [(imdbMM, [p+'.eol'], imdbModels)], standardJavaModulesAndArgs, ''])
    programs.append([javaModuleParallel, javaJar, '', [(imdbMM, [p+'.eol'], imdbModels)], parallelJavaModulesAndArgs, ''])
for p in imdbOCLFOOPScripts:
    programs.append(['OCL', 'OCL', '', [(imdbMM, [p+'.ocl'], imdbModels)], [[oclModules[0]]], ''])
    programs.append(['OCL_'+p, 'OCL_'+p, '', [(imdbMM, [p+'.ocl'], imdbModels)], [[oclModules[1]]], ''])

# Generate scenarios
if isGenerate:
    allSubs = []
    for program, programJar, classpath, scenarios, modulesAndArgs, additionalArgs in programs:
        selfContained = '_' in program
        isOCL = program.startswith('OCL')
        isJava = program.startswith('Java')
        isJms = program.startswith(evlJmsJar)
        programName = program if not selfContained else program.partition('_')[0]
        programSubset = []
        progFilePre = programName+'_run_'
        for metamodel, scripts, models in scenarios:
            for script in scripts:
                scriptSubset = []
                scriptName, scriptExt = os.path.splitext(script)
                for model in models:
                    modelSubset = []
                    modelName = os.path.splitext(model)[0]
                    for margs in modulesAndArgs:
                        moduleName = margs[0]
                        fileName = get_scenario_name(moduleName, scriptName, modelName)
                        command = sgeDirectives if sge else ''
                        command += jvmFlags
                        if jmc:
                            command += stdDir + fileName + '.jfr'
                        if isJava and len(margs) > 1 and 'parallel' in margs[1]:
                            script = normalize_foop(script)
                        if isJms:
                            basePathSub = '$BASEPATH$/'
                            scriptPath = 'scripts/'+script
                            outputPath = 'output/'+fileName
                            metamodelPath = basePathSub+'metamodels/'+metamodel
                            modelPath = basePathSub+'models/'+model
                        else:
                            scriptPath = scriptDir+script
                            metamodelPath = metamodelDir+metamodel
                            modelPath = modelDir+model
                            outputPath = stdDir+fileName
                        if jmc:
                            command += ' -Dcom.sun.management.jmxremote'
                        command += ' -'
                        command += 'cp' if classpath else 'jar'
                        command += ' "' + binDir + programJar+'.jar" '
                        if classpath:
                            command += classpath + ' '
                        if selfContained:
                            command += '"'+modelPath
                        elif isOCL:
                            command += '"'+scriptPath+'" "'+modelPath+'" "'+ metamodelPath
                        else:
                            command += '"'+scriptPath+'" -models "'
                            if modelPath.endswith('.slx'):
                                command += 'simulink.model.SimulinkModel#'+ \
                                    'cached=true,concurrent=false,file='+modelPath
                            else:
                                command += 'emf.EmfModel#cached=true,concurrent=true'+ \
                                    ',fileBasedMetamodelUri=file://'+metamodelPath+ \
                                    ',modelUri=file://'+modelPath
                        command += '" -profile'
                        if additionalArgs:
                            command += ' '+additionalArgs
                        if (len(margs) > 1 and margs[1]):
                            command += ' '+margs[1]
                        if len(stdDir) > 1:
                            command += ' -outfile "' + outputPath + intermediateExt + '"'
                        if repeats:
                            command += ' -repeats '+repeats
                        write_generated_file(fileName, [command])
                        modelSubset.append(subCmdPrefix +'"'+ fileName + fileExt + '"'+ subCmdSuffix)

                    if (len(models) > 1 and len(modulesAndArgs) > 1):
                        write_generated_file(progFilePre+scriptName+'_'+modelName, modelSubset)
                    
                    scriptSubset.extend(modelSubset)

                if len(scripts) > 1:
                    write_generated_file(progFilePre+scriptName, scriptSubset)
                
                programSubset.extend(scriptSubset)

        write_generated_file(progFilePre+'all', programSubset)
        allSubs.extend(programSubset)

    write_generated_file('run_all', allSubs)

    # Specific benchmark scenarios

    def write_all_operation_benchmark_scenarios(name = 'firstorder_all'):
        firstOrderScenarios = []
        for modelSize in imdbModels:
            modelName = imdbPrefix+modelSize
            for foopScript in imdbEOLFOOPScripts:
                firstOrderScenarios.append((eolModulesDefault[0], foopScript, modelName))
            for foopScript in imdbEOLFOOPScripts[:2]:
                for module in eolModulesDefault[1:]:
                    firstOrderScenarios.append((module, foopScript, modelName))
            for foopScript in imdbEOLFOOPScripts[2:]:
                firstOrderScenarios.append((eolModulesDefault[-1], foopScript, modelName))
            for foopScript in imdbJavaFOOPScripts:
                firstOrderScenarios.append((javaModule, foopScript, modelName))
            for foopScript in imdbOCLFOOPScripts:
                for module in oclModules:
                    firstOrderScenarios.append((module, foopScript, modelName))
        write_benchmark_scenarios(name, firstOrderScenarios)

    write_all_operation_benchmark_scenarios()

    write_benchmark_scenarios('select_'+imdbModels[7],
        [(module, imdbEOLFOOPScripts[0], imdbModels[7]) for module in eolModulesAndArgs[1:]]+
        [
            (eolModulesAndArgs[0], imdbEOLFOOPScripts[0], imdbModels[7]),
            (eolModulesAndArgs[0], imdbEOLFOOPScripts[1], imdbModels[7]),
            (eolModulesAndArgs[-1], imdbEOLFOOPScripts[1], imdbModels[7]),
        ]+
        [(module, imdbOCLFOOPScripts[0], imdbModels[7]) for module in oclModules]+
        [
            (standardJavaModulesAndArgs[0], imdbJavaFOOPScripts[0], imdbModels[7]),
            (parallelJavaModulesAndArgs[0], imdbJavaFOOPScripts[0], imdbModels[7])
        ]
    )

    eoloclscenarios = []
    for i in [0, 2, 3, 5, 8]:
        model = imdbModels[i].replace('.xmi', '')
        eoloclscenarios.extend([
            (oclModules[0], imdbOCLFOOPScripts[0], model),
            (oclModules[1], imdbOCLFOOPScripts[0], model),
            (eolModulesDefault[0], imdbEOLFOOPScripts[0], model),
            (eolModulesDefault[-1], imdbEOLFOOPScripts[0], model),
        ])
    write_benchmark_scenarios('select_EOLvsOCL', eoloclscenarios)

    write_benchmark_scenarios('atLeastN_'+imdbModels[4],
        [(module, imdbEOLFOOPScripts[2], imdbModels[4]) for module in eolModulesAndArgs[1:]]+[
            (standardJavaModulesAndArgs[0], imdbJavaFOOPScripts[1], imdbModels[4]),
            (parallelJavaModulesAndArgs[0], imdbJavaFOOPScripts[1], imdbModels[4])
        ]
    )

    write_benchmark_scenarios('count_'+imdbModels[5],
        [(module, imdbEOLFOOPScripts[3], imdbModels[5]) for module in eolModulesAndArgs[1:]]+[
            (standardJavaModulesAndArgs[0], imdbJavaFOOPScripts[2], imdbModels[5]),
            (parallelJavaModulesAndArgs[0], imdbJavaFOOPScripts[2], imdbModels[5])
        ]
    )
    
    write_benchmark_scenarios('thesis_query', [
        # select / filter 3.53m elements with default modules
        (oclModules[0], imdbOCLFOOPScripts[0], imdbModels[0]),
        (oclModules[1], imdbOCLFOOPScripts[0], imdbModels[0]),
        (eolModulesDefault[0], imdbEOLFOOPScripts[0], imdbModels[0]),
        (eolModulesDefault[-1], imdbEOLFOOPScripts[0], imdbModels[0]),
        (eolModulesDefault[0], imdbEOLFOOPScripts[-1], imdbModels[0]),
        (eolModulesDefault[-1], imdbEOLFOOPScripts[-1], imdbModels[0]),
        (javaModule, imdbJavaFOOPScripts[0], imdbModels[0]),
        (javaModuleParallel, imdbJavaFOOPScripts[0], imdbModels[0])
        # select / filter scalability with model size
    ]+[ (eolModulesDefault[0], imdbEOLFOOPScripts[0], imdbModel) for imdbModel in imdbModels[1:] ]+[
        (eolModulesDefault[-1], imdbEOLFOOPScripts[-1], imdbModel) for imdbModel in imdbModels[1:] ]+[
        (eolModulesDefault[0], imdbEOLFOOPScripts[-1], imdbModel) for imdbModel in imdbModels[1:] ]+[
        (eolModulesDefault[-1], imdbEOLFOOPScripts[-1], imdbModel) for imdbModel in imdbModels[1:] ]+[
        (javaModule, imdbJavaFOOPScripts[0], imdbModel) for imdbModel in imdbModels[1:] ]+[
        (javaModuleParallel, imdbJavaFOOPScripts[0], imdbModel) for imdbModel in imdbModels[1:]
        # select / filter scalability with threads
    ]+[ (eolMod, imdbEOLFOOPScripts[0], imdbModels[0]) for eolMod in eolModulesDefault[1:-1] ]+[
        (eolMod, imdbEOLFOOPScripts[-1], imdbModels[0]) for eolMod in eolModulesDefault[1:-1]
        # count with 2m elements
    ]+[ (eolModulesDefault[0], imdbEOLFOOPScripts[1], imdbModels[6]),
        (eolModulesDefault[-1], imdbEOLFOOPScripts[1], imdbModels[6]),
        (javaModule, imdbJavaFOOPScripts[1], imdbModels[6]),
        (javaModuleParallel, imdbJavaFOOPScripts[1], imdbModels[6])
        # atLeastN with 1m elements
    ]+[ (eolModulesDefault[0], imdbEOLFOOPScripts[2], imdbModels[4]),
        (eolModulesDefault[-1], imdbEOLFOOPScripts[2], imdbModels[4]),
        (javaModule, imdbJavaFOOPScripts[2], imdbModels[4]),
        (javaModuleParallel, imdbJavaFOOPScripts[2], imdbModels[4])
    ])

    # Note: does not include Simulink or distributed EVL as these are dependent on environment, so discretion is required
    write_benchmark_scenarios('thesis_validation', [
        (valMod, 'dblp_isbn', 'dblp-all') for valMod in evlModulesDefault
    ]+[ (valMod, javaValidationScripts[1], 'eclipseModel-all') for valMod in validationModulesDefault
    ]+[ (valMod, javaValidationScripts[0], javaModels[7]) for valMod in validationModulesDefault
    ]+[ (evlModulesDefault[0], javaValidationScripts[0], model) for model in javaModels ]+[
        (evlModulesDefault[-1], javaValidationScripts[0], model) for model in javaModels
    ]+[ (valMod, javaValidationScripts[2], javaModels[6]) for valMod in validationModulesScalabilityDefault ]+[
        (valMod, javaValidationScripts[3], javaModels[4]) for valMod in validationModulesScalabilityDefault
    ])

# Analysis / post-processing results
else:
    if (not os.path.isfile(resultsFileName) or os.stat(resultsFileName).st_size == 0):
        writer.writerow(columns)

    def compute_stats(stageName, regexObj, rawStr):
        times = []
        memories = []
        for resultMatch in re.findall(r'(?im)^(?:'+stageName+r'(?:\(\))?)(.*?)([0-9]+).?(?:ms|(?:millis(?:econds)?)).*?([0-9]+).{2,3}', rawStr):
            if resultMatch[1]:
                times.append(int(resultMatch[1]))
            if resultMatch[2]:
                memories.append(int(resultMatch[2]))
        timeStats = compute_descriptive_stats(times)
        memoryStats = compute_descriptive_stats(memories)
        return (timeStats, memoryStats)

    # First pass - transformation from multiple text files into CSV
    # Also computes mean and standard deviation from duplicates
    for dirpath, dirnames, filenames in os.walk(inDir):
        for filename in filter(lambda f: f.endswith(intermediateExt), filenames):
            fileNameMatch = re.match(fileNameRegex, filename)
            if not fileNameMatch:
                continue
        
            module = ''
            numThread = 1
            program = fileNameMatch.group(1)
            script = fileNameMatch.group(2)
            model = fileNameMatch.group(3)

            if program.startswith('EOCL'):
                module = program
                program = 'OCL'
            elif program.startswith('Java'):
                module = program
                program = 'Java'
            else:
                program = program[:3]
                moduleAndArgs = re.match(r'(?i)('+program+r'Module(?:[A-Za-z]*))([0-9]*)', filename)
                module = moduleAndArgs.group(1)
                numThread = moduleAndArgs.group(2)
                numThread = 1 if not numThread else int(numThread)
            
            row = [module, numThread, script, model]

            with open(os.path.join(dirpath, filename), 'r') as inFile:
                raw = inFile.read()
            
            execStats = compute_stats('execute', re, raw)
            modelStats = compute_stats('Loading model*', re, raw)
            row.extend(execStats[0])
            row.extend(execStats[1])
            row.extend(modelStats[0])
            row.extend(modelStats[1])
            row.append(program)

            rows.append(row)
        break   # Non-recursive

    # For reference, each row will eventually = [module, threads, script, model, execTimeMean, execTimeStdev, execSpeedup, execEfficiency, execMemoryMean, execMemoryStdev, modelTimeMean, modelTimeStdev, modelMemoryMean, modelMemoryStdev, program]
    def compute_metrics_closure(currentMetrics, relModule, row, filterCondition, relScript = row[2], decimalPlaces = 3):
        if (filterCondition):
            for nestedRow in rows:
                if (nestedRow[0] == relModule and (nestedRow[2] == relScript or nestedRow[2] == row[2]) and nestedRow[3] == row[3]):
                    speedup = round(nestedRow[4]/row[4], decimalPlaces) if nestedRow[4] and row[4] else None
                    efficiency = round(speedup/row[1], decimalPlaces) if speedup and row[1] else None
                    return (speedup, efficiency)
        return currentMetrics
    
    # Second pass - compute performance metrics, update rows and write to CSV file
    for i in range(0, len(rows)):
        row = rows[i]
        rowResults = row[:-1]
        # This is a nested for loop but with repetition factored out into a function
        metrics = (None, None)
        # Only one of the following calls will change the value in this iteration!
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[-1].upper() == 'EVL' or row[0] == oclModules[0])
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[0] == oclModules[1])
        metrics = compute_metrics_closure(metrics, eolModule, row, row[0] == eolModuleParallel, normalize_foop(row[2]))
        metrics = compute_metrics_closure(metrics, javaModule, row, row[0] == eolModule, normalize_foop(row[2]))
        metrics = compute_metrics_closure(metrics, javaModule, row, row[0] == eolModuleParallel, normalize_foop(row[2]))
        for oclModule in oclModules:
            metrics = compute_metrics_closure(metrics, oclModule, row, row[0] == eolModuleParallel, normalize_foop(row[2]))
            metrics = compute_metrics_closure(metrics, oclModule, row, row[0] == eolModule, normalize_foop(row[2]))
        
        rowResults.insert(6, metrics[0])
        rowResults.insert(7, metrics[1])
        
        writer.writerow(rowResults)
        rows[i] = rowResults

    resultsFile.close()

    if texFileName:
        # Third pass - write results to LaTeX file
        # see https://www.sharelatex.com/learn/Tables
        texFile = open(texFileName, 'w')
        texFile.write(
r'''\documentclass{article}
\usepackage{longtable}
\begin{document}
\LTcapwidth=\textwidth\setlength\LTleft{-3cm}
''')
        write_table(columns[:5]+columns[6:7]+columns[8:9], (row[:5]+row[6:7]+row[8:9] for row in rows), 'All results', 1, True)
        texFile.write(r'\end{document}')
        texFile.close()
