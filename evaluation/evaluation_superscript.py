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
parser.add_argument('--jmc', help='Enable application profiling using Java Flight Recorder.', action='store_true')
parser.add_argument('--sge', help='Output for YARCC.', action='store_true')
parser.add_argument('--smt', help='Whether the system uses Hyper-Threading technology.', action='store_true')
parser.add_argument('--numa', help='Enable Non-uniform memory access option.', action='store_true')
parser.add_argument('--g1gc', help='Use the default G1 garbage collector.', action='store_true')
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

sge = args.sge
g1gc = args.g1gc
jmc = args.jmc
smt = args.smt
numa = args.numa
logicalCores = 24 if sge else os.cpu_count()
fileExt = '.cmd' if (os.name == 'nt' and not sge) else '.sh' 
resultsRegex = r'(?i)(?:execute\(\)).{2}((?:[0-9]{1,2}:){0,3}(?:[0-9]{2})\.[0-9]{3}).{1,2}(?:([0-9]+).{0,1}(?:ms|(?:millis(?:econds)?)).).{2}([0-9]+).{2,3}'
fileNameRegex = r'(.*)_(.*_.*)_(.*)(\.txt)' # Script name must be preceded by metamodel!
resultsFile = open(resultsFileName, 'w') if not isGenerate else None
writer = csv.writer(resultsFile, lineterminator='\n') if not isGenerate else None
rows = []
columns = ['MODULE', 'THREADS', 'SCRIPT', 'MODEL', 'TIME', 'TIME_STDEV', 'SPEEDUP', 'EFFICIENCY', 'MEMORY', 'MEMORY_STDEV', 'MEMORY_DELTA']

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
jvmFlags = 'java -Xms768m -XX:MaxRAM'
jvmFlags += 'Fraction=1' if sge else 'Percentage=90'
jvmFlags += ' -XX:'
jvmFlags += 'MaxGCPauseMillis=730' if g1gc else '+UseParallelOldGC'
if numa:
    jvmFlags += ' -XX:+UseNUMA'
if jmc:
    jvmFlags += ' -XX:+FlightRecorder -XX:StartFlightRecording=dumponexit=true,filename='

subCmdPrefix = 'qsub ' if sge else 'call ' if os.name == 'nt' else './'
subCmdSuffix = nL

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
maxThreadsStr = str(round(threads[-1]/2) if smt else threads[-1])
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

def write_benchmark_scenarios(name, scenariosArgs):
    lines = [subCmdPrefix+get_scenario_name(module, script, model)+fileExt+subCmdSuffix for (module, script, model) in scenariosArgs]
    write_generated_file(name+'_benchmarks', lines*5)

# (Meta)Models
# Java models can be obtained from http://atenea.lcc.uma.es/index.php/Main_Page/Resources/LinTra#Java_Refactoring
javaMM = 'java.ecore'
imdbMM = 'movies.ecore'
dblpMM = 'dblp.ecore'
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
    'java_manyConstraint1Context',
    'java_manyContext1Constraint',
    'java_1Constraint',
    'java_equals'
    #'java_noguard'
]

evlParallelModules = [
    'EvlModuleParallelAnnotation',
    'EvlModuleParallelElements'
]
evlModules = ['EvlModule'] + evlParallelModules
evlModulesDefault = evlModules[0:1] + [module + maxThreadsStr for module in evlParallelModules]
evlParallelModulesAllThreads = [module + str(numThread) for module in evlParallelModules for numThread in threads]

evlScenarios = [
    (javaMM, [s+'.evl' for s in javaValidationScripts], javaModels),
    (imdbMM, ['imdb_validator.evl'], imdbModels),
    (dblpMM, ['dblp_isbn.evl'], ['dblp-all.xmi'])
]
evlModulesAndArgs = [[evlModulesDefault[0], '-module evl.'+evlModules[0]]]
for evlModule in evlParallelModules:
    for numThread in threads:
        threadStr = str(numThread)
        evlModulesAndArgs.append([evlModule+threadStr, '-module evl.concurrent.'+evlModule+' int='+threadStr])
programs.append(['EVL', evlScenarios, evlModulesAndArgs])

oclModules = ['EOCL-interpreted', 'EOCL-compiled']
programs.append(['OCL', [(javaMM, [s+'.ocl' for s in javaValidationScripts], javaModels)], [[oclModules[0]]]])
programs.append(['OCL_'+javaValidationScripts[1], [(javaMM, [javaValidationScripts[1]+'.ocl'], javaModels)], [[oclModules[1]]]])

validationModulesDefault = evlModulesDefault + oclModules

# First-Order Operations (EOL, OCL, Java)
imdbFOOPScripts = ['imdb_select', 'imdb_count', 'imdb_atLeastN', 'imdb_filter']
imdbOCLFOOPScripts = ['imdb_select']
imdbJavaFOOPScripts = ['imdb_filter', 'imdb_atLeastN']
imdbParallelJavaFOOPScripts = ['imdb_parallelFilter', 'imdb_parallelAtLeastN']
imdbParallelFOOPScripts = ['imdb_parallelSelect', 'imdb_parallelCount', 'imdb_parallelAtLeastN', 'imdb_parallelFilter']
eolModule = 'EolModule'
javaModule = 'JavaQuery'
standardJavaModulesAndArgs = [[javaModule]]
parallelJavaModulesAndArgs = [[javaModule, '-parallel']]
eolModuleParallel = eolModule+'Parallel'
eolModulesDefault = [eolModule] + [eolModuleParallel+str(numThread) for numThread in threads[1:]]
eolModulesAndArgs = [[eolModule, '-module eol.'+eolModule]]
for numThread in threads:
    threadStr = str(numThread)
    eolModulesAndArgs.append([eolModuleParallel+threadStr, '-module eol.concurrent.'+eolModuleParallel+' int='+threadStr])

programs.append(['EOL', [(imdbMM, [s+'.eol' for s in imdbFOOPScripts], imdbModels)], eolModulesAndArgs[0:1]])
programs.append(['EOL', [(imdbMM, [s+'.eol' for s in imdbParallelFOOPScripts], imdbModels)], eolModulesAndArgs[1:]])
for p in imdbJavaFOOPScripts:
    programs.append([javaModule, [(imdbMM, [p], imdbModels)], standardJavaModulesAndArgs])
for p in imdbParallelJavaFOOPScripts:
    programs.append([javaModule, [(imdbMM, [p], imdbModels)], parallelJavaModulesAndArgs])

for p in imdbOCLFOOPScripts:
    programs.append(['OCL', [(imdbMM, [p+'.ocl'], imdbModels)], [[oclModules[0]]]])
    programs.append(['OCL_'+p, [(imdbMM, [p+'.ocl'], imdbModels)], [[oclModules[1]]]])

# Generate scenarios
if isGenerate:
    allSubs = []
    for program, scenarios, modulesAndArgs in programs:
        selfContained = '_' in program
        isOCL = program.startswith('OCL')
        isJava = program.startswith('Java')
        programName = program if not selfContained else program.partition('_')[0]
        programCommand = program if isOCL or isJava or selfContained else 'epsilon-engine'
        programSubset = []
        progFilePre = programName+'_run_'
        for metamodel, scripts, models in scenarios:
            for script in scripts:
                scriptSubset = []
                scriptName = os.path.splitext(script)[0]
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
                        command += ' -jar "'+ binDir +programCommand+'.jar" "'+ scriptDir+script +'" '
                        
                        if isOCL:
                            command += '"'+modelDir+model +'" "'+ metamodelDir+metamodel
                        else:
                            command += '-models "emf.EmfModel#cached=true,concurrent=true'+ \
                            ',fileBasedMetamodelUri=file:///'+ metamodelDir+metamodel+ \
                            ',modelUri=file:///' + modelDir+model
                        command += '" -profile'
                        
                        if (len(margs) > 1 and margs[1]):
                            command += ' '+margs[1]
                        if len(stdDir) > 1:
                            command +=  ' -outfile "' + stdDir + fileName + intermediateExt + '"'

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
            for foopScript in imdbFOOPScripts:
                firstOrderScenarios.append((eolModulesDefault[0], foopScript, modelName))
            for foopScript in imdbParallelFOOPScripts[:2]:
                for module in eolModulesDefault[1:]:
                    firstOrderScenarios.append((module, foopScript, modelName))
            for foopScript in imdbParallelFOOPScripts[2:]:
                firstOrderScenarios.append((eolModulesDefault[-1], foopScript, modelName))
            for foopScript in imdbJavaFOOPScripts:
                firstOrderScenarios.append((javaModule, foopScript, modelName))
            for foopScript in imdbOCLFOOPScripts:
                for module in oclModules:
                    firstOrderScenarios.append((module, foopScript, modelName))
        write_benchmark_scenarios(name, firstOrderScenarios)

    write_all_operation_benchmark_scenarios()

    write_benchmark_scenarios('select_imdb-2.5',
        [(module, imdbParallelFOOPScripts[0], 'imdb-2.5') for module in eolModulesAndArgs[1:]]+
        [
            (eolModulesAndArgs[0], imdbFOOPScripts[0], 'imdb-2.5'),
            (eolModulesAndArgs[0], imdbFOOPScripts[1], 'imdb-2.5'),
            (eolModulesAndArgs[-1], imdbParallelFOOPScripts[1], 'imdb-2.5'),
        ]+
        [(module, imdbOCLFOOPScripts[0], 'imdb-2.5') for module in oclModules]+
        [
            (standardJavaModulesAndArgs[0], imdbJavaFOOPScripts[0], 'imdb-2.5'),
            (parallelJavaModulesAndArgs[0], imdbParallelJavaFOOPScripts[0], 'imdb-2.5')
        ]
    )

    eoloclscenarios = []
    for i in [0, 2, 3, 5, 8]:
        model = imdbModels[i]
        eoloclscenarios.extend([
            (oclModules[0], imdbOCLFOOPScripts[0], model),
            (oclModules[1], imdbOCLFOOPScripts[0], model),
            (eolModulesDefault[0], imdbFOOPScripts[0], model),
            (eolModulesDefault[-1], imdbParallelFOOPScripts[0], model)
        ])
    write_benchmark_scenarios('select_EOLvsOCL', eoloclscenarios)

    write_benchmark_scenarios('validation',
        [(module, 'dblp_isbn', 'dblp-all') for module in evlModulesDefault]+
        [(module, 'java_simple', 'eclipseModel-all') for module in validationModulesDefault]+
        [(module, 'java_simple', 'eclipseModel-2.5') for module in validationModulesDefault]+
        [(module, 'java_simple', 'eclipseModel-1.0') for module in validationModulesDefault]+
        [(module, 'java_simple', 'eclipseModel-0.2') for module in validationModulesDefault]+
        [(module.replace(maxThreadsStr, '1'), 'java_simple', 'eclipseModel-3.0') for module in validationModulesDefault]+
        [(module, 'java_findbugs', 'eclipseModel-2.0') for module in evlParallelModulesAllThreads+oclModules[0:1]]+
        [(module, 'java_manyConstraint1Context', 'eclipseModel-2.5') for module in validationModulesDefault[:-1]]+
        #[(module, 'java_manyContext1Constraint', 'eclipseModel-2.5') for module in validationModulesDefault[:-1]]+
        [(module, 'java_1Constraint', 'eclipseModel-all') for module in validationModulesDefault[:-1]]+
        [(module, 'java_findbugs', 'eclipseModel-3.0') for module in validationModulesDefault[:-1]]
    )

# Analysis / post-processing results
else:
    if (not os.path.isfile(resultsFileName) or os.stat(resultsFileName).st_size == 0):
        writer.writerow(columns)

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

            if (program.startswith('EOCL')):
                module = program
                program = 'OCL'
            else:
                program = program[:3]
                moduleAndArgs = re.match(r'('+program+r'(?i)Module(?:[A-Za-z]*))([0-9]*)', filename)
                module = moduleAndArgs.group(1)
                numThread = moduleAndArgs.group(2)
                numThread = 1 if not numThread else int(numThread)
            
            row = [module, numThread, script, model]
            times = []
            memories = []

            with open(os.path.join(dirpath, filename), 'r') as inFile:
                raw = inFile.read()
            
            for resultMatch in re.findall(resultsRegex, raw):
                if resultMatch[1]:
                    times.append(int(resultMatch[1]))
                if resultMatch[2]:
                    memories.append(int(resultMatch[2]))

            timeStats = compute_descriptive_stats(times)
            memoryStats = compute_descriptive_stats(memories)

            row.extend(timeStats)
            row.extend(memoryStats)
            row.append(program)
            
            rows.append(row)
        break   #Non-recursive

    # For reference, each row = [module, threads, script, model, timeMean, timeStdev, memoryMean, memoryStdev, memoryDelta, program]
    def compute_metrics_closure(currentMetrics, relModule, row, filterCondition, relScript = row[2], decimalPlaces = 3):
        if (filterCondition):
            for nestedRow in rows:
                if (nestedRow[0] == relModule and (nestedRow[2] == relScript or nestedRow[2] == row[2]) and nestedRow[3] == row[3]):
                    speedup = round(nestedRow[4]/row[4], decimalPlaces) if nestedRow[4] and row[4] else None
                    efficiency = round(speedup/row[1], decimalPlaces) if speedup else None
                    memDelta = round(nestedRow[6]/row[6], decimalPlaces) if nestedRow[6] and row[6] else None
                    return (speedup, efficiency, memDelta)
        return currentMetrics

    def normalize_foop(script):
        normalFOOP = script.replace('_parallel', '_')
        if len(normalFOOP) == len(script):
            return script
        metamodelScriptIndex = script.index('_')+1
        normalFOOP = normalFOOP[0:metamodelScriptIndex] + normalFOOP[metamodelScriptIndex].lower() + normalFOOP[metamodelScriptIndex+1:] if normalFOOP else script
        return normalFOOP

    # Second pass - compute performance metrics, update rows and write to CSV file
    for i in range(0, len(rows)):
        row = rows[i]
        rowResults = row[:-1]
        # This is a nested for loop but with repetition factored out into a function
        metrics = (None, None, None)
        # Only one of the following will change the value in this iteration!
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[-1].upper() == 'EVL' or row[0] == oclModules[0])
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[0] == oclModules[1])
        metrics = compute_metrics_closure(metrics, eolModule, row, row[0] == eolModuleParallel, normalize_foop(row[2]))

        rowResults.insert(6, metrics[0])
        rowResults.insert(7, metrics[1])
        rowResults.insert(10, metrics[2])
        
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
