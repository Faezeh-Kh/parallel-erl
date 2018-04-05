import os, argparse, re, csv, statistics, math, multiprocessing

parser = argparse.ArgumentParser()
parser.add_argument('mode', choices=['GENERATE', 'ANALYSE'], help='What do you want the program to do?')
parser.add_argument('--rootDir', help='The base directory. For all unspecified paths, everything will be assumed to be relative to this. Default value is current directory.')
parser.add_argument('--inDir', help='Input directory.')
parser.add_argument('--csvFile', help='Output file for the results (must be full path and CSV extension).')
parser.add_argument('--texFile', help='Output file for the results in LaTeX tabluar format (must be full path). Leave blank for default.')
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
jar = '.jar'
xmi = '.xmi'
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
    resultsFileName = defaultPath(args.csvFile, resultsDir+'results.csv')[:-1]
    resultsFileName += resultsExt if not resultsFileName.endswith(resultsExt) else ''
    texFileName = defaultPath(args.texFile, resultsDir+'results.tex')[:-1] if args.texFile else None
sge = args.sge
jmc = args.jmc
smt = args.smt
yarccCores = 12
logicalCores = yarccCores if sge else multiprocessing.cpu_count()	#os.cpu_count() doesn't work on Linux
fileExt = '.cmd' if (os.name == 'nt' and not sge) else '.sh' 
resultsRegex = r'(?i)(?:execute\(\)).{2}((?:[0-9]{1,2}:){0,3}(?:[0-9]{2})\.[0-9]{3}).{1,2}(?:([0-9]+).{0,1}(?:ms|(?:millis(?:econds)?)).).{2}([0-9]+).{2,3}'
fileNameRegex = r'(.*)_(.*_.*)_(.*)(\.txt)' #Script name must be preceded by metamodel!
resultsFile = open(resultsFileName, 'w') if not isGenerate else None
writer = csv.writer(resultsFile, lineterminator='\n') if not isGenerate else None
rows = []
columns = ['MODULE', 'THREADS', 'SCRIPT', 'MODEL', 'TIME', 'TIME_STDEV', 'SPEEDUP', 'EFFICIENCY', 'MEMORY', 'MEMORY_STDEV', 'MEMORY_DELTA']

sgeDirectives = '''export MALLOC_ARENA_MAX='''+str(round(yarccCores/4))+'''
#$ -cwd
#$ -o ../../stdout
#$ -e ../../stderr
#$ -l exclusive=TRUE
#$ -l ram-128=TRUE
#$ -l E5-2650v4=TRUE
#$ -binding linear:'''+str(yarccCores)+'''
#$ -pe smp '''+str(yarccCores)+'''
#$ -l h_vmem='''+str(60/yarccCores)+'''G
#$ -l h_rt=7:59:59
'''
jvmFlags = 'java -Xms640m -XX:MaxRAMPercentage=70 -XX:+UseParallelOldGC -XX:+AggressiveOpts'
if sge:
    jvmFlags += ' -XX:+UseNUMA'
if jmc:
    jvmFlags += ' -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=dumponexit=true,filename='

subCmdPrefix = 'qsub ' if sge else 'call ' if os.name == 'nt' else ''
subCmdSuffix = nL
threads = [1]
threadCounter = 1
while threadCounter < logicalCores:
    threadCounter *= 2
    if (threadCounter >= logicalCores):
        threads.append(logicalCores)
        break
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
    return moduleConfig+'_'+script+'_'+model

def write_generated_file(filename, lines):
    with open(genDir + filename + fileExt, 'w') as qsbFile:
        qsbFile.writelines(lines)

def write_benchmark_scenarios(name, scenariosArgs):
    lines = [subCmdPrefix+get_scenario_name(module, script, model)+fileExt+subCmdSuffix for (module, script, model) in scenariosArgs]
    write_generated_file(name+'_benchmarks', lines*3)

# (Meta)Models
# Java models can be obtained from http://atenea.lcc.uma.es/index.php/Main_Page/Resources/LinTra#Java_Refactoring
javaMM = 'java.ecore'
imdbMM = 'movies.ecore'
dblpMM = 'dblp.ecore'
dblpModels = ['dblp-all'+xmi]
eclipsePrefix = 'eclipseModel-'
imdbPrefix = 'imdb-'
imdbRanges = ['all', '0.1', '0.2'] + [str(round(i/10, 1)) for i in range(5, 35, 5)]
eclipseRanges = imdbRanges + imdbRanges + ['3.5', '4.0']
imdbModels = [imdbPrefix + imdbR + xmi for imdbR in imdbRanges]
javaModels = [eclipsePrefix + eclipseR + xmi for eclipseR in eclipseRanges]

#Scripts
javaValidationScripts = [
    'java_findbugs',
    'java_manyConstraint1Context',
    'java_manyContext1Constraint',
    'java_1Constraint'
]

#EVL
evlParallelModules = [
    'EvlModuleParallelStaged',
    'EvlModuleParallelElements',
    'EvlModuleParallelConstraints',
    # 'EvlModuleParallelContexts',
    # 'EvlModuleParallelThreads
]
evlModules = ['EvlModule'] + evlParallelModules
evlModulesDefault = evlModules[0:1] + [module + maxThreadsStr for module in evlParallelModules]
evlParallelModulesAllThreads = [module + str(numThread) for module in evlParallelModules for numThread in threads]

evlScenarios = [
    (javaMM, [s+'.evl' for s in javaValidationScripts], javaModels),
    #(imdbMM, ['imdb_validator.evl'], imdbModels),
    #(dblpMM, ['dblp_isbn.evl'], dblpModels)
]
evlModulesAndArgs = [[evlModulesDefault[0]]]
for evlModule in evlParallelModules:
    for numThread in threads:
        threadStr = str(numThread)
        evlModulesAndArgs.append([evlModule+threadStr, '-module evl.concurrent.'+evlModule+' int='+threadStr])
programs.append(['EVL', evlScenarios, evlModulesAndArgs])

#OCL
oclModules = ['EOCL-interpreted', 'EOCL-compiled']
programs.append(['OCL', [(javaMM, [s+'.ocl' for s in javaValidationScripts], javaModels)], [[oclModules[0]]]])
programs.append(['OCL_'+javaValidationScripts[0], [(javaMM, [javaValidationScripts[0]+'.ocl'], javaModels)], [[oclModules[1]]]])

validationModulesDefault = evlModulesDefault + oclModules

if isGenerate:
    allSubs = []
    for program, scenarios, modulesAndArgs in programs:
        programSubset = []
        progFilePre = program+'_run_'
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
                        stdFile = genDir + fileName + fileExt
                        command = sgeDirectives if sge else ''
                        command += jvmFlags
                        if jmc:
                            command += stdDir + fileName + '.jfr'
                        command += ' -jar "'+ \
                            binDir + program + jar +'" "'+ \
                            scriptDir+script +'" "'+ \
                            modelDir+model +'" "'+ \
                            metamodelDir+metamodel +'" -profile'
                        if (len(margs) > 1 and margs[1]):
                            command += ' '+margs[1]
                        if len(stdDir) > 1:
                            command += ' -outfile "' + stdDir + fileName + intermediateExt + '"'

                        with open(stdFile, 'w') as output:
                            output.write(command + nL)

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

    write_benchmark_scenarios('validation', # 34 unique scenarios
        [
            # 4m elements
            (module, 'java_findbugs', 'eclipseModel-4.0') for module in validationModulesDefault[:-2]+validationModulesDefault[-1:]
        ]+[
            # 1m elements
            (module, 'java_findbugs', 'eclipseModel-1.0') for module in validationModulesDefault[:-2]+validationModulesDefault[-1:]
        ]+[
            # 200k elements
            (module, 'java_findbugs', 'eclipseModel-0.2') for module in validationModulesDefault[:-2]+validationModulesDefault[-1:]
        ]+[
            # Single-threaded efficiency
            (module.replace(maxThreadsStr, '1'), 'java_findbugs', 'eclipseModel-3.0') for module in validationModulesDefault#[:-1]
        ]+[
            # Thread scalability for 2m elements
            (module, 'java_findbugs', 'eclipseModel-2.0') for module in evlParallelModulesAllThreads+oclModules[0:1]
        ]+[
            # Single context
            (module, 'java_manyConstraint1Context', 'eclipseModel-2.5') for module in validationModulesDefault[:-1]
        ]+[
            # Single constraint
            (module, 'java_1Constraint', 'eclipseModel-all') for module in validationModulesDefault[:-1]
        ]
    )
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
    def compute_metrics_closure(currentMetrics, relModule, row, filterCondition, decimalPlaces = 3):
        if (row[0] != relModule and filterCondition):
            for nestedRow in rows:
                if (nestedRow[0] == relModule and nestedRow[2] == row[2] and nestedRow[3] == row[3]):
                    speedup = round(nestedRow[4]/row[4], decimalPlaces) if nestedRow[4] and row[4] else None
                    efficiency = round(speedup/row[1], decimalPlaces) if speedup else None
                    memDelta = round(nestedRow[6]/row[6], decimalPlaces) if nestedRow[6] and row[6] else None
                    return (speedup, efficiency, memDelta)
        return currentMetrics

    # Second pass - compute performance metrics, update rows and write to CSV file
    for i in range(0, len(rows)):
        row = rows[i]
        rowResults = row[:-1]
        # This is a nested for loop but with repetition factored out into a function
        metrics = (None, None, None)
        # Only one of the following will change the value in this iteration!
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[-1].upper() == 'EVL' or row[0] == oclModules[0])
        metrics = compute_metrics_closure(metrics, evlModules[0], row, row[0] == oclModules[1])

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
