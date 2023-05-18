#
# Program: straincuratorload.py
#
# Inputs:
#
#	A tab-delimited file in the format:
#
#       field 1: MGI:Strain ID
#       field 2: MGI Allele ID : not required : pipe delimited 
#		if Private = No, then allele status must be Approved or Autoloaded
#       field 3: Strain Name
#       field 4: Standard (1/0)
#       field 5: Private (1/0)
#       field 6: Modified by
#
# Outputs:
#
#       2 BCP files:
#
#       PRB_Strain_Marker.bcp           master Strain records
#       MGI_Synonym.bcp
#
#       Diagnostics file of all input parameters and SQL commands
#       Error file
#
# History
#
# lec	05/12/2023
#	- wts2-902/flr-344/Strain Curator easy update load (part 1)
#

import sys
import os
import db
import mgi_utils
import loadlib

#db.setTrace()

inputFileName = sys.argv[1]
mode = sys.argv[2]

lineNum = 0
hasError = 0

diagFileName = os.environ['LOG_DIAG']
errorFileName = os.environ['LOG_ERROR']
inputFile = os.environ['INPUTDIR']
outputFile = os.environ['OUTPUTDIR']

diagFile = ''
errorFile = ''
markerFile = ''
synonymFile = ''

markerTable = 'PRB_Strain_Marker'
synonymTable = 'MGI_Synonym'
markerFileName = markerTable + '.bcp'
synonymFileName = synonymTable + '.bcp'

updateSQL = ''

strainmarkerKey = 0	# PRB_Strain_Marker._StrainMarker_key
synonymKey = 0          # MGI_Synonym._Synonym_key
hasStrainMarker = 0
hasSynonym = 0

mgiTypeKey = 10		# ACC_MGIType._MGIType_key for Strains
alleleTypeKey = 11      # ACC_MGIType._MGIType_key for Allele
markerTypeKey = 2       # ACC_MGIType._MGIType_key for Marker

synonymTypeKey = 1001   # MGI_SynonymType._SynonymType_key
qualifierKey = 615427	# nomenclature

cdate = mgi_utils.date('%m/%d/%Y')	# current date
 
# Purpose: prints error message and exits
# Returns: nothing
# Assumes: nothing
# Effects: exits with exit status
# Throws: nothing
def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (string)
    ):

    if message is not None:
        sys.stderr.write('\n' + str(message) + '\n')
 
    try:
        diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))

        if hasError == 0:
                errorFile.write("\nSanity check : successful\n")
        else:
                errorFile.write("\nSanity check : failed")
                errorFile.write("\nErrors must be fixed before file is published.\n")

        errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        diagFile.close()
        errorFile.close()
        inputFile.close()
    except:
        pass

    db.useOneConnection(0)
    sys.exit(status)
 
# Purpose: process command line options
# Returns: nothing
# Assumes: nothing
# Effects: initializes global variables
#          exits if files cannot be opened
# Throws: nothing
def init():
    global diagFile, errorFile, inputFile
    global markerFile, synonymFile
 
    #if mode == "preview":
    #    diagFileName = inputFileName + '.diagnostics'
    #    errorFileName = inputFileName + '.error'

    try:
        diagFile = open(diagFileName, 'a')
    except:
        exit(1, 'Could not open file %s\n' % diagFile)
                
    try:
        errorFile = open(errorFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % errorFile)
                
    try:
        inputFile = open(inputFileName, 'r', encoding="latin-1")
    except:
        exit(1, 'Could not open file %s\n' % inputFileName)
    
    if mode != "preview":
        try:
                markerFile = open(outputFile + '/' + markerFileName, 'w')
        except:
                exit(1, 'Could not open file %s\n' % markerFileName)

        try:
                synonymFile = open(outputFile + '/' + synonymFileName, 'w')
        except:
                exit(1, 'Could not open file %s\n' % synonymFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer()))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

    errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    return

# Purpose:  verify Strain
# Returns:  Strain Key
# Assumes:  nothing
# Effects:  verifies that the Strain exists
#	writes to the error file if the Strain is invalid
# Throws:  nothing
def verifyStrain(
    strainID, 	# Strain ID (string)
    lineNum	# line number (integer)
    ):

    strainKey = 0
    oldName = ''

    results = db.sql('''select s._strain_key, s.strain 
        from ACC_Accession a, PRB_Strain s
        where a._mgitype_key = 10 
        and a._logicaldb_key = 1 
        and a.accid = \'%s\'
        and a._object_key = s._strain_key
        ''' % (strainID), 'auto')

    for r in results:
        strainKey = r['_strain_key']
        oldName = r['strain']

    if strainKey == 0:
            errorFile.write('Invalid Strain (%d) %s\n' % (lineNum, strainID))

    return strainKey, oldName

# Purpose:  verify Allele
# Returns:  Allele Key, Marker Key, Allele Status Key
# Assumes:  nothing
# Effects:  verifies that the Allele & Marker (can be null) exists
#	writes to the error file if the Allele is invalid
# Throws:  nothing
def verifyAllele(
    alleleID, 	# Allele ID (string)
    lineNum	# line number (integer)
    ):

    alleleKey = 0
    markerKey = 0
    alleleStatusKey = 0
    alleleStatus = ""

    results = db.sql('''select s._allele_key, s._marker_key, s._allele_status_key, t.term
        from ACC_Accession a, ALL_Allele s, VOC_Term t
        where a._mgitype_key = 11 
        and a._logicaldb_key = 1 
        and a.accid = \'%s\'
        and a._object_key = s._allele_key
	and s._allele_status_key = t._term_key
	and s._marker_key is not null
        ''' % (alleleID), 'auto')

    for r in results:
        alleleKey = r['_allele_key']
        markerKey = r['_marker_key']
        alleleStatusKey = r['_allele_status_key']
        alleleStatus = r['term']

    if alleleKey == 0:
            errorFile.write('Invalid Allele (%d) %s\n' % (lineNum, alleleID))

    return alleleKey, markerKey, alleleStatusKey, alleleStatus

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing
def setPrimaryKeys():

    global strainKey, strainmarkerKey, accKey, mgiKey, annotKey, synonymKey

    results = db.sql(''' select nextval('prb_strain_marker_seq') as maxKey ''', 'auto')
    strainmarkerKey = results[0]['maxKey']

    results = db.sql(''' select nextval('mgi_synonym_seq') as maxKey ''', 'auto')
    synonymKey = results[0]['maxKey']

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing
def processFile():

    global lineNum
    global strainmarkerKey, synonymKey
    global updateSQL
    global hasError
    global hasStrainMarker, hasSynonym

    # For each line in the input file

    for line in inputFile.readlines():

        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = line[:-1].split('\t')

        try:
            strainID = tokens[0]
            alleleIDs = tokens[1]
            name = tokens[2]
            isStandard = tokens[3]
            isPrivate = tokens[4] 
            modifiedBy = tokens[5] 
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

        # skip header line
        if strainID == 'MGI:Strain ID':
                continue

        strainKey, oldName = verifyStrain(strainID, lineNum)
        modifiedByKey = loadlib.verifyUser(modifiedBy, lineNum, errorFile)

        if strainKey == 0 or modifiedByKey == 0:
            # set error flag to true
            hasError = 1

        # if errors, continue to next record
        if hasError == 1:
            continue

        # if no errors, process

        if len(alleleIDs) > 0:

            allAlleles = alleleIDs.split('|')

            for a in allAlleles:

                alleleKey, markerKey, alleleStatusKey, alleleStatus = verifyAllele(a, lineNum)

                if alleleKey == 0 or alleleKey == None:
                    hasError = 1
                    continue

		# if Private = No, then allele status must be Approved or Autoloaded
                if isPrivate == 0 and alleleStatusKey not in (847114,3983021):
                    hasError = 1
                    errorFile.write('Invalid Allele ID/Private/Status (%d) %s,%s,%s\n' % (lineNum, a, isPrivate, alleleStatus))
                    continue

                if mode == "preview":
                        continue

                markerFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
                    % (strainmarkerKey, strainKey, markerKey, alleleKey, qualifierKey, modifiedByKey, modifiedByKey, cdate, cdate))

                strainmarkerKey = strainmarkerKey + 1
                hasStrainMarker = 1

        if mode == "preview":
                continue

        updateSQL = updateSQL + \
	'''update PRB_Strain set strain = \'%s\', standard = %s, private = %s, _modifiedby_key = %s, modification_date = now() where _Strain_key = %s;\n''' \
	% (name, isStandard, isPrivate, modifiedByKey, strainKey)

        if name != oldName:
                synonymFile.write('%d|%d|%d|%d||%s|%s|%s|%s|%s\n' \
                        % (synonymKey, strainKey, mgiTypeKey, synonymTypeKey, oldName, modifiedByKey, modifiedByKey, cdate, cdate))
                synonymKey = synonymKey + 1
                hasSynonym = 1

    #	end of "for line in inputFile.readlines():"

    return

# Purpose:  processes bcp files
# Returns:  nothing
# Assumes:  configuration env is set properly
# Effects:  BCPs the data into the database
# Throws:   nothing
def bcpFiles():

    # do not process if using "preview" mode
    if mode == "preview":
        return

    # do not process if errors are detected
    #if hasError == 1:
    #    errorFile.write("\nCannot process this file.  Sanity check failed\n")
    #    return

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'
    db.commit()
    markerFile.flush()
    synonymFile.flush()

    if hasStrainMarker == 1:
    	bcp1 = '%s %s %s %s %s %s "|" "\\n" mgd' % (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), markerTable, outputFile, markerFileName)
    	diagFile.write('%s\n' % bcp1)
    	os.system(bcp1)
    	# update prb_strain_marker_seq auto-sequence
    	db.sql(''' select setval('prb_strain_marker_seq', (select max(_StrainMarker_key) from PRB_Strain_Marker)) ''', None)
    	db.commit()

    if hasSynonym == 1:
    	bcp2 = '%s %s %s %s %s %s "|" "\\n" mgd' % (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), synonymTable, outputFile, synonymFileName)
    	diagFile.write('%s\n' % bcp2)
    	os.system(bcp2)
    	# update mgi_synonym_seq auto-sequence
    	db.sql(''' select setval('mgi_synonym_seq', (select max(_Synonym_key) from MGI_Synonym)) ''', None)
    	db.commit()

    if len(updateSQL) > 0:
        diagFile.write('running updateSQL...\n')
        diagFile.write(updateSQL)
        db.sql(updateSQL, None)
        db.commit()

    return

#
# Main
#

init()
setPrimaryKeys()
processFile()
bcpFiles()
exit(0)

