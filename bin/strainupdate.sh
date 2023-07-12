#!/bin/sh
#
# Purpose:
#	wrapper for strainupdate.py
#
# History
#
# lec   07/12/2023
#	- wts2-1198/fl2-410/Moving WTS2-686 to curator control
#
# lec   05/12/2023
#	- wts2-902/flr-344/Strain Curator easy update load (part 1)
#

BINDIR=`dirname $0`
COMMON_CONFIG=`cd ${BINDIR}/..; pwd`/strainupdate.config
USAGE="Usage: strainupdate.sh"

#
# Make sure the common configuration file exists and source it.
#
if [ -f ${COMMON_CONFIG} ]
then
    . ${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

#
# Initialize the log file.
#
LOG=${LOGDIR}/strainupdate.sh.log
rm -rf ${LOG}
touch ${LOG}

#
# Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#####################################
#
# Main
#
#####################################

#
# updateArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#
preload ${OUTPUTDIR}
cleanDir ${OUTPUTDIR}

# NOTE: keep this commented out until production release
#
# There should be a "lastrun" file in the input directory that was updated
# the last time the load was run for this input file. If this file exists
# and is more recent than the input file, the load does not need to be run.
#
LASTRUN_FILE=${INPUTDIR}/lastrunupdate
if [ -f ${LASTRUN_FILE} ]
then
    if test ${LASTRUN_FILE} -nt ${INPUT_FILE_DEFAULT}
    then
        echo "Input file has not been updated - skipping load" | tee -a ${LOG_PROC}
        # set STAT for shutdown
        STAT=0
        echo 'shutting down'
        shutDown
        exit 0
    fi
fi

echo "Running strain/curator/update load" | tee -a ${LOG_DIAG}
${PYTHON} ${CURATORSTRAINLOAD}/bin/strainupdate.py ${INPUT_FILE_DEFAULT} load | tee -a ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "curatorstrainload.py"

#
# Touch the "lastrun" file to note when the load was run.
#
touch ${LASTRUN_FILE}

#
# run postload cleanup and email logs
#
shutDown

