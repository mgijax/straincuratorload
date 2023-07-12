#!/bin/sh
#
# Purpose:
#	wrapper for straincreate.py and strainupdate.py
#
# History
#
# lec   05/12/2023
#	- wts2-902/flr-344/Strain Curator easy update load (part 1)
#

BINDIR=`dirname $0`
COMMON_CONFIG=`cd ${BINDIR}/..; pwd`/curatorstrainload.config
USAGE="Usage: curatorstrainload.sh"

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
LOG=${LOGDIR}/curatorstrainload.sh.log
rm -rf ${LOG}
touch ${LOG}

${CURATORSTRAINLOAD}/bin/straincreate.sh | tee -a $LOG
${CURATORSTRAINLOAD}/bin/strainupdate.sh | tee -a $LOG

