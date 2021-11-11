#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull 
# snapmirror related details from it.
#
# AIQUM Requirements:
#   1. AIQUM 9.7 or higher.
#   2. An AIQUM "Database User" account with the "Report Schema" role.
#
# Python Requirements:
#   1. The mysql-connector-python module must be installed.
#
# AIQUM Database Schema documentation is on the NetApp Support Site:
# https://mysupport.netapp.com/documentation/docweb/index.html?productID=63834
#
################################################################################

import mysql.connector
import sys
import datetime
from argparse import ArgumentParser
from getpass import getpass

# Connection setup for AIQUM.
def aiqum_db_connect(aiq_host,aiq_user,aiq_password):
    try:
        cnx = mysql.connector.connect(host=aiq_host,
                                      user=aiq_user,
                                      password=aiq_password,
                                      database='netapp_model_view'
                                     )
    except:
        print()
        print("Error connecting to AIQUM Database. Exiting.")
        print()
        raise

    return cnx

# Query AIQUM for volume data and print in CSV format.
def aiqum_snapmirrors(cnx):
    cursor = cnx.cursor()
    query = ("SELECT sm.sourceVserver,sm.sourceVolume,"
             "vserver.name,volume.name,"
             "sm.mirrorState,sm.relationshipType,sm.lagTime,"
             "cluster.lastUpdateTime "
             "FROM snap_mirror AS sm "
             "INNER JOIN cluster ON sm.destinationClusterId = cluster.objid "
             "INNER JOIN vserver ON sm.destinationVserverId = vserver.objid "
             "INNER JOIN volume ON sm.destinationVolumeId = volume.objid "
             "WHERE sm.relationshipType='EXTENDED_DATA_PROTECTION'"
            )
    cursor.execute(query)

    print("SourceVserver,SourceVolume,"
          "DestinationVserver,DestinationVolume,"
          "MirrorState,MirrorType,LagTime,"
          "LastUpdated")
    for row in cursor:
        if row[6]:
            lagTime = str(datetime.timedelta(seconds=(row[6])))
            lagTime = lagTime.replace(",", "")
        else:
            lagTime = ""
        epochtime = "%i" % (row[7] / 1000)
        lastupdated = datetime.datetime.fromtimestamp(int(epochtime))
        print("%s,%s,"
              "%s,%s,"
              "%s,%s,%s,"
              "%s"
              %
              (row[0],row[1],
               row[2],row[3],
               row[4],row[5],lagTime,
               lastupdated
              )
             )

    return 1

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

# Parse the command line
parser = ArgumentParser(
    usage="%(prog)s [options]",
    description="Sample code to pull snapmirror details from the AIQUM Datbase."
)
parser.add_argument(
    '-a', '--aiqumhost', nargs='?', required=True, help='AIQUM Host'
)
parser.add_argument(
    '-u', '--username', nargs='?', required=True, help='AIQUM Username'
)
parser.add_argument(
    '-p', '--password', nargs='?', help='Password for AIQUM username'
)
args = parser.parse_args()
if not args.password: args.password = getpass()

# Connect to AIQUM and print the snapmirror details.
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password)
aiqum_snapmirrors(cnx)
