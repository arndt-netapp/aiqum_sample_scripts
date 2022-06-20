#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull volume
# related details from it.
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
def aiqum_volumes(cnx):
    cursor = cnx.cursor()
    query = ("SELECT cluster.name,vserver.name,vol.name,vol.junctionPath,"
             "export_policy.name,vol.size,vol.sizeUsed,"
             "vol.securityStyle,vol.volType,vol.styleExtended,"
             "vol.snapshotCount,vol.snapshotReserveSize,vol.sizeUsedBySnapshots,"
             "vol.securityUserID,vol.securityGroupID,vol.securityPermissions,"
             "vol.inodeFilesTotal,vol.inodeFilesUsed,"
             "aggregate.name,aggregate.aggregateType,"
             "cluster.lastUpdateTime "
             "FROM volume AS vol "
             "INNER JOIN cluster ON vol.clusterId = cluster.objid "
             "INNER JOIN vserver ON vol.vserverId = vserver.objid "
             "LEFT JOIN aggregate ON vol.aggregateId = aggregate.objid "
             "INNER JOIN export_policy ON vol.exportPolicyId = export_policy.objid "
             "WHERE (vol.volType='RW' or vol.volType='DP')"
            )
    cursor.execute(query)

    print("Cluster,Vserver,Volume,JunctionPath,"
          "ExportPolicyName,VolSize(GB),VolUsed(GB),"
          "SecurityStyle,VolType,VolStyle,"
          "SnapshotCount,SnapshotReserveSize(GB),SnapshotUsed(GB),"
          "UserID,GroupID,Permissions,"
          "InodesTotal,InodesUsed,"
          "Aggregqate,AggregateType,"
          "LastUpdated")
    for row in cursor:
        # Skip this row and log an error if we are missing values.
        if (row[6] is None):
            print("Missing value in row:", file=sys.stderr)
            print(row, file=sys.stderr)
            print("Continuing.", file=sys.stderr)
            continue
        VolSizeGB = "%.1f" % (row[5]  / (1024*1024*1024))
        VolUsedGB = "%.1f" % (row[6]  / (1024*1024*1024))
        SsResGB   = "%.1f" % (row[11] / (1024*1024*1024))
        SsUsedGB  = "%.1f" % (row[12] / (1024*1024*1024))
        epochtime = "%i" % (row[20] / 1000)
        lastupdated = datetime.datetime.fromtimestamp(int(epochtime))
        print("%s,%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,"
              "%s,%s,"
              "%s"
              %
              (row[0],row[1],row[2],row[3],
               row[4],VolSizeGB,VolUsedGB,
               row[7],row[8],row[9],
               row[10],SsResGB,SsUsedGB,
               row[13],row[14],row[15],
               row[16],row[17],
               row[18],row[19],
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
    description="Sample code to pull volume details from the AIQUM Datbase."
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

# Connect to AIQUM and print the volume details.
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password)
aiqum_volumes(cnx)
