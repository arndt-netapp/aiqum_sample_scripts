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
             "export_policy.name,vol.size,vol.sizeTotal,vol.sizeUsed,vol.cloudTierFootprintBytes,"
             "vol.securityStyle,vol.volType,vol.styleExtended,"
             "snapshot_policy.name,vol.snapshotCount,vol.snapshotReserveSize,vol.sizeUsedBySnapshots,"
             "vol.securityUserID,vol.securityGroupID,vol.securityPermissions,"
             "vol.inodeFilesTotal,vol.inodeFilesUsed,vol.quotaStatus,"
             "aggregate.name,aggregate.aggregateType,"
             "vol.tieringPolicy,vol.tieringMinimumCoolingDays,"
             "vol.compressionSpaceSaved,vol.deduplicationSpaceSaved,"
             "cluster.lastUpdateTime "
             "FROM volume AS vol "
             "INNER JOIN cluster ON vol.clusterId = cluster.objid "
             "INNER JOIN vserver ON vol.vserverId = vserver.objid "
             "LEFT JOIN aggregate ON vol.aggregateId = aggregate.objid "
             "INNER JOIN export_policy ON vol.exportPolicyId = export_policy.objid "
             "INNER JOIN snapshot_policy ON vol.snapshotPolicyId = snapshot_policy.objid "
             "WHERE (vol.volType='RW' or vol.volType='DP')"
            )
    cursor.execute(query)

    print("Cluster,Vserver,Volume,JunctionPath,"
          "ExportPolicyName,VolSize(GB),VolDataSize(GB),VolUsed(GB),CloudTierUsed(GB),"
          "SecurityStyle,VolType,VolStyle,"
          "SnapshotPolicy,SnapshotCount,SnapshotReserveSize(GB),SnapshotUsed(GB),"
          "UserID,GroupID,Permissions,"
          "InodesTotal,InodesUsed,QuotaStatus,"
          "Aggregqate,AggregateType,"
          "TieringPolicy,TieringMinCoolingDays,"
          "CompressionSaved(GB),DeduplicationSaved(GB),"
          "LastUpdated")
    for row in cursor:
        # Skip this row and log an error if we are missing values.
        if (row[6] is None):
            print("Missing value in row:", file=sys.stderr)
            print(row, file=sys.stderr)
            print("Continuing.", file=sys.stderr)
            continue
        VolSizeGB     = "%.1f" % (row[5]  / (1024*1024*1024))
        VolDataSizeGB = "%.1f" % (row[6]  / (1024*1024*1024))
        VolUsedGB     = "%.1f" % (row[7]  / (1024*1024*1024))
        if row[8]:
            CloudUsedGB = "%.1f" % (row[8]  / (1024*1024*1024))
        else:
            CloudUsedGB = 0
        SsResGB     = "%.1f" % (row[14] / (1024*1024*1024))
        SsUsedGB    = "%.1f" % (row[15] / (1024*1024*1024))
        if row[26]:
            CompGB = "%.1f" % (row[26] / (1024*1024*1024))
        else:
            CompGB = 0
        if row[27]:
            DedupGB = "%.1f" % (row[27] / (1024*1024*1024))
        else:
            DedupGB = 0
        epochtime   = "%i" % (row[28] / 1000)
        lastupdated = datetime.datetime.fromtimestamp(int(epochtime))
        print("%s,%s,%s,%s,%s,"
              "%s,%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,"
              "%s,%s,"
              "%s,%s,"
              "%s"
              %
              (row[0],row[1],row[2],row[3],
               row[4],VolSizeGB,VolDataSizeGB,VolUsedGB,CloudUsedGB,
               row[9],row[10],row[11],
               row[12],row[13],SsResGB,SsUsedGB,
               row[16],row[17],row[18],
               row[19],row[20],row[21],
               row[22],row[23],
               row[24],row[25],
	       CompGB,DedupGB,
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
