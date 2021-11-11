#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull user
# quota details from it.
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

# Query AIQUM for user quota data and print in CSV format.
def aiqum_user_quotas(cnx):
    cursor = cnx.cursor()
    query = ("SELECT cluster.name,vserver.name,volume.name,volume.junctionPath,"
             "qtree.name,quota_user.quotaUserID,quota_user.quotaUserName,"
             "uq.quotaTarget,uq.diskLimit,uq.diskUsed,"
             "cluster.lastUpdateTime "
             "FROM user_quota AS uq "
             "INNER JOIN cluster ON uq.clusterId = cluster.objid "
             "INNER JOIN vserver ON uq.vserverId = vserver.objid "
             "INNER JOIN volume ON uq.volumeId = volume.objid "
             "INNER JOIN qtree ON uq.qtreeId = qtree.objid "
             "INNER JOIN quota_user ON uq.objid = quota_user.userQuotaID "
            )
    cursor.execute(query)

    print("Cluster,Vserver,Volume,JunctionPath,"
          "Qtree,UserID,UserName,"
          "QuotaTarget,DiskLimit(GB),DiskUsed(GB),"
          "LastUpdated"
         )
    for row in cursor:
        DiskLimitGB = "%.1f" % (row[8] / (1024*1024))
        DiskUsedGB  = "%.1f" % (row[9] / (1024*1024))
        epochtime   = "%i" % (row[10] / 1000)
        lastupdated = datetime.datetime.fromtimestamp(int(epochtime))
        print("%s,%s,%s,%s,"
              "%s,%s,%s,"
              "%s,%s,%s,"
              "%s"
              %
              (row[0],row[1],row[2],row[3],
               row[4],row[5],row[6],
               row[7],DiskLimitGB,DiskUsedGB,
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
    description="Sample code to pull user quota details from the AIQUM Datbase."
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

# Connect to AIQUM and print the user quota details.
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password)
aiqum_user_quotas(cnx)
