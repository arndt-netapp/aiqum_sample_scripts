#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull aggregate
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

# Query AIQUM for aggr data and print in CSV format.
def aiqum_aggregates(cnx):
    cursor = cnx.cursor()
    query = ("SELECT cluster.name,node.name,node.model,"
             "aggr.name,aggr.sizeUsedPercent,aggr.sizeTotal,aggr.sizeUsed,"
             "aggr_obj_cm.usedSpace,cluster.lastUpdateTime "
             "FROM aggregate AS aggr "
             "INNER JOIN node ON aggr.nodeId = node.objid "
             "INNER JOIN cluster ON aggr.clusterId = cluster.objid "
             "LEFT JOIN aggregate_objectstore_config_mapping AS aggr_obj_cm \
              ON aggr.objid = aggr_obj_cm.aggregateid "
            )
    cursor.execute(query)

    print("Cluster,Node,Model,"
          "Aggregate,PercentUsed,Size(GB),UsedSize(GB),"
          "CloudTierUsed(GB),LastUpdated")
    for row in cursor:
        SizeGB      = "%.1f" % (row[5]  / (1024*1024*1024))
        UsedGB      = "%.1f" % (row[6]  / (1024*1024*1024))
        if (row[7]):
            CloudUsedGB = "%.1f" % (row[7]  / (1024*1024*1024))
        else:
            CloudUsedGB = 0
        epochtime   = "%i" % (row[8] / 1000)
        lastupdated = datetime.datetime.fromtimestamp(int(epochtime))
        print("%s,%s,%s,"
              "%s,%s,%s,%s,"
              "%s,%s"
              %
              (row[0],row[1],row[2],
               row[3],row[4],SizeGB,UsedGB,
               CloudUsedGB,lastupdated
              )
             )

    return 1

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

# Parse the command line
parser = ArgumentParser(
    usage="%(prog)s [options]",
    description="Sample code to pull aggr details from the AIQUM Datbase."
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

# Connect to AIQUM and print the aggr details.
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password)
aiqum_aggregates(cnx)
