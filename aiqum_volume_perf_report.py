#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull volume
# performance related details from it.
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
# Reference objid mappings for the netapp_performance database in:
# https://www.netapp.com/media/17053-tr4709.pdf
#
################################################################################

import mysql.connector
import sys
import datetime
import time
from argparse import ArgumentParser
from getpass import getpass

# Connection setup for AIQUM.
def aiqum_db_connect(aiq_host,aiq_user,aiq_password,aiq_db):
    try:
        cnx = mysql.connector.connect(host=aiq_host,
                                      user=aiq_user,
                                      password=aiq_password,
                                      database=aiq_db
                                      )
    except:
        print()
        print("Error connecting to AIQUM Database. Exiting.")
        print()
        raise

    return cnx

# Query AIQUM for object mappings.
def aiqum_object_mappings(cnx,clustercsv):
    cursor = cnx.cursor()

    # Get cluster objid dict.
    querytxt = "SELECT objid,name FROM cluster "
    x = 0
    for clustername in clustercsv.split(","):
        if x == 0:
            querytxt = querytxt + "WHERE name='" + clustername + "' "
        else:
            querytxt = querytxt + "OR name='" + clustername + "' "
        x = x + 1
    query = (querytxt)
    cursor.execute(query)
    for row in cursor:
        clustermap[row[0]] = row[1]

    # Create where clause used for all the following queries.
    x = 0
    whereclause = ""
    for clusterid in clustermap:
        if x == 0:
            whereclause = whereclause + "WHERE clusterid='" + str(clusterid) + "' "
        else:
            whereclause = whereclause + "OR clusterid='" + str(clusterid) + "' "
        x = x + 1

    # Get vserver objid dict.
    query = ("SELECT objid,name FROM vserver " + whereclause)
    cursor.execute(query)
    for row in cursor:
        vservermap[row[0]] = row[1]

    # Get volume objid dict and populate mappings.
    query = ("SELECT objid,name,clusterid,vserverid FROM volume " + whereclause)
    cursor.execute(query)
    for row in cursor:
        volmap[row[0]] = {}
        volmap[row[0]]['name'] = row[1]
        volmap[row[0]]['cluster'] = clustermap[row[2]]
        volmap[row[0]]['vserver'] = vservermap[row[3]]

    # Get map of QoS objid to volume objid (QoS holderid).
    query = ("SELECT objid,holderid FROM qos_workload " + whereclause)
    cursor.execute(query)
    for row in cursor:
        qosmap[row[0]] = row[1]

    return 1

# Query AIQUM for volume data and print in CSV format.
def aiqum_volumes_perf(cnx,days):
    cursor = cnx.cursor()

    # Convert days requested to epoch time in history.
    seconds = int(days) * 86400
    starttime = int(time.time()) - seconds
    starttime = starttime * 1000

    print("Cluster,Vserver,Volume,Timestamp,IOPs,Throughput(bytes/sec)")
    for clusterid in clustermap:
        query = ("SELECT objid,fromtime,ops,totalData "
                 "FROM summary_qos_volume_workload_" + str(clusterid) + " "
                 "WHERE fromtime > " + str(starttime)
                )
        cursor.execute(query)
        for row in cursor:
            qosid = row[0]
            if qosid in qosmap:
                volid = qosmap[qosid]
                if volid in volmap:
                    epochtime = "%i" % (row[1] / 1000)
                    timestamp = datetime.datetime.fromtimestamp(int(epochtime))
                    print("%s,%s,%s,%s,%s,%s"
                          %
                          (volmap[volid]['cluster'],
                          volmap[volid]['vserver'],
                          volmap[volid]['name'],
                          timestamp,row[2],row[3])
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
parser.add_argument(
    '-c', '--clusters', nargs='?', required=True, help='CSV list of clusters'
)
parser.add_argument(
    '-d', '--days', nargs='?', required=True, help='Days of history to pull'
)
args = parser.parse_args()
if not args.password: args.password = getpass()

# Connect to AIQUM - first to the netapp_model_view db.
db = "netapp_model_view"
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password,db)

# Gather a mapping of objid to names for clusters, vservers, and volumes.
# netapp_model qos_workload.holderid == volume.objid.
# netapp_performance summary_qos_volume_workload_<clusterid>.objid == holderid.
clustermap = {}
vservermap = {}
volmap = {}
qosmap = {}
aiqum_object_mappings(cnx,args.clusters)

# Connect to AIQUM - now to the netapp_performance db.
db = "netapp_performance"
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password,db)

# Gather and print the volume performance details for the target clusters.
aiqum_volumes_perf(cnx,args.days)
