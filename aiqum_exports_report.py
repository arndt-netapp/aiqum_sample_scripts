#!/usr/bin/env python3

################################################################################
#
# This sample code shows how to connect to the AIQUM Database and pull export
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

# Query AIQUM for exports data and print in CSV format.
def aiqum_exports(cnx):
    # Query for all export rules.
    cursor = cnx.cursor()
    query = ("SELECT cluster.name,vserver.name,export_policy.name,"
             "rule.clientMatch,rule.roRule,rule.rwRule,rule.superUserSecurity "
             "FROM export_rule AS rule "
             "INNER JOIN cluster ON rule.clusterId = cluster.objid "
             "INNER JOIN vserver ON rule.vserverId = vserver.objid "
             "INNER JOIN export_policy ON "
             "rule.exportPolicyId = export_policy.objid "
            )
    cursor.execute(query)

    # Save the rules for each policyName.
    rules = {}
    for row in cursor:
        cluster    = row[0]
        vserver    = row[1]
        policyName = row[2]
        rule = ("client:" + row[3] + 
                " read-only:" + row[4] + 
                " read-write:" + row[5] + 
                " superuser:" + row[6]
               )
        if cluster not in rules: rules[cluster] = {}
        if vserver not in rules[cluster]: rules[cluster][vserver] = {}
        if policyName not in rules[cluster][vserver]:
            rules[cluster][vserver][policyName] = rule
        else:
            rules[cluster][vserver][policyName] = (
                rules[cluster][vserver][policyName] + "; " + rule)

    # Loop through the policies and print rules for each as we go.
    print("Cluster,Vserver,ExportPolicy,ExportRules")
    for cluster in rules:
        for vserver in rules[cluster]:
            for policyName in rules[cluster][vserver]:
                print("%s,%s,%s,%s" %
                      (cluster,vserver,policyName,
                       rules[cluster][vserver][policyName]
                      )
                     )

    return 1

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

# Parse the command line
parser = ArgumentParser(
    usage="%(prog)s [options]",
    description="Sample code to pull export policy details from the AIQUM Datbase."
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

# Connect to AIQUM and print the exports details.
cnx = aiqum_db_connect(args.aiqumhost,args.username,args.password)
aiqum_exports(cnx)
