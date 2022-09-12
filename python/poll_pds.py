#!/usr/bin/env python

import os
import sys
import commands
import json
import time
import datetime

default_endpoint = "https://pds-exports.maps-td.tomtom.com/pds-snapshots"

endpoint = {
    "default": default_endpoint,
    "kor": "https://pds-exports-skor.maps-td.tomtom.com/pds-snapshots",
    "yhm": "https://pds-exports.maps-td-dev.tomtom.com/pds-snapshots"
}

number_of_tries = 3

def getProductInfo(coverage):
    print "\n\nGet product info for coverage '%s'" % coverage
    mnrProductsJsonObject = json.loads(open('mnr_products.json').read())
    coverage_split = coverage.split('_')[0]
    for product in mnrProductsJsonObject.values():
        if product.has_key(coverage_split):
           return product[coverage_split]
        else:
           msg = "FAILURE: Coverage '%s' not found" % coverage_split
           print msg
           os.system("echo %s > params.env" % msg)
           sys.exit(0)

def getLatestSnapshot(url):
    tries = 0
    while tries < number_of_tries:
        try: 
           pdsJsonObject = json.loads(commands.getoutput("curl -s %s" % url))
           snapshot = pdsJsonObject['snapshots'][0]
           return snapshot
        except:
           print "Oops! Unable to connect to pds-snapshot-service. Try again in 10sec..."
           time.sleep(10)
           tries += 1
    printLine()
    msg = "FAILURE: Unable to connect to pds-snapshot-service"
    print msg
    os.system("echo %s > params.env" % msg)
    sys.exit(0)

def getCurrentPdsJournalVersion(branchId, url=endpoint["default"]):
    latestSnapshot = getLatestSnapshot("%s?branch_id=%s" % (url, branchId))
    if latestSnapshot.has_key("journal_version".encode('utf-8')):
       return (latestSnapshot["journal_version"], latestSnapshot["last_modified"][:-1])
    else:
       msg= "FAILURE: PDS info not found for branchId %s" % branchId
       print msg
       os.system("echo %s > params.env" % msg)
       sys.exit(0)

def freshEnough(modified, maxDaysOld):
    now = time.mktime(time.strptime('%s' %  datetime.datetime.now().isoformat()[:-7], '%Y-%m-%dT%H:%M:%S'))
    modified = time.mktime(time.strptime('%s' %  modified, '%Y-%m-%dT%H:%M:%S'))
    daysOld = (now - modified) / 86400.0
    if (daysOld <= maxDaysOld):
       return True
    else:
       return False

def printLine():
    print "\n---------------------------------\n"

def main(coverage, seconds, sleepSeconds, maxDaysOld):
    product = getProductInfo(coverage)
    branchId = product["branchId".encode('utf-8')]
    journalVersionUsedForProduct = product["baseJournalVersion".encode('utf-8')]
    softwareVersion = product["softwareVersion".encode('utf-8')]
    print "Previous MNR-product created from journalVersion %s" % journalVersionUsedForProduct
    printLine()

    endTime = time.time() + seconds    
    print "Loop created: %s seconds" % seconds

    while time.time() < endTime:
          (currentPdsJournalVersion, modified) = getCurrentPdsJournalVersion(branchId, endpoint.get(coverage, default_endpoint))
          if journalVersionUsedForProduct < currentPdsJournalVersion:
             if freshEnough(modified, maxDaysOld):
                 printLine()
                 msg = "SUCCESS: New PDS found for %s journal version %s - created on %s" % (coverage, currentPdsJournalVersion, modified)
                 print msg
                 printLine()
                 os.system("echo %s,%s,%s,%s,%s > params.env" % (msg, coverage, branchId, currentPdsJournalVersion, softwareVersion))
                 sys.stdout.flush()
                 sys.exit(0)
             else:
                print "Current pds version of branch %s, journal version %s is not fresh enough: last modified %s" % (branchId, currentPdsJournalVersion, modified)
                print "... retry in %s seconds" % sleepSeconds
                sys.stdout.flush()
                time.sleep(sleepSeconds)
          else:
             print "Current pds version of branch %s is still the same (%s)" % (branchId, currentPdsJournalVersion)
             print "... retry in %s seconds" % sleepSeconds
             sys.stdout.flush()
             time.sleep(sleepSeconds)
    printLine()
    msg = "UNSTABLE: No new PDS version found after %s seconds" % seconds
    print msg
    os.system("echo %s > params.env" % msg)
    printLine()
    sys.exit(0)

if __name__ == "__main__":
   if (len(sys.argv)!=5):
      print "usage: python poll_pds.py <Coverage> <SecondsToLoop> <SleepTime> <MaxDaysOld>"
      sys.exit(-1)
   main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]) , float(sys.argv[4]))




