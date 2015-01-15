#Copyright 2011 Do@. All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are
#permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice, this list of
#      conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright notice, this list
#      of conditions and the following disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY Do@ ``AS IS'' AND ANY EXPRESS OR IMPLIED
#WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#The views and conclusions contained in the software and documentation are those of the
#authors and should not be interpreted as representing official policies, either expressed
#or implied, of Do@.

#Importer for locations from geonames

from city import City
import csv
import logging
import redis
import countries
import re
from importer import Importer

import sys

csv.field_size_limit(sys.maxsize)

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialetc, **kwargs)
    for row in csv_reader:
        yield[unicode(cell, 'utf-8') for cell in row]


class GeonamesImporter(Importer):

    def __init__(self, fileName, redisHost, redisPort, redisDB = 0):
        """
        Init a geonames cities importer
        @param fileName path to the geonames datafile
        @param redisConn redis connection
        """

        Importer.__init__(self, fileName ,redisHost, redisPort, redisDB)

        fileNames = fileName.split(',')
        self.fileName = fileNames[0]
        self.adminCodesFileName = fileNames[1] if len(fileNames) > 1 else None
        self._adminCodes = {}

    def _readAdminCodes(self):
        """
        Read administrative codes for states and regions
        """

        if not self.adminCodesFileName:
            logging.warn("No admin codes file name. You won't have state names etc")
            return

        try:
            fp = open(self.adminCodesFileName)
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.adminCodesFileName, e))
            return

        reader = csv.reader(fp, delimiter='\t')
        for row in reader:
            val1 = unicode(row[0], 'utf-8')
            val2 = unicode(row[1], 'utf-8')

            #print val1
            #print val2
            self._adminCodes[val1.strip()] = val2.strip()


    def runImport(self):
        """
        File Format:
        5368361 Los Angeles     Los Angeles     Angelopolis,El Pueblo de Nu....     34.05223        -118.24368      P       PPL
        US              CA      037                     3694820 89      115     America/Los_Angeles     2009-11-02

        """

        self._readAdminCodes()
        
        try:
            fp = open(self.fileName)
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.fileName, e))
            return False

        reader = csv.reader(fp, delimiter='\t')
        pipe = self.redis.pipeline()

        i = 0
        for row in reader:

            try:
                geoid = row[1]
                name = unicode(row[2], 'utf-8')
                #asciiname = row[3]
                altname = unicode(row[3], 'utf-8')
                lat = float(row[4])
                lon = float(row[5])
                featureclass = row[6]
                featurecode = row[7]
                countrycode = row[8]
                cc2 = row[9]
                population = row[15]
                timezone = row[18]
                country = unicode(row[8], 'utf-8')
                code = unicode(row[10], 'utf-8')

                #print countries["CA"]

                #print featureclass

                if "P" not in featureclass: continue
                #print "not skipping"
                #print featureclass

                adminCode = '.'.join((country, code))
                region = re.sub('\\(.+\\)', '', self._adminCodes.get(adminCode, '')).strip()
                #print "altname"
                #print altname

                if (len(country) == 0): continue


                #if (country == "CA"):
                #    if (region == "British Columbia"):
                #        print featureclass
                #        print name
                #        print country
                #        print region

 #                       print code
 #                       print featurecode
  #                  print region
                  #      print country
                  #      print countrycode
                  #      print name
   #                 print featureclass
    #                print featurecode
     #               print population

                    
#                #for us states - take only state code not full name
#                if country == 'US':
#                    region = row[10]#

#                lat = float(row[4])
#                lon = float(row[5])#

                loc = City(name = name,
                                country = country,
                                state = region,
                                altname = altname,
                                lat = lat,
                                lon = lon)

                loc.save(pipe)



            except Exception, e:
                logging.error("Could not import line %s: %s" % (row, e))
            i += 1
            if i % 1000 == 0:
                pipe.execute()
        pipe.execute()

        logging.info("Imported %d locations" % i)

        return True