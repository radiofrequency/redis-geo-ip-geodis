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

#Importer for locations from ip2location.com databases


import csv
import logging
import redis

from importer import Importer
from iprange import IPRange
from iplocation import IPLocation

class IP2LocationImporter(Importer):

    def runImport(self, reset = False):
        """
        My File Format
        "16843008","16843263","AU","AUSTRALIA","QUEENSLAND","SOUTH BRISBANE","-27.483330","153.016670","-","+10:00"

        File Format:
        "67134976","67135231","US","UNITED STATES","CALIFORNIA","LOS ANGELES","34.045200","-118.284000","90001"

        """
        if reset:
            print "Deleting old ip data..."
            self.redis.delete(IPRange._indexKey)
            self.redis.delete(IPLocation._indexKey)

        print "Starting import..."
            
        try:
            fp = open(self.fileName)
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.fileName, e))
            return False

        reader = csv.reader(fp, delimiter=',', quotechar='"')
        pipe = self.redis.pipeline()

        i = 0
        for row in reader:
            
            try:
                #parse the row
                countryCode = row[2]
                rangeMin = int(row[0])
                rangeMax = int(row[1])
                lat = float(row[6])
                lon = float(row[7])


                try:
                    state = row[4]
                    city = row[5]
                except:
                    state = ''
                    city = ''

                try:
                    timezone = row[9]
                except:
                    timezone = ''

                #take the zipcode if possible
                try:
                    zipcode = row[8]
                except:
                    zipcode = ''


                #junk record
                if countryCode == '-' and (not lat and not lon):
                    continue
             

                iplocation = IPLocation( rangeMin, rangeMax, lat, lon, zipcode, timezone, countryCode, state, city)

                iplocation.save(pipe);
                       
                #range = IPRange(rangeMin, rangeMax, lat, lon, zipcode)
                #range.save(pipe)

            except Exception, e:
                logging.error("Could not save record: % %s %s %s " % (e, countryCode, lat, log))

            i += 1
            if i % 10000 == 0:
                logging.info("Dumping pipe. did %d ranges" % i)
                pipe.execute()

        pipe.execute()
        logging.info("Imported %d locations" % i)

        return i

            