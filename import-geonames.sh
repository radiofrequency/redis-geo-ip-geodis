echo "downloading..."
#curl http://download.geonames.org/export/dump/allCountries.zip > data/allCountries.zip
#curl http://download.geonames.org/export/dump/admin1CodesASCII.txt > data/adminCodes.txt
#unzip -o data/AllCountries.zip
src/geodis.py -g -f data/allCountries.txt,data/adminCodes.txt