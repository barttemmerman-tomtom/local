python es-dump-documents.py --from-timestamp "2020-09-10T10:11:29.265Z" --to-timestamp "2020-10-01T00:00:00.000Z" --outputfile outputfile-201009.json >201009.log 2>&1
python es-dump-documents.py --from-timestamp "2020-10-01T00:00:00.000Z" --to-timestamp "2020-11-01T00:00:00.000Z" --outputfile outputfile-201010.json >201010.log 2>&1
python es-dump-documents.py --from-timestamp "2020-11-01T00:00:00.000Z" --to-timestamp "2020-12-01T00:00:00.000Z" --outputfile outputfile-201011.json >201011.log 2>&1
python es-dump-documents.py --from-timestamp "2020-12-01T00:00:00.000Z" --to-timestamp "2021-01-01T00:00:00.000Z" --outputfile outputfile-201012.json >201012.log 2>&1
