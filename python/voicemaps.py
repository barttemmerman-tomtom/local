import json
import urllib2

#calling rest web services from python
#Simple GET requests

url = 'http://besrvud-cio04:8080/phonemes-service-2.4/phonemes/BEL/BEL'
requestType = 'phonemes'

url = 'http://besrvud-cio04:8080/phonemes-service-2.4/phonemes/BEL/BEL?text=Leop%Tunn%'
requestType = 'phonemes'

url = 'http://besrvud-cio04:8080/phonemes-service-2.4/normalizedtext/BEL/BEL?text=Leop%'

url = 'http://besrvud-cio04:8080/phonemes-service-2.4/normalizedtext/BEL/BEL'
requestType = 'normalizedtext'

url = 'http://besrvud-cio04:8080/phonemes-service-2.4/phonemes/BEL/BEL'
url = 'http://localhost:8080/phonemes-service/phonemes/BEL/BEL'
requestType = 'phonemes'


print "url: %s" % url
print "request type: %s" % requestType

try:
    print "accessing web service.."
    result = urllib2.urlopen(url)
except urllib2.HTTPError, e:
    print "HTTP error: %d" % e.code
except urllib2.URLError, e:
    print "Network error: %s" % e.reason.args[1]
    
if not (result and result.getcode() == 200):
    raise IOError("Houston we got a problem!")

if not result:
    raise IOError("couldn't connect to phoneme data")
jSONDecoder = json.JSONDecoder()
print "parsing result.."

def decode(data):
    start = 0
    if data[0] == '[':
        start = 1
    if data[-1] != '}':
        data = data[start:-1]
    if data:
        return jSONDecoder.decode(data)

jsonData = ""
phonemeCounter = 0
for line in result.fp:
    line = line.strip()
    jsonData += line
    if (line.endswith("},") and (requestType != 'normalizedtext' or ']' in jsonData)) or \
       (line.endswith("} ") and (requestType != 'normalizedtext' or ']' in jsonData)):
        data = decode(jsonData)
        if data:
            #print "data: %s" % data
            phonemeCounter+=1
        jsonData = ""
data = decode(jsonData)
if data:
    phonemeCounter+=1
    print "final data: %s" % data

print "count: %s" % phonemeCounter
print "parsing done!"

    
    
    