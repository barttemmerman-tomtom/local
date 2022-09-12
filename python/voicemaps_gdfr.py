import json
import urllib2
import time
from mock import Mock


class PhonemeAttributes:

    def getURLLib(self):
        return urllib2
    
    def getDataStream(self, requestURL):
        """
        make the call to the web service, including possible waits and retries
        :param requestURL: URL to consult for data retrieval
        """

        for i in range(10):
            print("%sRequest: %s" % ("" if i==0 else "retry %s: " % i, requestURL))

            result = self.getURLLib().urlopen(requestURL)
            if result and result.getcode() == 200:
                break
            
            seconds_wait = 10
            if result:
                print("request not successful: %s" % result.getcode())
            else:
                print("request not successful, <none> response")
            print("waiting %s seconds for retry.." % seconds_wait)
            
            time.sleep(seconds_wait)
            
        return result

    def get_data_from_service(self, hostname, service, requestType, country, dataset):
        """gets data from service"""
        request = "http://%s/%s/%s/%s/%s" % (hostname, service, requestType, country, dataset)

        #open the data stream
        result = self.getDataStream(request)
        if not result:
            raise IOError("couldn't get any phoneme data")
            
        jSONDecoder = json.JSONDecoder()
        print("Parse result")

        jsonData = ""
        streamProperlyEnded = 0
        streamProperlyStarted = 0
        for line in result.fp:
            #print "line found: %s" % line
            line = line.strip()
            
            #skip empty lines
            if not len(line):
                continue

            #when no json data is available and a '[' is encountered..
            #..then this is the stream start
            if not streamProperlyStarted:
                if not len(jsonData) and line.startswith("["):
                    streamProperlyStarted = 1
                    continue
                else:
                    jsonData = line
                    break

            #when no json data is available and a '[' is encountered..
            #..then this is the stream start
            if not len(jsonData) and line.endswith("]"):
                streamProperlyEnded = 1
                break

            jsonData += line
                
            if (jsonData.endswith("},") or \
                jsonData.endswith("}")):
                
                if jsonData.endswith(","):
                    jsonData = jsonData[:-1]
                
                data = jSONDecoder.decode(jsonData)
                if data:
                    yield data
                jsonData = ""
                
        if not streamProperlyStarted:
            raise IOError("stream start could not be found%s" % ("" if not(len(jsonData)) else ", begin data: \"%s\"" % jsonData,))
        elif not streamProperlyEnded:
            raise IOError("unexpected end of stream%s" % ("" if not(len(jsonData)) else ", last data: \"%s\"" % jsonData,))
        else:
            if len(jsonData):
                data = jSONDecoder.decode(jsonData)
                if data:
                    yield data


    def unitTest(self):
        #==============================================================================
        # TEST STUFF
        #==============================================================================
        hostname = 'besrvud-cio04:8080'
        service = 'phonemes-service-2.4'
        requestType = 'phonemes'
        country = 'BEL'
        dataset = 'BEL'

        mockResultFP = Mock()
        mockResultFP.getcode = Mock(return_value=200)
        mockResultFP.fp = [
        " {",
        "  \"originalText\": \"J Cockx\",",
        "  \"normalizedText\": \"J. Cockx\",",
        "  \"tifLanguage\": \"DUT\",",
        "  \"shortnames\": []",
        " },",
        " {",
        "  \"originalText\": \"J Cockx\",",
        "  \"normalizedText\": \"J. Cockx\",",
        "  \"tifLanguage\": \"FRE\",",
        "  \"shortnames\": []",
        " }",
        "]"
        ]

        mockURLLib = urllib2
        mockURLLib.urlopen = Mock()
        mockURLLib.urlopen.return_value = mockResultFP

        client = PhonemeAttributes()
        client.getURLLog = Mock(return_value=mockURLLib)

        requestResult = client.get_data_from_service(hostname, service, requestType, country, dataset)

        print "result: %s" % requestResult
        for dataObject in requestResult:
            print dataObject

hostname = 'besrvud-cio04:8080'
service = 'phonemes-service-2.4'
requestType = 'phonemes'
country = 'BEL'
dataset = 'BEL?text=Leopold'

client = PhonemeAttributes()
requestResult = client.get_data_from_service(hostname, service, requestType, country, dataset)

print "result: %s" % requestResult
objectCounter = 0
for dataObject in requestResult:
    objectCounter += 1
    print dataObject

print "count: %s" % objectCounter
