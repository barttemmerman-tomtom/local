
import urllib2

#calling rest web services from python


#Simple GET requests

url = 'http://developer.yahoo.com/'

try:
    data = urllib2.urlopen(url).read()
except urllib2.HTTPError, e:
    print "HTTP error: %d" % e.code
except urllib2.URLError, e:
    print "Network error: %s" % e.reason.args[1]
    
    
    
#Simple POST requests
    
url = 'http://search.yahooapis.com/ContentAnalysisService/V1/termExtraction'
appid = 'YahooDemo'

context = '''
Italian sculptors and painters of the renaissance favored
the Virgin Mary for inspiration
'''
query = 'madonna'

params = urllib.urlencode({
    'appid': appid,
    'context': context,
    'query': query
})

data = urllib.urlopen(url, params).read()