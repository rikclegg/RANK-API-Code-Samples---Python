# rankDataGroupRequestSync.py

import blpapi
import sys
import datetime
import os


SESSION_STARTED                 = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE         = blpapi.Name("SessionStartupFailure")
SESSION_CONNECTION_UP           = blpapi.Name("SessionnConnectionUp")
SESSION_CONNECTION_DOWN         = blpapi.Name("SessionConnectionDown")
SESSION_TERMINATED              = blpapi.Name("SessionTerminated")

SERVICE_OPENED                  = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE            = blpapi.Name("ServiceOpenFailure")
SERVICE_DOWN                    = blpapi.Name("ServiceDown")

SLOW_CONSUMER_WARNING           = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED   = blpapi.Name("SlowConsumerWarningCleared")

EXCEPTION                       = blpapi.Name("Exception")
GROUPREPORT                     = blpapi.Name("GroupReport")
  
# production service
d_service="//blp/rankapi"

d_host="localhost"
d_port=8194

bEnd=False


print ("Bloomberg - RANK API Example - rankDataGroupRequestSync")

# Session options are used to create the connectivity through to Bloomberg.
sessionOptions = blpapi.SessionOptions()
sessionOptions.setServerHost(d_host) # This represents the chose connectivity method. In this case, we are using Desktop API, so the host is 'localhost'
sessionOptions.setServerPort(d_port) # The default port is 8194.
    
print ("Connecting to %s:%d" % (d_host, d_port))

# Create a Session
session = blpapi.Session(sessionOptions)

# Start a Session
if not session.start():
    print("Failed to start session.")
    exit(-1)

# Open service 
if not session.openService("//blp/rankapi"):
    print("Failed to open RANK API service")
    exit(-1)

print ("Service opened...")
service = session.getService(d_service)

# build request
request = service.createRequest("GroupQuery")

### set date/time range
request.set("date", datetime.datetime(2021, 9, 29, 0, 0, 0, 0)) 
            
### units enum 0=Shares, 1=Local, 2=USD, 3=EUR, 4=GBP
request.set("units", "Shares")             

### source enum 0=Broker Contributed
request.set("source", "Broker Contributed")


### exchanges or securities 
# exchanges can be set using Bloomberg exchange code 
#exchanges = request.getElement("securityCriteria").setChoice("exchanges")
#exchange = exchanges.appendElement()
#exchange.setElement("code", "US");

### securities can be set to either Bloomberg ticker or figi
securities = request.getElement("securityCriteria").setChoice("securities")
securities.appendElement().setElement("ticker", "IBM US Equity")
securities.appendElement().setElement("ticker", "MSFT US Equity")
securities.appendElement().setElement("ticker", "VOD LN Equity")

# figi can be used instead of ticker
#security.setElement("figi", "BBG000B9XRY4"); # figi for AAPL US Equity

print ("Sending Request: %s" % request.toString())

requestID = session.sendRequest(request)

print ("RANK group request sent.")

done = False
while not done:
    
    event = session.nextEvent(1000)

    if event.eventType() == blpapi.Event.RESPONSE:

        for msg in event:
            # for printing raw message
            #print(msg)

            if msg.correlationIds()[0].value() == requestID.value():
                print ("MESSAGE TYPE: %s" % msg.messageType())

                if msg.messageType() == EXCEPTION:
                    print (msg)
                    print ("Exception occured")    
                    done = True
                
                elif msg.messageType() == GROUPREPORT:
                    ts = msg.getElementAsDatetime("timestampUtc")
                    print ("Timestamp: ", ts)
                    #print ("Message: \n", msg)

                    securities = msg.getElement("securities")

                    for security in securities.values():
                        
                        ticker = security.getElement("security").getElement("ticker").getValueAsString()
                        print("Ticker: ", ticker)

                        records = security.getElement("records")

                        for record in records.values():

                            brokerAcronym = record.getElement("broker").getElement("acronym").getValueAsString()
                            brokerName = record.getElement("broker").getElement("name").getValueAsString()
                            brokerRank = record.getElement("broker").getElement("rank").getValueAsInteger()

                            bought = record.getElementAsFloat("bought")
                            sold = record.getElementAsFloat("sold")
                            traded = record.getElementAsFloat("traded")
                            crossed = record.getElementAsFloat("crossed")
                            total = record.getElementAsFloat("total")
                            highTouch = record.getElementAsFloat("highTouch")
                            lowTouch = record.getElementAsFloat("lowTouch")
                            numReports = record.getElementAsFloat("numReports")

                            print (f"\tBroker: [{brokerAcronym}] {brokerName} Rank: {brokerRank} Bought: {bought} Sold: {sold} Traded: {traded} Crossed: {crossed} Total: {total} High Touch: {highTouch} Low Touch: {lowTouch} Count: {numReports}")
                            done = True

session.stop()
exit()


__copyright__ = """
Copyright 2022. Bloomberg Finance L.P.
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""





