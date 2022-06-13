# rankDataRequest.py

import blpapi
import sys
import datetime
import os

# for additional DEBUG logging
#os.environ['BLPAPI_LOGLEVEL'] = 'DEBUG'

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
REPORT                          = blpapi.Name("Report")
  
# production service
d_service="//blp/rankapi"

d_host="localhost"
d_port=8194

bEnd=False

class SessionEventHandler():

    def processEvent(self, event, session):
        try:
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event,session)
            
            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                self.processResponseEvent(event)
            
            else:
                self.processMiscEvents(event)
                
        except:
            print ("Exception:  %s" % sys.exc_info()[0])
            
        return False

    # only relevant for subscription service
    def processAdminEvents(self, event):
            print ("Processing ADMIN event")

            for msg in event:
                if msg.messageType() == SLOW_CONSUMER_WARNING:
                    print ("Warning: Entered Slow Consumer status")

                elif msg.messageType() == SLOW_CONSUMER_WARNING_CLEARED:
                    sys.stderr.write("Slow Consumer status cleared")
                
                else:
                    print(msg)                 
    

    def processSessionStatusEvent(self, event, session):
        print("Processing SESSION_STATUS event")

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print ("Session started...")
                session.openServiceAsync(d_service)
            
            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print >> sys.stderr, ("Error: Session startup failed")
            
            elif msg.messageType() == SESSION_TERMINATED:
                print ("Session has been terminated")
            
            elif msg.messageType() == SESSION_CONNECTION_UP:
                print ("Session connection is up")
            
            elif msg.messageType() == SESSION_CONNECTION_DOWN:
                print ("Session connection is down")
            
            else:
                print (msg)
   

    def processServiceStatusEvent(self, event, session):
        print ("Processing SERVICE_STATUS event")

        for msg in event:
            print (msg)

            if msg.messageType() == SERVICE_OPENED:
                print ("Service opened...")
                service = session.getService(d_service)
                
                # build request
                request = service.createRequest("Query")
                
                ### set date/time range
                request.set("start", datetime.datetime(2021, 9, 29, 0, 0, 0, 0)) 
                request.set("end", datetime.datetime(2021, 9, 30, 0, 0, 0, 0)) 
                            
                ### group by 0=Broker , 1=Security
                request.set("groupBy", "Security")
                #request.set("groupBy", "Broker")

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

                #security.setElement("figi", "BBG000B9XRY4"); # figi for AAPL US Equity
              
                ### specify broker by acronym or rank
                #broker = request.getElement("brokers").appendElement()
                #broker.setElement("acronym", "MLCO"); # acronym

                ### source enum 0=Broker Contributed
                request.set("source", "Broker Contributed")

                ### units enum 0=Shares, 1=Local, 2=USD, 3=EUR, 4=GBP
                request.set("units", "Shares")             
                
                print ("Sending Request: %s" % request.toString())

                #self.requestID = session.sendRequest(request)
                
                self.requestID = blpapi.CorrelationId()
                session.sendRequest(request, correlationId=self.requestID)
                
                print ("RANK data request sent.")

            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                print >> sys.stderr, ("Error: Service failed to open")
                bEnd=True

            elif msg.messageType() == SERVICE_DOWN:
                print ("Service down")
                bEnd=True
                


    def processResponseEvent(self, event):
        print ("Processing RESPONSE event")

        for msg in event:
            # for printing raw message
            #print(msg)

            if msg.correlationIds()[0].value() == self.requestID.value():
                print ("MESSAGE TYPE: %s" % msg.messageType())

                if msg.messageType() == EXCEPTION:
                    print (msg)
                    print ("Exception occured")    
                
                elif msg.messageType() == REPORT:
                    ts = msg.getElementAsDatetime("timestampUtc")
                    print ("Timestamp: ", ts)
                    #print ("Message: \n", msg)
                    pm = msg.toPy()
                    
                    records = msg.getElement("records")

                    #for record in records.values():
                    #    
                    #    #print ("Record: \n", record)
                    #    if record.hasElement("security"):
                    #        security = record.getElement("security").getElement("ticker").getValueAsString()
                    #    else:
                    #        security = ""
                    #    
                    #    brokerAcronym = record.getElement("broker").getElement("acronym").getValueAsString()
                    #    brokerName = record.getElement("broker").getElement("name").getValueAsString()
                    #    brokerRank = record.getElement("broker").getElement("rank").getValueAsInteger()
                    #
                    #    topBrokers = record.getElement("topBrokers")
                    #    
                    #    topBrokerList=""
                    #
                    #    for topBroker in topBrokers.values():
                    #        topBrokerAcronym = topBroker.getElement("acronym").getValueAsString()
                    #        topBrokerName = topBroker.getElement("name").getValueAsString()
                    #        topBrokerRank = topBroker.getElement("rank").getValueAsInteger()
                    #
                    #        topBrokerList= topBrokerList + f"{topBrokerRank}: [{topBrokerAcronym}] {topBrokerName} |"
                    #
                    #    topBrokerList = "|" + topBrokerList
                    #
                    #
                    #    bought = record.getElementAsFloat("bought")
                    #    sold = record.getElementAsFloat("sold")
                    #    traded = record.getElementAsFloat("traded")
                    #    crossed = record.getElementAsFloat("crossed")
                    #    total = record.getElementAsFloat("total")
                    #    highTouch = record.getElementAsFloat("highTouch")
                    #    lowTouch = record.getElementAsFloat("lowTouch")
                    #    numReports = record.getElementAsFloat("numReports")
                    #
                    #    print (f"Security: {security}  Bought: {bought} Sold: {sold} Traded: {traded} Crossed: {crossed} Total: {total} High Touch: {highTouch} Low Touch: {lowTouch} Count: {numReports}  Broker: [{brokerAcronym}] {brokerName} Rank: {brokerRank} TopBrokers: {topBrokerList}")
                        
                
                global bEnd
                bEnd = True    
                        

    def processMiscEvents(self, event):
        print ("Processing " + event.eventType() + " event")

        for msg in event:

            print ("MESSAGE: %s" % (msg.toString()))


def main():

    # Session options are used to create the connectivity through to Bloomberg.
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host) # This represents the chose connectivity method. In this case, we are using Desktop API, so the host is 'localhost'
    sessionOptions.setServerPort(d_port) # The default port is 8194.
    
    print ("Connecting to %s:%d" % (d_host, d_port))

    eventHandler = SessionEventHandler() # We are using the asynchronous paradigm in this example, therefore we are using an event handler.

    session = blpapi.Session(sessionOptions, eventHandler.processEvent)

    if not session.startAsync():
        print ("Failed to start session.")
        return

    global bEnd
    while bEnd==False:
        pass
    
    session.stop()
    exit()

if __name__ == "__main__":
    print ("Bloomberg - RANK API Example - rankDataRequst")
    main()


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





