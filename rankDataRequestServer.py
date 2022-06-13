# rankDataRequestServer.py

import sys
import blpapi
import datetime

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

AUTHORIZATION_SUCCESS           = blpapi.Name("AuthorizationSuccess")
AUTHORIZATION_FAILURE           = blpapi.Name("AuthorizationFailure")

SLOW_CONSUMER_WARNING           = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED   = blpapi.Name("SlowConsumerWarningCleared")

EXCEPTION                       = blpapi.Name("Exception")
REPORT                          = blpapi.Name("Report")

d_rank = "//blp/rankapi"
d_auth = "//blp/apiauth"
d_host = "10.137.23.19"             # IP address of the Trading API Server instance.
d_port = 8194
d_user = "RANKAPI\\SERVER_API"      # User name of the server-side user.
d_ip = "1.1.1.1"                    # Any IP address unique for this user.

bEnd=False


class SessionEventHandler():
    
    def sendAuthRequest(self,session):
                
        authService = session.getService(d_auth)
        authReq = authService.createAuthorizationRequest()
        authReq.set("emrsId",d_user)
        authReq.set("ipAddress", d_ip)
        self.identity = session.createIdentity()
        
        print ("Sending authorization request: %s" % (authReq))
        
        session.sendAuthorizationRequest(authReq, self.identity)
        
        print ("Authorization request sent.")

    
    def sendCreateOrder(self, session):

        service = session.getService(d_rank)
        
        # build request
        request = service.createRequest("Query")
        
        ### set date/time range
        request.set("start", datetime.datetime(2021, 3, 1, 0, 0, 0, 0)) 
        request.set("end", datetime.datetime(2021, 3, 2, 0, 0, 0, 0)) 
                    
        ### group by 0=Broker , 1=Security
        #request.set("groupBy", "Broker")
        request.set("groupBy", "Security")

        ### exchanges or securities 
        # exchanges can be set using Bloomberg exchange code 
        #exchanges = request.getElement("securityCriteria").setChoice("exchanges")
        #exchange = exchanges.appendElement()
        #exchange.setElement("code", "US");
    
        ### securities can be set to either Bloomberg ticker or figi
        securities = request.getElement("securityCriteria").setChoice("securities")
        securities.appendElement().setElement("ticker", "AAPL US Equity")
        securities.appendElement().setElement("ticker", "MSFT US Equity")
        securities.appendElement().setElement("ticker", "VOD LN Equity")

        #security.setElement("figi", "BBG000B9XRY4"); # figi for AAPL US Equity
        
        ### specify broker by acronym or rank
        #broker = request.getElement("brokers").appendElement()
        #broker.setElement("acronym", "ABCD"); # acronym

        ### source enum 0=Broker Contributed
        request.set("source", "Broker Contributed")

        ### units enum 0=Shares, 1=Local, 2=USD, 3=EUR, 4=GBP
        request.set("units", "Shares")             
        
        print ("Sending Request: %s" % request.toString())

        #self.requestID = session.sendRequest(request)
        
        self.requestID = blpapi.CorrelationId()
        session.sendRequest(request, identity=self.identity, correlationId=self.requestID)  #Note the addition of the identity object.
        
        print ("RANK data request sent.")


    def processAdminEvent(self,event):  
        print("Processing ADMIN event")

        for msg in event:
            if msg.messageType() == SLOW_CONSUMER_WARNING:
                print("Warning: Entered Slow Consumer status")
                
            elif msg.messageType() == SLOW_CONSUMER_WARNING_CLEARED:
                print("Slow consumer status cleared")
                
            else:
                print(msg)


    def processSessionStatusEvent(self,event,session):  
        print("Processing SESSION_STATUS event")

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print("Session started...")
                session.openServiceAsync(d_auth)
                
            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print("Error: Session startup failed", file=sys.stderr)
                
            else:
                print(msg)
                

    def processServiceStatusEvent(self,event,session):
        print ("Processing SERVICE_STATUS event")
        
        for msg in event:
            
            if msg.messageType() == SERVICE_OPENED:
                
                serviceName = msg.asElement().getElementAsString("serviceName")
                
                print("Service opened [%s]" % (serviceName))

                if serviceName==d_auth:
                    
                    print("Auth service opened... Opening EMSX service...")
                    session.openServiceAsync(d_rank)
                
                elif serviceName==d_rank:
                    
                    print("RANK service opened... Sending authorization request...")
                    
                    self.sendAuthRequest(session)
                
            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                    print("Error: Service Failed to open", file=sys.stderr)
                
                
    
    def processAuthorizationStatusEvent(self,event):
        
        print ("Processing AUTHORIZATION_STATUS event")

        for msg in event:

            print("AUTHORIZATION_STATUS message: %s" % (msg))


                
    def processResponseEvent(self, event, session):
        print("Processing RESPONSE event")
        
        for msg in event:
            
            print("MESSAGE: %s" % msg.toString())
            print("CORRELATION ID: %d" % msg.correlationIds()[0].value())

            if msg.messageType() == AUTHORIZATION_SUCCESS:
                print("Authorization successful....")
                print ("SeatType: %s" % (self.identity.getSeatType()))
                self.sendCreateOrder(session)

            elif msg.messageType() == AUTHORIZATION_FAILURE:
                print("Authorization failed....", file=sys.stderr)
                # insert code here to automatically retry authorization...

            elif msg.correlationIds()[0].value() == self.requestID.value():
                print("MESSAGE TYPE: %s" % msg.messageType())
                
                if msg.messageType() == EXCEPTION:
                    print (msg)
                    print ("Exception occured")    
                
                elif msg.messageType() == REPORT:
                    ts = msg.getElementAsDatetime("timestampUtc")
                    print ("Timestamp: ", ts)
                    print(msg)

                global bEnd
                bEnd = True            
            else:
                print ("Unexpected message...")
                print (msg)
                    
            
    def processMiscEvents(self, event):
        
        print("Processing " + event.eventType() + " event")
        
        for msg in event:

            print("MISC MESSAGE: %s" % (msg.tostring()))


    def processEvent(self, event, session):
        try:
            
            if event.eventType() == blpapi.Event.ADMIN:
                self.processAdminEvent(event)
            
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.AUTHORIZATION_STATUS:
                self.processAuthorizationStatusEvent(event)

            elif event.eventType() == blpapi.Event.RESPONSE:
                self.processResponseEvent(event,session)
            
            else:
                self.processMiscEvents(event)
                
        except:
            print("Exception:  %s" % sys.exc_info()[0])
            
        return False

                
def main():
    
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host)
    sessionOptions.setServerPort(d_port)

    print("Connecting to %s:%d" % (d_host,d_port))

    eventHandler = SessionEventHandler()

    session = blpapi.Session(sessionOptions, eventHandler.processEvent)

    if not session.startAsync():
        print("Failed to start session.", file=sys.stderr)
        return
    
    global bEnd
    while bEnd==False:
        pass
    
    print ("Terminating...")
    
    session.stop()

if __name__ == "__main__":
    print("Bloomberg - RANK API Example - Server - rankDataRequestServer")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")


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
