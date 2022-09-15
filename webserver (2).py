# Python 3 server example
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time
from urllib import response
from urllib.request import HTTPDefaultErrorHandler
import requests
from work_with_postgre2 import do_mock, do_work

hostName = "localhost"
serverPort = 8181

class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/j':
            res = do_work() 
            #res = do_mock()  #do_work()  
            self.send_response(200)
            #self.send_header("Content-type", "application/json")            
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
            command_string="<p><b>Please click here to close the result: </b><a href=http://localhost:8181/jjjj>Close</a></p>"
            self.wfile.write(bytes(command_string, "utf-8"))
            self.wfile.write(bytes(res, "utf-8"))
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            url='https://www.youtube.com/results'
            query={'search_query':'audi'}
            response=requests.get(url,params=query)
            command_string="<p><b>youtube audi: </b><a href="+response.url+">open</a></p>"
            self.wfile.write(bytes(command_string, "utf-8"))
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
            command_string="<p><b>Please click here to show the result: </b><a href=http://localhost:8181/j>Show</a></p>"
            self.wfile.write(bytes(command_string, "utf-8"))
            self.path='https://pythonbasics.org'  ##################################
            self.wfile.write(bytes("</body></html>", "utf-8"))

if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), MyHandler)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")