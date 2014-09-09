import json, requests

class Settings(object):

    controllerHost = "controller"
    controllerPort = "8500"
    catalog_api = "v1/catalog"
    services = {}

    def __init__(self,path,namespace,strict=True):
        # Load the json settings file provided.
        self.settings = json.loads(open(path).read(), strict=strict)[namespace]

    def pollServices(self, controllerHost = None):

        if controllerHost is not None:
            self.controllerHost = controllerHost

        services_uri = "http://%s:%s/%s/%s" % (
            self.controllerHost, 
            str(self.controllerPort), 
            self.catalog_api, 
            "services"
        )

        r = requests.get(services_uri)

        if r.status_code != 200:
            print "ERROR: unable to communicate with consul server"

        services = r.json()
        for service in services:
            print "Found service %s" % service
            service_uri = "http://%s:%s/%s/%s/%s" % (
                self.controllerHost, 
                str(self.controllerPort), 
                self.catalog_api, 
                "service", 
                service
            )
            info = requests.get(service_uri).json()
            self.services[service] = info[0]

    def getServiceAddress(self, service):
        if service not in self.services:
            raise Exception("Service [%s] was not in known services!" % service)
        return self.services[service]['Address']

    def getServiceInfo(self, service):
        if service not in self.services:
            raise Exception("Service [%s] was not in known services!" % service)
        return self.services[service]

    def __getitem__(self,key):
        # Returns the requested setting.
        return self.settings[key]

    def __setitem__(self,key,val):
        # Overrides the setting with a new value.
        # Note that this does not overwrite the file.
        self.settings[key] = val
