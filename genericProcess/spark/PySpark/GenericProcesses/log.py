import getpass, os, socket
from time import gmtime, strftime
#from common_functions import updateRunStatus as urs

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = "\033[1m"
CRED = '\033[31m'

def disable():
    HEADER = ''
    OKBLUE = ''
    OKGREEN = ''
    WARNING = ''
    FAIL = ''
    ENDC = ''

def getCurTime(format="%Y-%m-%d %H:%M:%S"):
        return strftime(format, gmtime())

def getServerUser():
	hostname=socket.gethostname()
	user=getpass.getuser()
	curTime=getCurTime()
	return hostname+"|"+user+"|"+curTime+"|"

	
def infog( msg,batch_id=0,prog_id=0,status='Success', track=False):
    print getServerUser() + OKGREEN + msg + ENDC
    #if track:
      #urs(batch_id,prog_id,status,msg)

def info( msg,batch_id=0,prog_id=0,status='Success', track=False):
    print getServerUser() + OKBLUE + msg + ENDC
    #if track:
      #urs(batch_id,prog_id,status,msg)

def warn( msg, track=False):
    print getServerUser() + WARNING + msg + ENDC
    
def err( msg, batch_id=0,prog_id=0,status='Failure',track=False):
    print getServerUser() + FAIL +"\t"+ msg + ENDC
    #if track:
      #urs(batch_id,prog_id,status,msg)

def rerr( msg, track=False):
    print getServerUser() + FAIL + msg + ENDC

