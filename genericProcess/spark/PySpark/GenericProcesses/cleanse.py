import os, sys, csv, argparse

def identifyByHeader(f,colDel):
  return len(next(csv.reader(f, delimiter=colDel)))

def parseData(f,colDel):
  arr={}
  for rec in csv.reader(f, delimiter=colDel):
    arr[len(rec)]=arr.setdefault(len(rec),0)+1
  return max(arr,key=arr.get)

def adjustNewline(wh,reader,numOfAttr,colDel,recDel,ignoreHdr):
  jnval=':jn:'
  jn=[jnval]
  hdrLine=next(reader) if ignoreHdr else reader
  for rec in reader:
    irec=rec
    while len(irec)<numOfAttr:
      irec=irec+jn+next(reader)
      irec=(((colDel.join(irec)).replace(colDel+jnval+colDel,'')).replace(colDel+jnval,'').replace(jnval+colDel,'')).split(colDel)
    wh.write(colDel.join(irec)+recDel)
  return

def main(args):
  absFile=args.fileToClean
  hdrBool=args.headerAvail
  colDel=args.delimiter
  ignoreHdr=args.ignoreHeader
  tmpFile=args.outFile
  recDel='\n'

  fh=open(absFile,'r')
  numOfAttr=identifyByHeader(fh,colDel) if hdrBool else parseData(fh,colDel)
  reader=csv.reader(open(absFile,'rb'), delimiter=colDel)
  wh=open(absFile+'_'+tmpFile,'w+')
  adjustNewline(wh,reader,numOfAttr,colDel,recDel,ignoreHdr)

  wh.close()
  fh.close()

def parseArguments(parser):
    # Positional mandatory arguments
    parser.add_argument("-f", "--fileToClean", help="Absolute FileName - File to be cleansed")
    parser.add_argument("-t", "--headerAvail", help="True/False - Data to be cleansed contains header record (Attribute Names)")
    parser.add_argument("-d", "--delimiter", help="[,:|] Column Delimiter of the data file")
    # Optional arguments
    parser.add_argument("-o", "--outFile", help="Output File", default='tmp_file')
    parser.add_argument("-i", "--ignoreHeader", help="Ignore the First Record", default=True)
    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    # Parse arguments
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Perform Cleansing of data specific to Client")
    args = parseArguments(parser)

    # Raw print arguments
    print("You are running the script with arguments: ")
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
           sys.exit(0)
    main(args)
