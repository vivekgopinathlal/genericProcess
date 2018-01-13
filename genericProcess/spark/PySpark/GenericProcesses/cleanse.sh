#!/bin/sh

usage()
{
  echo "Usage: Script wil cleanse the unnecessary newlines"
  echo "$0 --hdr [0/1] <If header exists its 1 else 0>
	   --filenm [/abs/file/path/name] <Provide filename to be cleansed>
	   --delim [,/|/:/] <possible column delimiter>
	   --ignr_hdr [0/1] <Should the output data contain header or not>
	   --ofile [/abs/file/path/name] <Provide filename the post cleanse file>"
  exit
}

#while getopts hdr:filenm:delim:ignr_hdr: option
while [ $# -gt 0 ]
do
option=$1
 case "${option}" in
 "--hdr")          shift
                   hdr=${1} ;;
 "--filenm")       shift
                   #filenm=${OPTARG} ;;
                   filenm=${1} ;;
 "--delim")        shift
                   #delim=${OPTARG} ;;
                   delim=${1} ;;
 "--ignr_hdr")     shift
                   #ignr_hdr=${OPTARG} ;;
                   ignr_hdr=${1} ;;
 "--ofile")        shift
		   ofile=${1};;
       *)          usage
                   exit 1;;
 esac
shift
done

if [[ -z ${hdr} || -z ${filenm} || -z ${delim} ]]; then
 usage
fi
ignr_hdr=${ignr_hdr:-0}
frame_abs_path=${filenm}".MODIFIED"
ofile=${ofile:-"${frame_abs_path}"}
#hdr=${1}
#filenm=${2}
#delim=${3}

if [ ${hdr} ]
then
  echo "Inside True"
  numOfAttr=$(awk -F"${delim}" 'NR==1{print NF}' ${filenm})
  echo "--"${numOfAttr}"--"
else
  numOfAttr=$(awk -F"${delim}" '{_[NF]=_[NF]+1;} END{for(val in _){if(max<_[val]){max=_[val]; attr=val}}print attr}' ${filenm})
fi
echo "Total num of attr : "$numOfAttr
#-- code that corrects teh newline --
awk -F"${delim}" '
BEGIN{if('${ignr_hdr}') skip_hdr=1; }
{if(skip_hdr){ skip_hdr=0; next;}}
NF<'${numOfAttr}'{
      if(NF>1) 
         cnt=cnt+NF-1;
      else
         cnt=cnt+NF
           
      if(cnt=='${numOfAttr}')
         print $0
      else
         printf $0; 
           
}
NF=='${numOfAttr}'{print $0}' ${filenm} >> ${ofile}
echo "Completed: Data Written to - "${ofile}
