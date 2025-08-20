#!/bin/bash

if [ -z "$1" ]
then
echo "PANIC !!!!!!\n I NEED an input file representing the used NODES ( similar to \$OAR_NODE_FILE | sort | uniq )"
else

fileNODE=$1

#              
ARRAY_WAITTIME=( 500 )
ARRAY_SyncTime=( 10 50 100 150 200 300 400 )
ARRAY_Repetition=( 1 ) # 4 5 )
ARRAY_NbPeers=( 5 ) # 30 50 
ARRAY_UpdatesNb=( 1000 ) # 400 600 800 1000 ) #  10 100 
ARRAY_NbPeers_Updating=( 1 ) # 




rm advancement

###read csv in###
# nbline=1
# while [ $nbline -lt $(awk 'END { print NR }' configs.csv) ]
# do
# {
#      lineskip=$nbline
# while IFS=, read -r nbpeers nbpeersUpdating nbupdates SyncTime numeroUNIQUE
# do 

#     if ((lineskip))
#     then
#         ((lineskip--))
#      else
#         echo "nbpeers:${nbpeers} nbpeersUpdating:${nbpeersUpdating} nbupdates:${nbupdates} SyncTime:${SyncTime} Version:${numeroUNIQUE}"
#         waitTime=${ARRAY_WAITTIME[0]}
###end of read csv in ###

###normal in###
for numeroUNIQUE in "${ARRAY_Repetition[@]}"
do

for nbpeers in "${ARRAY_NbPeers[@]}"
do
for nbpeersUpdating in "${ARRAY_NbPeers_Updating[@]}"
do
for nbupdates in "${ARRAY_UpdatesNb[@]}"
do

if [ $nbpeers -lt $nbpeersUpdating ]
then
echo "$nbpeers < $nbpeersUpdating"
else

for waitTime in "${ARRAY_WAITTIME[@]}"
do
for SyncTime in "${ARRAY_SyncTime[@]}"
do

###end of normal in###

rm "/home/quacher/.ssh/known_hosts"
folder="Results/${nbpeers}Peers/${nbpeersUpdating}Updater/${nbupdates}Updates/${waitTime}waitTime/${SyncTime}SyncTime/Version$numeroUNIQUE"
echo "numeroUNIQUE: $numeroUNIQUE - nbpeers: $nbpeers - nbpeersUpdating: $nbpeersUpdating - nbupdates: $nbupdates - waitTime: ${waitTime} - SyncTime: ${SyncTime}" >> advancement
mkdir -p $folder

./run_multipleBIS.sh $nbpeers $nbupdates $nbpeersUpdating  $fileNODE $waitTime $SyncTime


others=$(cat other)

echo "RETRIEVEDATA"
./RetrieveData.sh resultRetrieve $folder > $folder/Retrieve.log 2>&1
echo "RETRIEVEDATA - THE END"


save=( )

for f in $folder/go_trans_* 
do
     save+=" $f/CRDT_IPFS/node1/time.csv"
done


###normal out###

done 
done
fi 

done
done
done
done

###end of normal out###


###Read csv Out###
# fi
# done
# }  < configs.csv
# ((nbline++))
# echo ""
# echo ""
# done
##end of readcsv###

fi
