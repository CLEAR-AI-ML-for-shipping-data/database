# Notes

- Define types of voyages:
    - full length: origin port to destination port
    - only_when_moving
    - estimated missing trajectories

Notes from meeting in august 2024:
- stats for size of index for a bigger data set (linear or not)
- How much ram memory does the database need?
- [BYOL](https://arxiv.org/pdf/2006.07733)
- Visuallize and cluster view for trajecrory segments 
- Documentation for db usage

#### Questions:
- Should these be in the AIS_data / voyages? 
speed, course, heading, ROT, destination, EOT
- Commmon DB schema standards?


## Data transfer

rsync -avh --progress -e "ssh -i ~/.ssh/omnitron.cer" /Users/sidkas/Downloads/sfv2023.7z sid@172.25.113.95:/home/sid/workspace/clear_ais/


sudo apt install p7zip-full
7za x sfv2023.7z 


## ais 2018 stats

Count:29421289

"""public"".""ais_data"""	"2370 MB"	"884 MB"	"3254 MB"
"""public"".""voyage_segments"""	"49 MB"	"2832 kB"	"52 MB"


## Todo
- Identify voyages per each ship ID
- Identify voyage sements in  timeframe / window.
- How to reduce index size:
    - use lat, lon geometry point as index instead of combining timestamp and shipid
- Does the index size % reduce further with more data? (how does the graph look like)
- log all events

- add units to the variable names
- use iso format for the timestamps

## workshop 30th sep 2024
- Internal waters and speed < xx
- needs timestamps along with the coordinates for the voyage segments
- check if helcom project is opensource and can be updated further.
- static and dynamic obstacles.


## Work flow
- load csvs one by one
- order the data based on ship id / mmsi
- wait till the trajectory is complete before the data is pushed into the datbase
- compress the trajectory
- split database into months


> Web interfaces from docker:

Click on the links to open the locally served web pages
- [Martin tileserver](http://localhost:8090/catalog)
- [pg_admin db management](http://localhost:5050)
- [PostgREST API](http://localhost:8080)
- [Swagger postgREST API docs](http://localhost:8070)

## NAS info:
IP: 172.25.113.94
mac address: 90:09:D0:65:9B:C5

device: ClearNAS
admin: clear_admin
pass: TT5N3c8u6L


sudo mount -v -t cifs //172.25.113.94/ClearData /mnt/nas -osec=ntlmv2,username=clear_admin,password=TT5N3c8u6L,domain=ClearNAS,vers=3.0

sudo mount -v -t cifs //172.25.113.94/ClearData ./data/nas -o sec=ntlmv2,username=clear_admin,password=TT5N3c8u6L,domain=ClearNAS,vers=3.0,uid=999,file_mode=0750,dir_mode=0750


ssh -f -N -T -R localhost:5050:localhost:5050 cit@10.7.0.0 -p 8082

## Latest updates
- create new schemas automatically based on year - modified before inserting the data to database.
- pipeline to automate inserting data.
- make sure the process ends after inserting the data and doesn't get stuck in a forever loop