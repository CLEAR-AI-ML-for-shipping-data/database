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