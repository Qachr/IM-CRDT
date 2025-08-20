# SSD1=/dev/$(lsblk | grep -w 447.1G |  head -n1  | awk '{print $1;}')
# SSD2=/dev/$(lsblk | grep -w 223.6G |  head -n1  | awk '{print $1;}')
# HDD=/dev/$(lsblk | grep -w 3.6T |  head -n1  | awk '{print $1;}')
# mkfs $SSD1
# mkdir CRDT_IPFS
# mount $SSD1 /root/CRDT_IPFS
# rm -r /root/CRDT_IPFS/