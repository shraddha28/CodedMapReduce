count=$(find DROPBOX -type f | wc -l)
temp=$count
echo `date +%R\ ` ": Start Timer"
while [ $count -le 1 ]
do
	count=$(find DROPBOX -type f | wc -l)
	if [ $temp != $count ]
	then
	echo `date +%R\ ` ": File Uploaded Successfully"
	fi
	temp=$count
	if [ $count -ge 1 ]
	then
	echo `date +%R\ ` ": All files succesfully uploaded"
	echo `date +%R\ ` ": Stop Timer"
	break
	fi
done