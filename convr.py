import csv
import os

def conv_source(filename, exp_id, outname):
	with open(filename,'rb') as indata, open(outname, 'wb') as outdata:
		indata = csv.reader(indata,delimiter='\t')
		outdata = csv.writer(outdata,delimiter='\t')
		headerflag = 0
		for row in indata:
			if (headerflag == 0):
				row.append('exp_id')
				headerflag = 1
			else:
				row.append(exp_id)
			outdata.writerow(row)


def itr_dir(exp_folder, exp_id):
	#temporarily put files in the folder on the desktop
	storing_path = "/Users/Etsu/Desktop/temp/"
	#count = 0
	for root, dirs, files in os.walk(exp_folder):
		for name in files:
            #make sure the file is complete
			if "complete_" in name and "incomplete" not in name:
				outname = ''.join([storing_path, name])
				inname = os.path.join(root,name)
            	print ("converting", inname)
            	# print ("store at", outname)
            	# conv_source(inname, exp_id, outname)

            #if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name:
            #	outname = ''.join([storing_path, name])
			#	if (os.path.exists(outname)):
			#		continue
			#	else:
			#		inname = os.path.join(root,name)
			#		print ("converting", inname)
			#		print ("store at", outname)
			#		conv_source(inname, exp_id, outname)
	#print (count)
			

def main():
	#filename = raw_input("give the complete file source: ")
	exp_id = raw_input("give the exp_id: ")
	#conv_source(filename, exp_id)
	exp_dir = raw_input("give the exp folder path: ")
	itr_dir(exp_dir, exp_id)


if __name__ == "__main__":
	main()



