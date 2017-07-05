import MySQLdb
import os
import csv

# export DYLD_LIBRARY_PATH=/usr/local/mysql/lib/

def prep(filename, exp_id, store):
	if (filename == ""):
		return
	print ("converting", filename)
	with open(filename,'rb') as indata, open(store, 'wb') as outdata:
		indata = csv.reader(indata,delimiter='\t')
		outdata = csv.writer(outdata, delimiter = "\t")
		headerflag = 0
		for row in indata:
			if (headerflag == 0):
				row.append('exp_id')
				headerflag = 1
			else:
				row.append(1)
			for i,value in enumerate(row):
				if (value == ""):
					row[i] = "\N"
			outdata.writerow(row)


def itrDir(exp_folder, exp_id, storing_path):
	#temporarily put files in the folder on the desktop
	for root, dirs, files in os.walk(exp_folder):
		for name in files:
			print name
			if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name:
				outname = ''.join([storing_path, name])
				inname = os.path.join(root,name)
				print(inname,outname)
				prep(inname, exp_id, outname)
			else:
				continue


def loadFile(directory,tetrisDB):
	for subdir, dirs, files in os.walk(directory):
		for data in files:
			if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
				datadir = os.path.join(subdir, data)
				datadir = os.path.abspath(datadir)
				# load into database 
				# try:
				header = ""
				print ("loading, ", datadir)
				with open(datadir,'rb') as indata:
					indata = csv.reader(indata,delimiter='\t')
					for row in indata:
						header = ','.join(row)
						break
				try:
					cursor = tetrisDB.cursor()
					query = """LOAD DATA LOCAL INFILE '{}' INTO TABLE complete FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ({}); """
					print query
					cursor.execute(query.format(datadir, header))
					tetrisDB.commit()
				except:
					errorfile.write("Exception: ")
					errorfile.write(data)
					errorfile.write("\n")
					tetrisDB.rollback()


if __name__ == "__main__":
	indir = raw_input("give the experiment folder name: ")
	#enter with the / in the path
	ot = raw_input("give the output folder name: ")
	exp_id = raw_input("assign the experiment id: ")
	#first convert all the data
	itrDir(indir,exp_id,ot)
	#then using the loaddata function to input all the data into the database
	#have a file to write errors
	tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "", db = "TestDB", local_infile = 1)
	errorfile = open("errors.txt", "w")
	loadFile(ot,tetrisDB)
	errorfile.close()
	tetrisDB.close()

				
	
	


