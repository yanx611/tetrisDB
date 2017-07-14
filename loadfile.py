import MySQLdb
import os
import csv

# export DYLD_LIBRARY_PATH=/usr/local/mysql/lib/

def prep(filename, exp_id, store, ln):
	if (filename == ""):
		return
	print ("converting", filename)
	with open(filename,'rb') as indata, open(store, 'wb') as outdata:
		indata = csv.reader(indata,delimiter='\t')
		outdata = csv.writer(outdata, delimiter = "\t")
		headerflag = 0
		headercount = 0
		for row in indata:
			if (headerflag == 0):
				headercount = len(row)
				row.append('exp_id')
				headerflag = 1
			else:
				if (len(row) != headercount):
					num = headercount - len(row)
					for i in range(num):
						row.append("\N")
				elif (len(row) > headercount):
					#record the data and skip
					ln.write(filename)
					ln.write("\n ... \n")
					ln.write(row)
					ln.write("\n")		
					continue
				row.append(str(exp_id))
			for i,value in enumerate(row):
				if (value == "" or value == "None"):
					row[i] = "\N"
			outdata.writerow(row)


def itrDir(exp_folder, exp_id, storing_path, ln):
	#temporarily put files in the folder on the desktop
	for root, dirs, files in os.walk(exp_folder):
		for name in files:
			if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name and "complete_" in name and "2013" not in name and "lock" not in name:
				outname = ''.join([storing_path, name])
				inname = os.path.join(root,name)
				prep(inname, exp_id, outname, ln)
			else:
				continue


def loadFile(directory,tetrisDB,ef,imf):
	for subdir, dirs, files in os.walk(directory):
		for data in files:
			if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
				datadir = os.path.join(subdir, data)
				datadir = os.path.abspath(datadir)
				# load into database 
				# try:
				header = ""
				print ("V loading, ", datadir)
				with open(datadir,'rb') as indata:
					indata = csv.reader(indata,delimiter='\t')
					for row in indata:
						header = ','.join(row)
						break
				try:
					cursor = tetrisDB.cursor()
					query = """LOAD DATA LOCAL INFILE '{}' INTO TABLE complete FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ({}); """
					cursor.execute(query.format(datadir, header))
					tetrisDB.commit()
					imf.write(data)
					imf.write("\n")
				except:
					ef.write("Exception: ")
					ef.write(data)
					ef.write("\n")
					tetrisDB.rollback()


if __name__ == "__main__":
	convim = raw_input("1. convert+import 2.import -> ")
	if (convim == "1"):
		indir = raw_input("give the experiment folder name: ")
		#enter with the / in the path
		ot = raw_input("give the output folder name: ")
		exp_id = raw_input("assign the experiment id: ")
		part = raw_input("assign the part id: ")
		ln = "wrongdata-"+str(exp_id)+"-"+str(part)+".txt"
		lnfile = open(ln,"w")
		#first convert all the data
		itrDir(indir,exp_id,ot,lnfile)
		lnfile.close()
		#then using the loaddata function to input all the data into the database
		#have a file to write errors
		conv = raw_input("Import or not?")
		if (conv == "Yes"):
			tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
			efn = "errors-"+str(exp_id)+"-"+str(part)+".txt"
			ifn = "imported"+str(exp_id)+"-"+str(part)+".txt"
			errorfile = open(efn, "w")
			importfile = open(ifn,"w")
			loadFile(ot,tetrisDB, errorfile, importfile)
			errorfile.close()
			importfile.close()
			tetrisDB.close()
	elif (convim == "2"):
		ot = raw_input("give the output folder name: ")	
		exp_id = raw_input("assign the experiment id: ")
		part = raw_input("assign the part id: ")
		tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
		efn = "errors-"+str(exp_id)+"-"+str(part)+".txt"
		ifn = "imported-"+str(exp_id)+"-"+str(part)+".txt"
		errorfile = open(efn, "w")
		importfile = open(ifn,"w")
		loadFile(ot,tetrisDB, errorfile, importfile)
		errorfile.close()
		importfile.close()
		tetrisDB.close()

				
	
	


