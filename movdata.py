import MySQLdb
import os
import csv

def movData(tetrisDB, flist, exp_id):
	pn = 'p'+ str(exp_id)
	eid = exp_id
	header = ""
	with open("header.tsv",'rb') as hd:
		hd = csv.reader(hd,delimiter='\t')
		for row in hd:
			header = ','.join(row)
			break
	with open(flist,'r') as fl:
		for line in fl:
			#catch each session, remove the \n at the end of the line
			sesn = line[:len(line)-1]
			try:
				cursor = tetrisDB.cursor()
				# header partition_num exp_id session
				# header is original table's content
				# leave new table's field to be null
				queryinsert = """insert into completeTbl ({}) select * from complete partition({}) where exp_id = {} and session = '{}' """
				querydelete = """delete from complete partition({}) where exp_id = {} and session = '{}' """
				cursor.execute(queryinsert.format(header, pn, eid, sesn))
				cursor.execute(querydelete.format(pn, eid, sesn))
				tetrisDB.commit()
				print ("Finish transfer session "+ sesn + " in experiment " + str(eid))
			except:
				tetrisDB.rollback()
				print ("Error! - failed transfer session "+ sesn + " in experiment " + str(eid))

# test for single row of data move
# not used for new 
def mov(tetrisDB):
	try:
		cursor = tetrisDB.cursor()
		iquery = """insert into moveCheck (id, name) select * from testTB where id = {}"""
		dquery = """delete from testTB where id = {}"""
		cursor.execute(iquery.format(4))
		cursor.execute(dquery.format(4))
		tetrisDB.commit()
	except:
		tetrisDB.rollback()


def extsession(expfolder,year):
	# iterate through files and search for session 
	record = open("rpi_tour2014.txt",'w')
	for root, dirs, files in os.walk(expfolder):
		for data in files:
			if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
				inbegin = data.find(year)
				inend = data.find(".tsv")
				if (inbegin != inend and year in data[inbegin:inend]):
					record.write(data[inbegin+1:inend])
					record.write("\n")

if __name__ == "__main__":
	tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB")
	#movData(tetrisDB, flist , exp_id)
	#mov(tetrisDB)
	#extsession("/run/user/1000/gvfs/smb-share:server=hass11.win.rpi.edu,share=research/CogWorksLab/cogback/Projects/Tetris/RPI Tournaments","_2014")
	movData(tetrisDB,"rpi_tour2014.txt",7)
	tetrisDB.close()
