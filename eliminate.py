import MySQLdb
import os
import csv

def creatUnim(filename, ls):
	with open(filename,'rb') as alreadyin:
		alreadyin = csv.reader(alreadyin,delimiter='\t')
		for row in alreadyin:
			for i in row:
				ls.append(i)
				break
				


def itrDir(exp_folder, storing_path, ls):
	#temporarily put files in the folder on the desktop
	for root, dirs, files in os.walk(exp_folder):
		for name in files:
			if name not in ls:
				outname = ''.join([storing_path, name])
				inname = os.path.join(root,name)
				os.rename(inname, outname)
			else:
				continue




if __name__ == "__main__":
	fn = raw_input("give the file that includes imported filenames: ")
	ls = []
	creatUnim(fn,ls)
	indir = raw_input("give the experiment folder name: ")
	#enter with the / in the path
	ot = raw_input("give the output folder name: ")
	itrDir(indir,ot,ls)

