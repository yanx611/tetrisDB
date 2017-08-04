import csv
import os

attributes = []

def check_agree(filename):
    with open(filename,'rb') as indata:
		indata = csv.reader(indata,delimiter='\t')
		for row in indata:
            		for i in row:
				if (i not in attributes):
					attributes.append(i)
			break
    
def itr_dir(exp_folder):
	#temporarily put files in the folder on the desktop
	storing_path = "/home/cogworks/Desktop/tmp2015F/"
        for root, dirs, files in os.walk(exp_folder):
                for name in files:
                        #make sure the file is complete
                        if "complete_" in name and "incomplete" not in name:
				inname = os.path.join(root,name)
				check_agree(inname)


def main():
	exp_dir = raw_input("give the exp folder path: ")
	itr_dir(exp_dir)
	print (attributes)


if __name__ == "__main__":
	main()



