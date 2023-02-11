import glob

outfile = 'commands.txt'

dirs = glob.glob('*2004*')

with open(outfile,'w') as ofile:
        for dirn in dirs:
                ofile.write("sync "+dirn+"/ s3://ini210004tommorrell/tomography_archive/"+dirn+"/\n")


